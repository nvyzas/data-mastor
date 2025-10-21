import json
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path

import pandas as pd
import typer
from deepdiff import DeepDiff
from scrapy import Spider
from scrapy.exceptions import DropItem
from sqlalchemy.orm import sessionmaker

from data_mastor.cliutils import get_yamldict_key
from data_mastor.dbman import get_engine
from data_mastor.scraper.models import (
    Listing,
    Product,
    Source,
)
from data_mastor.scraper.schemas import ListingItem, SourceItem
from data_mastor.scraper.utils import abort


# TODO make it into a generic class
class Storer:
    def __init__(self, now: datetime | None = None, dont_store: bool = False):
        self.now = datetime.now() if now is None else now
        self.dont_store = dont_store
        # db session
        self.added = []
        self.deleted = []
        self.engine = get_engine()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            now=crawler.settings.get("NOW"),
            dont_store=crawler.settings.get("DONT_STORE"),
        )

    def _add_to_session(self, entity, spider: Spider):
        try:
            self.session.add(entity)
        except Exception as exc:
            abort(spider, exc, self.close_spider)
        else:
            self.added.append(entity)

    def _process_samples(self, spider: Spider):
        # get samples
        samples = getattr(spider, "get_samples", lambda: None)()
        if not samples:
            spider.logger.warning("Spider does not provide any samples for testing")
            return
        # perform test
        for item in samples:
            try:
                self.process_item(item, spider)
            except Exception as exc:
                abort(spider, exc)
        # purge session state (removing the sample items)
        self.session.expunge_all()
        self.added = []

    def open_spider(self, spider: Spider):
        spider.logger.debug(f"Running {type(self).__name__} open_spider")

        # print num entities # TODO: fix
        # num = num_entities(self.session, spider.itemcls())
        # print(f"Number of entities on pipeline start: {num}")

        # simulate item processing using samples (before doing the actual scraping)
        self._process_samples(spider)

    def close_spider(self, spider: Spider):
        spider.logger.debug(f"Running {type(self).__name__} close_spider")
        added = "\n".join(map(str, self.added))
        deleted = "\n".join(map(str, self.deleted))
        spider.logger.info(f"Added: {added}")
        spider.logger.info(f"Deleted: {deleted}")
        if self.dont_store:
            try:
                self.session.flush()
            except Exception as exc:
                spider.logger.error(f"Session flush failed with {exc}")
            else:
                spider.logger.info("Session flush was successful")
        else:
            try:
                self.session.commit()
            except Exception as exc:
                spider.logger.error(exc)
                spider.logger.info(f"Session commit failed with {exc}")
                self.session.rollback()
            else:
                spider.logger.info("Session commit was successful")

        # print num entities # TODO: fix
        # num = num_entities(self.session, spider.itemcls())
        # print(f"Number of entities on pipeline end: {num}")

        self.session.close()

    def process_item(self, item, spider):
        raise NotImplementedError


class ListingStorer(Storer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.products = pd.read_sql_table(Product.__tablename__, self.engine)

    def process_item(self, item: ListingItem, spider: Spider):
        # create copy to be returned
        it = replace(item)

        # format price # TODO be able to process more price fields (via init arg?)
        def cure_price(price):
            return price.replace("€", "").replace(",", ".").strip() if price else price

        it.price = cure_price(it.price)

        # log differences after processing
        spider.logger.debug(f"Item diff: {DeepDiff(asdict(item), asdict(it))}")

        # create listing object
        listing = Listing(**asdict(it))

        # add timestamp
        listing.created_at = self.now

        # add to session
        self._add_to_session(listing, spider)
        return item


class SourceStorer(Storer):
    def process_item(self, item: SourceItem, spider: Spider):
        # create copy to be returned
        it = replace(item)

        # find parent
        parent = None
        parent_url = it.parent_url
        # look in session
        if parent_url:
            parents = [
                src
                for src in self.session.new
                if getattr(src, Source.url.key) == parent_url
            ]
            if len(parents) > 1:
                spider.logger.warning(f"{it} has multiple parents in sess: {parents}")
            elif len(parents) == 1:
                parent = parents[0]
        # check
        if parent_url and parent is None:
            raise DropItem(f"Could not find parent for {item}")
        if it.level > 0 and parent is None:
            raise DropItem(f"{item} has non-zero level and no parent")

        # create source item
        src = Source(**asdict(it))
        src.parent = parent
        src.created_at = self.now

        # add item
        self.session.add(src)
        self.added.append(src)

        # return item
        return item


def process_json_feed(json_path: str | Path, **pipe_kwargs):
    # read feed
    with open(json_path) as file:
        feed = json.load(file)
    # init pipeline
    pipe: SourceStorer | ListingStorer
    items: list[SourceItem] | list[ListingItem]
    feedpath = str(Path(json_path).absolute())
    if "_src/" in feedpath:
        pipe = SourceStorer(**pipe_kwargs)
        items = [SourceItem(**dct) for dct in feed]
    elif "_lst/" in feedpath:
        pipe = ListingStorer(**pipe_kwargs)
        items = [ListingItem(**dct) for dct in feed]
    else:
        raise RuntimeError(f"No '_src/' or '_lst/' in feedpath '{feedpath}'")
    # process
    process_items(items, pipe)


def process_items(items: list, pipe: Storer):
    """Store items taken from a feed (output of a previous spider run).

    For this to be possible, the feed items must have the same form as scraped by the
    spider, thus self.process_item must return the scraped item unchanged
    """
    # init spider
    spider = Spider("dummy")
    # manually call open_spider
    pipe.open_spider(spider)
    # process feed items
    processed = []
    for item in items:
        it = pipe.process_item(item, spider)
        processed.append(it)
    # manually call close_spider
    pipe.close_spider(spider)
    # return the processed items (expected to be same as in feed!)
    return processed


def main(args_yaml: Path = Path("conf.yml")):
    # parse args
    args = get_yamldict_key(args_yaml, "pipelines")
    dont_store = args.get("dont_store", True)
    path = Path(args["path"]).absolute()
    json_filepaths = [path] if path.is_file() else list(path.rglob("feed.json"))
    # process
    for f in json_filepaths:
        process_json_feed(f, dont_store=dont_store)


if __name__ == "__main__":
    typer.run(main)
