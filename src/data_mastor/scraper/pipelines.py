import json
import logging
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Self

import pandas as pd
import typer
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

TIMESTAMP_FMT = "%Y-%m-%d_%H-%M-%S"


# DO make it into a generic class
class Storer[TEntity: Listing | Source, TItem: ListingItem | SourceItem]:
    def __init__(
        self,
        entitycls: type[TEntity],
        now: str | None = None,
        dont_store: bool = False,
    ) -> None:
        self.now = datetime.strptime(now, TIMESTAMP_FMT) if now else datetime.now()
        self.dont_store = dont_store
        self.entitycls = entitycls
        if not issubclass(self.entitycls, (Listing, Source)):
            raise TypeError(f"self.entitycls ({self.entitycls}) is not Listing/Source")
        # db session
        self._added: list[TEntity] = []
        self._deleted: list[TEntity] = []
        self._engine = get_engine()
        self._session = sessionmaker(bind=self._engine)()

    # meant to be called only by a Storer subclass, not (directly) by scrapy
    @classmethod
    def from_crawler(cls, crawler, entitycls: type[TEntity] | None = None) -> Self:
        if entitycls is None:
            raise TypeError("entitycls is None")
        return cls(
            entitycls,
            now=crawler.settings.get("NOW"),
            dont_store=crawler.settings.get("DONT_STORE"),
        )

    def _add_to_session(self, entity, spider: Spider) -> None:
        try:
            self._session.add(entity)
        except Exception as exc:
            abort(spider, exc, self.close_spider)
        else:
            self._added.append(entity)

    def _process_samples(self, spider: Spider) -> None:
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
        spider.logger.info("Testsamples were processed without errors")
        # purge session state (removing the sample items)
        self._session.expunge_all()
        self._added = []
        # DO also test session flush/commit

    def _log_num_entities(self, spider: Spider) -> None:
        num = self.entitycls.num_entities(self._session)
        spider.logger.info(f"Number of entities on pipeline start: {num}")

    def open_spider(self, spider: Spider) -> None:
        spider.logger.debug(f"Running {type(self).__name__} open_spider")
        self._log_num_entities(spider)

        # simulate item processing using samples (before doing the actual scraping)
        self._process_samples(spider)

    def close_spider(self, spider: Spider) -> None:
        spider.logger.debug(f"Running {type(self).__name__} close_spider")
        self._log_num_entities(spider)
        spider.logger.info(f"Added {len(self._added)} items")
        spider.logger.info(f"Deleted {len(self._deleted)} items")
        if self.dont_store:
            try:
                self._session.flush()
            except Exception as exc:
                abort(spider, exc)
            else:
                spider.logger.info("Session flush was successful")
            finally:
                self._session.rollback()
        else:
            try:
                self._session.commit()
            except Exception as exc:
                self._session.rollback()
                abort(spider, exc)
            else:
                spider.logger.info("Session commit was successful")

        # print num entities # DO fix
        # num = num_entities(self.session, spider.itemcls())
        # print(f"Number of stored {entitycls} on pipeline end: {num}")

        self._session.close()

    def process_item(self, item: TItem, spider: Spider) -> TItem:
        """Process an item; subclasses must return the original item instance
        (ListingItem or SourceItem)."""
        raise NotImplementedError


class ListingStorer(Storer[Listing, ListingItem]):
    def __init__(self, entitycls: type[Listing] = Listing, **kwargs) -> None:
        super().__init__(entitycls, **kwargs)
        self.products = pd.read_sql_table(Product.__tablename__, self._engine)

    # entitycls arg is included just for conformity with signature of base method
    @classmethod
    def from_crawler(cls, crawler, entitycls: type[Listing] | None = None):
        entitycls_ = crawler.settings.get("LISTING_CLASS", Listing)
        if entitycls_ == Listing:
            logging.getLogger("crawler").warning(f"Using default entitycls: {Listing}")
        return super().from_crawler(crawler, entitycls=entitycls_)

    def mapper(self, item: ListingItem) -> Listing:
        # parse price attributes from item
        def parse_price(price: str | None) -> float | None:
            if price is None:
                return None
            return float(price.replace("€", "").replace(",", ".").strip())

        price_data = {}
        for attr in item.get_price_attrs():
            price_data[attr] = parse_price(getattr(item, attr))

        # create the listing
        listing = self.entitycls(item.text, **price_data)

        # create new attributes
        listing.created_at = self.now

        # return
        return listing

    def process_item(self, item: ListingItem, spider: Spider) -> ListingItem:
        # create a copy to be returned (so that the output feeds are same as input)
        item_ret = replace(item)

        # create listing object from item
        listing = self.mapper(item)

        # add to session
        self._add_to_session(listing, spider)

        # return original item
        return item_ret


class SourceStorer(Storer):
    def __init__(self, entitycls: type[Source] = Source, **kwargs) -> None:
        super().__init__(entitycls, **kwargs)

    def process_item(self, item: SourceItem, spider: Spider) -> SourceItem:
        # create copy to be returned
        it = replace(item)

        # find parent
        parent = None
        parent_url = it.parent_url
        # look in session
        if parent_url:
            parents = [
                src
                for src in self._session.new
                if getattr(src, Source.url.key) == parent_url
            ]
            if len(parents) > 1:
                spider.logger.warning(f"{it} has multiple parents in sess: {parents}")
            elif len(parents) == 1:
                parent = parents[0]
        # check
        if parent_url and parent is None:
            raise DropItem(f"Could not find parent for {it}")
        if it.level > 0 and parent is None:
            raise DropItem(f"{it} has non-zero level and no parent")

        # create source item
        src = Source(**asdict(it))
        src.parent = parent
        src.created_at = self.now

        # add item
        self._session.add(src)
        self._added.append(src)

        # return item
        return item


def process_json_feed(json_path: str | Path, **pipe_kwargs) -> None:
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
