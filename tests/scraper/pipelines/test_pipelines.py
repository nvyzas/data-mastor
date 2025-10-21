import pytest
from scrapy.exceptions import DropItem
from sqlalchemy import select
from sqlalchemy.orm.session import Session

from data_mastor.scraper.models import Listing, Source
from data_mastor.scraper.pipelines import ListingStorer, SourceStorer, process_items
from data_mastor.scraper.schemas import ListingItem, SourceItem

s0 = SourceItem("lvl0", "", 0)
s1 = SourceItem("lvl1", "lvl0", 1)
s2 = SourceItem("lvl2", "lvl1", 2)


class TestSourceStorer:
    @pytest.fixture
    def pipe(self) -> SourceStorer:
        return SourceStorer()

    def test_pipe_normal(self, session: Session, pipe: SourceStorer):
        process_items([s0, s1, s2], pipe)
        stmt = select(Source)
        entities = session.scalars(stmt).all()
        assert len(entities) == 3

    def test_pipe_broken_hierarchy(self, pipe: SourceStorer):
        with pytest.raises(DropItem):
            process_items([s1, s2], pipe)


l1 = ListingItem("text1", "1")
l2 = ListingItem("text2", "2")
listings = [l1, l2]


class TestListingStorer:
    @pytest.fixture
    def pipe(self) -> ListingStorer:
        return ListingStorer()

    def test_lstpipe(self, session: Session, pipe: ListingStorer):
        processed = process_items(listings, pipe)
        entities = session.scalars(select(Listing)).all()
        assert len(entities) == 2
        assert processed == listings


if __name__ == "__main__":
    pytest.main([__file__])
