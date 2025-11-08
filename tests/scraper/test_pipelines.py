import pytest
from scrapy.exceptions import DropItem
from sqlalchemy import select
from sqlalchemy.orm.session import Session, sessionmaker

from data_mastor.scraper.models import Listing, Source
from data_mastor.scraper.pipelines import ListingStorer, SourceStorer, process_items
from data_mastor.scraper.schemas import ListingItem, SourceItem

s0 = SourceItem("lvl0", "", 0)
s1 = SourceItem("lvl1", "lvl0", 1)
s2 = SourceItem("lvl2", "lvl1", 2)


@pytest.fixture(autouse=True)
def reset_db(reset_db):
    pass


class TestSourceStorer:
    @pytest.fixture
    def pipe(self) -> SourceStorer:
        return SourceStorer()

    def test_pipe_normal(
        self, sessmkr: sessionmaker[Session], pipe: SourceStorer
    ) -> None:
        assert len(sessmkr().scalars(select(Source)).all()) == 0
        process_items([s0, s1, s2], pipe)
        assert len(sessmkr().scalars(select(Source)).all()) == 3

    def test_pipe_broken_hierarchy(
        self, sessmkr: sessionmaker[Session], pipe: SourceStorer
    ) -> None:
        assert len(sessmkr().scalars(select(Source)).all()) == 0
        with pytest.raises(DropItem):
            process_items([s1, s2], pipe)
        assert len(sessmkr().scalars(select(Source)).all()) == 0


l1 = ListingItem("text1", "1")
l2 = ListingItem("text2", "2")
listings = [l1, l2]


class TestListingStorer:
    @pytest.fixture
    def pipe(self) -> ListingStorer:
        return ListingStorer()

    def test_lstpipe(self, sessmkr: sessionmaker[Session], pipe: ListingStorer) -> None:
        assert len(sessmkr().scalars(select(Listing)).all()) == 0
        process_items(listings, pipe)
        assert len(sessmkr().scalars(select(Listing)).all()) == 2


if __name__ == "__main__":
    pytest.main([__file__])
