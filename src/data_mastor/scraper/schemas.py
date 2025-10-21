from dataclasses import dataclass


@dataclass
class SourceItem:
    url: str
    parent_url: str
    level: int


@dataclass
class ListingItem:
    text: str
    price: str | None
