from dataclasses import asdict, replace
from datetime import datetime

from deepdiff import DeepDiff
from sqlalchemy import Column, Float, ForeignKey, Table, UniqueConstraint, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    relationship,
)
from sqlalchemy.types import DateTime, Integer, String


class Base(MappedAsDataclass, DeclarativeBase):
    @classmethod
    def members_to_update(cls) -> list[str]:
        raise NotImplementedError

    def update_from_other(self, other):
        self_copy = replace(self)
        for key in self.members_to_update():
            value_in_self = getattr(self, key)
            value_in_other = getattr(other, key)
            if value_in_self != value_in_other:
                setattr(self, key, value_in_other)
        excl = [f"root[{key}]" for key in self.__table__.columns]
        diff = DeepDiff(asdict(self_copy), asdict(self), exclude_paths=excl)
        return diff

    @classmethod
    def num_entities(cls, session):
        return len(session.scalars(select(cls)).all())


sources_to_tags = Table(
    "sources_to_tags",
    Base.metadata,
    Column("source_id", ForeignKey("sources.id"), primary_key=True),
    Column("tag_id", ForeignKey("tags.id"), primary_key=True),
)


class Source(Base):
    __tablename__ = "sources"
    URL_LENGTH = 150

    # id
    id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True, repr=True, init=False
    )
    # url
    url: Mapped[str] = mapped_column(String(URL_LENGTH), repr=True, init=True)
    # parent url
    parent_url: Mapped[str] = mapped_column(String(URL_LENGTH), repr=True, init=True)
    # parent id
    parent_id: Mapped[int] | None = mapped_column(
        Integer, ForeignKey(f"{__tablename__}.id"), repr=True, init=False
    )
    # adjacency relationship
    parent: Mapped["Source| None"] = relationship(
        "Source", remote_side=[id], repr=False, init=False
    )
    # level
    level: Mapped[int] = mapped_column(default=None, repr=True)
    # datetime of creation or update
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), init=False, repr=False
    )
    # include (manually specified)
    include: Mapped[bool] = mapped_column(default=True, init=False, repr=False)
    # status of last usage access
    status: Mapped[int] = mapped_column(
        default=200, nullable=True, repr=False, init=False
    )

    # relationship on the 1-side: Source-Listings -> 1-many -> parent-children
    listings: Mapped[list["Listing"]] = relationship(init=False, repr=False)
    # relationship with tags (many-many)
    tags: Mapped[list["Tag"]] = relationship(
        secondary="sources_to_tags", init=False, back_populates="sources", repr=False
    )

    # constraint
    __table_args__ = (UniqueConstraint(url, parent_id, name="unique_source"),)

    @classmethod
    def members_to_update(cls):
        return [attr.key for attr in [Source.created_at, Source.status]]

    def __repr__(self) -> str:
        return f"Source({self.id},{self.url},{self.parent_id},{self.level})"

    def _run_hook_while_traversing(self, session, hook):
        source = self
        while True:
            hook(source)
            if not source.parent:
                break
            source = source.parent

    def calc_full_url(self, session):
        fullurl_parts = [self.url]

        def append_parent_url(source):
            if source.parent:
                fullurl_parts.append(source.parent.url)

        self._run_hook_while_traversing(session, append_parent_url)

        fullurl_parts.reverse()
        self.full_url = "/".join(fullurl_parts)
        return self.full_url

    def calc_all_tags(self, session):
        all_tags = []

        def append_tags(source):
            if source.tags:
                for t in source.tags.split(","):
                    all_tags.append(t)

        self._run_hook_while_traversing(session, append_tags)

        self.all_tags = all_tags
        return self.all_tags


tags_to_tags = Table(
    "tags_to_tags",
    Base.metadata,
    Column(
        "parent_id", Integer, ForeignKey("tags.id"), primary_key=True, nullable=True
    ),
    Column("child_id", Integer, ForeignKey("tags.id"), primary_key=True, nullable=True),
)


class Tag(Base):
    __tablename__ = "tags"
    NAME_LENGTH = 40

    # id
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    # name
    name: Mapped[str] = mapped_column(String(NAME_LENGTH))
    # relationship with sources (many-many)
    sources: Mapped[list["Source"]] = relationship(
        secondary="sources_to_tags", init=False, back_populates="tags"
    )
    # relationship with self (many-many)
    parents: Mapped[list["Tag"]] = relationship(
        "Tag",
        secondary=tags_to_tags,
        primaryjoin=id == tags_to_tags.c.child_id,
        secondaryjoin=id == tags_to_tags.c.parent_id,
        back_populates="children",
    )
    children: Mapped[list["Tag"]] = relationship(
        "Tag",
        secondary=tags_to_tags,
        primaryjoin=id == tags_to_tags.c.parent_id,
        secondaryjoin=id == tags_to_tags.c.child_id,
        back_populates="parents",
    )


class Product(Base):
    __tablename__ = "products"
    NAME_LENGTH = 50

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    name: Mapped[str] = mapped_column(String(NAME_LENGTH), nullable=False)

    # relationship in 1-side: Product-Listings -> 1-many -> parent-children
    listings: Mapped[list["Listing"]] = relationship(init=False)

    def __repr__(self) -> str:
        return f"Product({self.id},{self.name})"


class Listing(Base):
    __tablename__ = "listings"
    TEXT_LENGTH = 150

    # id
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    # name
    text: Mapped[str] = mapped_column(String(TEXT_LENGTH), init=True)
    # datetime of creation by scraping session
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, init=False
    )
    # corresponding source id
    source_id: Mapped[int | None] = mapped_column(
        ForeignKey(Source.id, onupdate="CASCADE", ondelete="SET NULL"), init=False
    )
    # corresponding product id # TODO define product relationship?
    product_id: Mapped[int | None] = mapped_column(
        ForeignKey(Product.id, onupdate="CASCADE", ondelete="SET NULL"), init=False
    )
    # price
    price: Mapped[float] = mapped_column(Float, nullable=True, init=True)

    # constraint
    __table_args__ = (
        UniqueConstraint(text, created_at, source_id, name="unique_listing"),
    )
