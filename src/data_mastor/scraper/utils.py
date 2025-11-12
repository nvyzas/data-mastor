import logging
from collections.abc import Callable

from scrapy import Spider

DLMW_KEY = "DOWNLOADER_MIDDLEWARES"
DLMWBASE_KEY = "DOWNLOADER_MIDDLEWARES_BASE"
SPMW_KEY = "SPIDER_MIDDLEWARES"
SPMWBASE_KEY = "SPIDER_MIDDLEWARES"


# used by spiders
def between_middlewares(
    dlmw: dict[str, int],
    after: list[str] | None = None,
    before: list[str] | None = None,
):
    position = 0
    dlmw_str = {
        k.split(".")[-1] if isinstance(k, str) else k.__name__: v
        for k, v in dlmw.items()
    }
    # place it after all 'before'
    after = after or []
    for mw in after:
        mw_pos = dlmw_str.get(mw)
        if mw_pos is None:
            continue
        if mw_pos > position:
            position = mw_pos + 1
    min_position = position
    # place it before all 'after'
    before = before or []
    for mw in before:
        mw_pos = dlmw_str.get(mw)
        if mw_pos is None:
            continue
        if mw_pos < position:
            position = mw_pos - 1
            if position < min_position:
                raise ValueError(
                    f"All 'after' mw should precede 'before' mw: {locals()}"
                )
    max_position = position
    # place it in the middle
    mean_position = (min_position + max_position) / 2
    return mean_position


# used by middlewares
def is_bad_user_agent(user_agent):
    bad_words = ["scrap", "crawl", "spider", "bot"]
    is_empty = user_agent.strip() == ""
    return any(map(lambda word: word in user_agent, bad_words)) or is_empty


# used by middlewares/pipelines
def abort(spider: Spider, exc: Exception | str, if_fail: Callable | None = None):
    if isinstance(exc, str):
        exc = RuntimeError(exc)
    try:
        from twisted.internet import reactor

        spider.logger.exception(exc)
        spider.logger.critical("Closing spider and stopping reactor")
        d = spider.crawler.engine.close_spider(spider, reason=exc)  # type: ignore
        d.addBoth(lambda _: reactor.stop())  # type: ignore
    except Exception as e:
        if if_fail is not None:
            if_fail()
        raise e
    raise exc


# scrapy logging
def configure_scrapy_logging_levels() -> None:
    logging.getLogger("scrapy.core.engine").setLevel("DEBUG")
    logging.getLogger("scrapy.core.scraper").setLevel("INFO")
    logging.getLogger("scrapy.utils.log").setLevel("INFO")
    logging.getLogger("scrapy-playwright").setLevel("INFO")
    logging.getLogger("scrapy_user_agents.user_agent_picker").setLevel("ERROR")
    logging.getLogger("scrapy_user_agents.user_agent_picker").setLevel("ERROR")
    logging.getLogger("scrapy_user_agents.middlewares").setLevel("INFO")
    logging.getLogger("scrapy.addons").setLevel("WARNING")
    logging.getLogger("asyncio").setLevel("INFO")
