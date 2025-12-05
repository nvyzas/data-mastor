import os
import socket
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Self, cast

import psutil
from itemadapter import ItemAdapter
from scrapy import Request, Spider, signals
from scrapy.http import Response

from data_mastor.scraper.utils import abort, is_bad_user_agent

ENVVAR_PROXY_IP = "PROXY_IP"
ENVVAR_NO_LEAK_TEST = "NO_LEAK_TEST"
ENVVAR_NO_UA_TEST = "NO_UA_CHECK"
ENVVAR_ALLOWED_INTERFACE = "ALLOWED_INTERFACE"
ENVVAR_PROXY_LEAKTEST_SCRIPT = "PROXY_LEAKTEST_SCRIPT"
ENVVAR_LEAKTEST_SCRIPT = "LEAKTEST_SCRIPT"

NO_UA_CHECK_WARNING = "User-agent header check is off!"
NO_LEAK_TEST_WARNING = "DNS leak test is disabled!"


# dns leak test utility function
def _is_leaking(script, num_tries=3) -> bool:
    print(f"PrivacyChecker: running dnsleak test: '{script}'")
    out = ""
    for i in range(num_tries):
        # check
        print(f"Check {i + 1}")
        result = subprocess.run(
            script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        # print
        out = result.stdout
        err = result.stderr
        if out:
            out = f"{script} (stdout):" + out.strip()
            print(out)
        if err:
            err = f"{script} (stderr):" + err.strip()
            print(err)
        # break
        if result.returncode == 0:
            break
    if "DNS is not leaking." in out:
        return False
    return True


# utility function to get interface ip
def _interface_ip(interface_name) -> str:
    interfaces = psutil.net_if_addrs()
    if interface_name in interfaces:
        for addr in interfaces[interface_name]:
            if addr.family == socket.AF_INET:
                return addr.address
    else:
        raise ValueError(f"Interface '{interface_name}' does not exist!")
    return ""


# utility function to get interface up/down status
def _interface_is_up(interface_name: str) -> bool:
    interfaces = psutil.net_if_stats()
    if interface_name in interfaces:
        return interfaces[interface_name].isup
    else:
        raise ValueError(f"Interface '{interface_name}' does not exist!")


# MIDDLEWARES
# Not all methods need to be defined in middleware classes. If a method is not defined,
# scrapy acts as if the downloader middleware does not modify the passed objects.


class PrivacyCheckerDlMw:
    @classmethod
    def from_crawler(cls, crawler) -> Self:
        mw = cls()
        crawler.signals.connect(mw.spider_opened, signal=signals.spider_opened)
        return mw

    def process_request(self, request: Request, spider: Spider):
        # check the User-Agent header
        if self._check_ua:
            ua = request.headers.get("User-Agent")
            if ua is None:
                abort(spider, "There is no 'User-Agent' field in the request headers")
            else:
                ua_str = ua.decode("utf-8")
                if is_bad_user_agent(ua_str):
                    abort(spider, f"User-Agent header '{ua_str}' is not permitted")

        # set proxy/bindaddress
        if self._proxy_ip:
            request.meta["proxy"] = self._proxy_ip
        elif self._interface_ip:
            request.meta["bindaddress"] = self._interface_ip
        spider.logger.debug(f"request.meta={request.meta}")

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider: Spider):
        spider.logger.info("PrivacyChecker: Spider opened: %s" % spider.name)

        # user-agent check
        self._check_ua = not os.environ.get(ENVVAR_NO_UA_TEST, False)
        if not self._check_ua:
            spider.logger.warning(NO_UA_CHECK_WARNING)

        # set proxy ip
        self._proxy_ip = os.environ.get(ENVVAR_PROXY_IP)

        # allowed interface check
        iface = os.environ.get(ENVVAR_ALLOWED_INTERFACE)
        if not self._proxy_ip and iface:
            # check if interface is up
            try:
                is_up = _interface_is_up(iface)
            except Exception as exc:
                abort(spider, exc)
                raise  # for type-checker/readabilty
            if not is_up:
                abort(spider, f"Allowed interface ({iface}) is down")
            # get interface ip
            try:
                interface_ip = _interface_ip(iface)
            except Exception as exc:
                abort(spider, exc)
                raise  # for type-checker/readability
            if not interface_ip:
                abort(spider, f"Allowed interface ({iface}) has no ip")
            self._interface_ip = interface_ip

        # leaktest
        if os.environ.get(ENVVAR_NO_LEAK_TEST, False):
            spider.logger.warning(NO_LEAK_TEST_WARNING)
            return

        # perform the test to the proxy / allowed interface
        if self._proxy_ip:
            script_var = ENVVAR_PROXY_LEAKTEST_SCRIPT
        else:
            script_var = ENVVAR_LEAKTEST_SCRIPT
        script = os.environ.get(script_var, "leaktest.sh")
        if _is_leaking(script):
            abort(spider, "Dns leak test failed!")


class ResponseSaverSpMw:
    """Saves HTML responses to disk when enabled via the SAVE_HTML setting.

    For spiders in local mode (scraping from local files), this middleware also rewrites
    Request URLs to point to the saved local HTML files instead of the original URLs.
    """

    # SOMEDAY make these into settings
    SUFFIXES = ["_page"]
    SUFFIX = "_pg"

    def __init__(self, save_html: bool, out_dir: Path | None) -> None:
        self.save_html = save_html
        if out_dir is None:
            raise ValueError("save_html is enabled but out_dir is None")
        self.out_dir = out_dir
        self.in_dir: Path | None = None

    @classmethod
    def from_crawler(cls, crawler) -> Self:
        save_html = crawler.settings.getbool("SAVE_HTML", False)
        out_dir = Path(crawler.settings.get("OUT_DIR", None))
        mw = cls(save_html, out_dir)
        crawler.signals.connect(mw.spider_opened, signal=signals.spider_opened)
        return mw

    def spider_opened(self, spider: Spider):
        spider.logger.info("ResponseSaver: Spider opened: %s" % spider.name)
        # check out_dir
        out_dir = Path(spider.crawler.settings.get("OUT_DIR"))
        if self.save_html:
            try:
                out_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                abort(spider, exc)
        # set in_dir if scraping local file
        if (u := spider.start_urls[0]) and u.startswith("file://"):
            in_dir_str = "/".join(u.replace("file://", "").rstrip("/").split("/")[:-1])
            self.in_dir = Path(in_dir_str)

    # used for both reading and writing
    def _html_filename(self, url: str) -> str:
        # get the last part of the url (local or not)
        tip = url.rstrip("/").split("/")[-1]
        # replace forbidden filename characters
        filename = tip.replace("?", "_").replace("=", "")
        # remove ".html" (could be in the middle of local urls)
        filename = filename.replace(".html", "")
        # keep a single unified suffix
        for suffix in self.SUFFIXES:
            filename = filename.replace(suffix, self.SUFFIX)
        # for local urls
        page_split = filename.split(self.SUFFIX)
        if len(page_split) > 2:
            filename = page_split[0] + self.SUFFIX + page_split[-1]
        return filename + ".html"

    def process_spider_output(
        self,
        response: Response,
        result: Iterable[Request | ItemAdapter],
        spider: Spider,
    ):
        # Save response as html (if SAVE_HTML is true
        if self.save_html:
            html_path = self.out_dir / self._html_filename(response.url)
            with open(html_path, "wb") as file:
                file.write(response.body)

        # yield items or Redirect requests to a previously saved html file (if in local mode)
        for item in result:
            if response.url.startswith("file://") and isinstance(item, Request):
                html_path = cast(Path, self.in_dir) / self._html_filename(item.url)
                item = item.replace(url=f"file://{html_path.absolute()}")
            yield item
