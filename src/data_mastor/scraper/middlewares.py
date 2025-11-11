import os
import socket
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Self

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
        dlmw = cls()
        crawler.signals.connect(dlmw.spider_opened, signal=signals.spider_opened)
        return dlmw

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
    """Spider-agnostic spider middleware to save HTML responses to files.

    This middleware saves HTML responses to disk when enabled via the SAVE_HTML setting.

    For spiders in local mode (scraping from local files), this middleware also rewrites
    Request URLs to point to the saved local HTML files instead of the original URLs.

    Configuration:
        Enable in settings.py or custom_settings:

        SPIDER_MIDDLEWARES = {
            'data_mastor.scraper.middlewares.ResponseSaverDLMW': 950,
        }

        # Optional: enable HTML saving (default: False)
        SAVE_HTML = True
    """

    def _generate_html_path(self, out_dir: Path, url: str) -> Path:
        """Generate a filename from a URL.

        Args:
            url: The URL string

        Returns:
            A safe filename for saving the HTML content
        """
        if url.startswith("file://"):
            # For local files, use the filename
            return Path(url.replace("file://", ""))
        # For web URLs, create a safe filename from the last part of URL
        parts = url.rstrip("/").split("/")
        filename = parts[-1]
        # Remove query parameters and ensure .html extension
        filename = filename.split("?")[0]
        if not filename.endswith(".html"):
            filename += ".html"
        return out_dir / filename

    def _generate_html_url(self, out_dir: Path, url: str) -> str:
        html_path = self._generate_html_path(out_dir, url)
        return f"file://{html_path.absolute()}"

    def process_spider_output(
        self,
        response: Response,
        result: Iterable[Request | ItemAdapter],
        spider: Spider,
    ):
        """Process spider output, saving HTML and rewriting URLs for local mode.

        Args:
            response: The response being processed
            result: An iterable of Request and/or Item objects
            spider: The spider instance

        Yields:
            Request and/or Item objects from result, with Request URLs rewritten
            to local file paths if in local mode
        """
        # Get output directory from settings - abort if not set
        out_dir = spider.settings.get("OUT_DIR")
        if not out_dir:
            abort(spider, "OUT_DIR setting is required for ResponseSaverDLMW")
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Save response as html (if SAVE_HTML is true)
        save_html = spider.settings.getbool("SAVE_HTML", False)
        if save_html:
            html_path = self._generate_html_path(out_dir, response.url)
            with open(html_path, "wb") as file:
                file.write(response.body)

        # Redirect requests to a previously saved html file (if in local mode)
        for item in result:
            if isinstance(item, Request) and response.url.startswith("file://"):
                html_url = self._generate_html_url(out_dir, item.url)
                item = item.replace(url=html_url)
            yield item
