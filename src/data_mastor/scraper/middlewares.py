import os
import socket
import subprocess

import psutil
from scrapy import Request, Spider, signals
from scrapy.http import Response

from data_mastor.scraper.utils import abort, is_bad_user_agent

ENVVAR_PROXY_IP = "PROXY_IP"
ENVVAR_NO_LEAK_TEST = "NO_LEAK_TEST"
ENVVAR_NO_UA_TEST = "NO_UA_CHECK"
ENVVAR_ALLOWED_INTERFACE = "ALLOWED_INTERFACE"
ENVVAR_PROXY_LEAKTEST_SCRIPT = "PROXY_LEAKTEST_SCRIPT"
ENVVAR_LEAKTEST_SCRIPT = "LEAKTEST_SCRIPT"


# dns leak test utility function
def _is_leaking(script, num_tries=3):
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
def _interface_ip(interface_name):
    interfaces = psutil.net_if_addrs()
    if interface_name in interfaces:
        for addr in interfaces[interface_name]:
            if addr.family == socket.AF_INET:
                return addr.address
    else:
        raise ValueError(f"Interface '{interface_name}' does not exist!")
    return ""


# utility function to get interface up/down status
def _interface_is_up(interface_name):
    interfaces = psutil.net_if_stats()
    if interface_name in interfaces:
        return interfaces[interface_name].isup
    else:
        raise ValueError(f"Interface '{interface_name}' does not exist!")


# MIDDLEWARES
# Not all methods need to be defined in middleware classes. If a method is not defined,
# scrapy acts as if the downloader middleware does not modify the passed objects.


class PrivacyCheckerDLMW:
    @classmethod
    def from_crawler(cls, crawler):
        dlmw = cls()
        crawler.signals.connect(dlmw.spider_opened, signal=signals.spider_opened)
        return dlmw

    def process_request(self, request: Request, spider: Spider):
        # Check the User-Agent header
        if self._check_ua:
            ua = request.headers.get("User-Agent")
            if ua is None:
                abort(spider, "There is no 'User-Agent' field in the request headers")
            else:
                ua_str = ua.decode("utf-8")
                if is_bad_user_agent(ua_str):
                    abort(spider, f"User-Agent header '{ua_str}' is not permitted")

        # set proxy/bindaddress
        if self.proxy_ip:
            request.meta["proxy"] = self.proxy_ip
            if self.interface_ip:
                msg = "Both 'proxy' and 'bindaddress' are set. Ignoring bindaddress"
                spider.logger.warning(msg)
        elif self.interface_ip:
            request.meta["bindaddress"] = self.interface_ip
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
            spider.logger.warning("User-agent header check is off!")

        # Initialize proxy and interface IP attributes
        self.proxy_ip = ""
        self.interface_ip = ""

        # leaktest
        _do_leaktest = not os.environ.get(ENVVAR_NO_LEAK_TEST, False)
        if not _do_leaktest:
            spider.logger.warning("DNS leak test is disabled!")
        # check proxy
        proxy_ip = os.environ.get(ENVVAR_PROXY_IP)
        if proxy_ip:
            leaktest_script = os.environ.get(
                ENVVAR_PROXY_LEAKTEST_SCRIPT, "leaktest.sh"
            )
            # check for leaks
            if _do_leaktest and _is_leaking(leaktest_script):
                spider.logger.warning("Proxy dnsleak test failed!")
            else:
                self.proxy_ip = proxy_ip

        # check regular network interface
        if not self.proxy_ip:
            # check interface
            interface = os.environ.get(ENVVAR_ALLOWED_INTERFACE)
            if interface:
                # REF: split up try/except blocks
                try:
                    is_up = _interface_is_up(interface)
                    interface_ip = _interface_ip(interface)
                except ValueError as exc:
                    abort(spider, f"{exc}")
                else:
                    if not is_up:
                        abort(spider, f"Allowed interface '{interface}' is down")
                    if not interface_ip:
                        abort(spider, f"Allowed interface '{interface}' has no ip")
                    self.interface_ip = interface_ip
            # check for leaks
            leaktest_script = os.environ.get(ENVVAR_LEAKTEST_SCRIPT, "leaktest.sh")
            if _do_leaktest and _is_leaking(leaktest_script):
                abort(spider, "Dnsleak test failed!")


class ResponseSaverDLMW:
    """Spider-agnostic middleware to save HTML responses to files.

    This middleware saves HTML responses to disk without requiring a specific spider class.
    It uses duck-typing to check for optional spider attributes and falls back to defaults.

    Configuration:
        Enable in settings.py or custom_settings:

        DOWNLOADER_MIDDLEWARES = {
            'data_mastor.scraper.middlewares.ResponseSaverDLMW': 950,
        }

        # Optional: configure output directory (default: 'out')
        HTML_SAVE_DIR = 'path/to/output'

    Spider attributes (all optional):
        - html_save_dir: Directory to save HTML files (Path or str)
        - html_namer: Callable that takes a response and returns filename (str)

    Example spider usage:
        class MySpider(scrapy.Spider):
            name = 'myspider'

            # Optional: custom directory
            html_save_dir = Path('custom/output')

            # Optional: custom naming function
            def html_namer(self, response):
                return f"{self.name}_{hash(response.url)}.html"
    """

    def process_response(self, request, response: Response, spider: Spider):
        # Called with the response returned from the downloader.

        # Get output directory: spider attribute > spider settings > middleware settings > default
        out_dir = getattr(spider, "html_save_dir", None)
        if out_dir is None:
            out_dir = spider.settings.get("OUT_DIR") or spider.settings.get(
                "HTML_SAVE_DIR", "out"
            )

        # Convert to Path if needed
        from pathlib import Path

        out_dir = Path(out_dir)

        # Ensure directory exists
        out_dir.mkdir(parents=True, exist_ok=True)

        # Get filename: use spider's html_namer if available, otherwise use default
        html_namer = getattr(spider, "html_namer", None)
        if html_namer and callable(html_namer):
            html_file = html_namer(response)
        else:
            # Default naming: extract from URL or use hash
            url = response.url
            if url.startswith("file://"):
                # For local files, use the filename
                html_file = Path(url.replace("file://", "")).name
            else:
                # For web URLs, create a safe filename from the last part of URL
                parts = url.rstrip("/").split("/")
                filename = parts[-1] if parts else "response"
                # Remove query parameters and ensure .html extension
                filename = filename.split("?")[0]
                if not filename.endswith(".html"):
                    filename += ".html"
                html_file = filename

        # Save the response body
        with open(out_dir / html_file, "wb") as file:
            file.write(response.body)

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response
