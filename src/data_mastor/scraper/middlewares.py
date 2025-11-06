import os
import socket
import subprocess
from pathlib import Path

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
    """Spider-agnostic spider middleware to save HTML responses to files.

    This middleware saves HTML responses to disk when enabled via the SAVE_HTML setting.
    It requires the OUT_DIR setting to be configured and will abort if it's not set.

    For spiders in local mode (scraping from local files), this middleware also rewrites
    Request URLs to point to the saved local HTML files instead of the original URLs.

    Configuration:
        Enable in settings.py or custom_settings:

        SPIDER_MIDDLEWARES = {
            'data_mastor.scraper.middlewares.ResponseSaverDLMW': 950,
        }

        # Required: configure output directory
        OUT_DIR = 'path/to/output'

        # Optional: enable HTML saving (default: False)
        SAVE_HTML = True
    """

    @staticmethod
    def _generate_filename(url: str) -> str:
        """Generate a filename from a URL.

        Args:
            url: The URL string

        Returns:
            A safe filename for saving the HTML content
        """
        if url.startswith("file://"):
            # For local files, use the filename
            return Path(url.replace("file://", "")).name
        else:
            # For web URLs, create a safe filename from the last part of URL
            parts = url.rstrip("/").split("/")
            filename = parts[-1] if parts else "response"
            # Remove query parameters and ensure .html extension
            filename = filename.split("?")[0]
            if not filename.endswith(".html"):
                filename += ".html"
            return filename

    @staticmethod
    def _is_local_mode(spider: Spider) -> bool:
        """Check if spider is in local mode (scraping from local files).

        Args:
            spider: The spider instance

        Returns:
            True if at least one start_url starts with 'file://', False otherwise
        """
        start_urls = getattr(spider, "start_urls", [])
        return any(url.startswith("file://") for url in start_urls)

    def process_spider_output(self, response: Response, result, spider: Spider):
        """Process spider output, saving HTML and rewriting URLs for local mode.

        Args:
            response: The response being processed
            result: An iterable of Request and/or Item objects
            spider: The spider instance

        Yields:
            Request and/or Item objects from result, with Request URLs rewritten
            to local file paths if in local mode
        """
        # Check if we should save HTML
        save_html = spider.settings.getbool("SAVE_HTML", False)

        saved_file_path = None
        if save_html:
            # Get output directory from settings - abort if not set
            out_dir = spider.settings.get("OUT_DIR")
            if not out_dir:
                abort(spider, "OUT_DIR setting is required for ResponseSaverDLMW")

            # Convert to Path if needed
            out_dir = Path(out_dir)

            # Ensure directory exists
            out_dir.mkdir(parents=True, exist_ok=True)

            # Generate filename from URL
            html_file = self._generate_filename(response.url)

            # Save the response body
            saved_file_path = out_dir / html_file
            with open(saved_file_path, "wb") as file:
                file.write(response.body)

        # Check if we're in local mode
        is_local_mode = self._is_local_mode(spider)

        # Process the result
        for item in result:
            # If in local mode and item is a Request, rewrite URL to local file
            if is_local_mode and isinstance(item, Request) and saved_file_path:
                # Rewrite the request URL to point to the saved local file
                item = item.replace(url=f"file://{saved_file_path.absolute()}")

            yield item
