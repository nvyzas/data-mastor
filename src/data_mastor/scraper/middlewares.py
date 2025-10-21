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

        # TODO: remove local mode checking in this middleware (just dont add it instead)
        if spider.local_mode:
            # enforce file:// prefix to prevent non-local requests
            url = request.url
            if not url.startswith("file://"):
                abort(spider, f"Non-local URL request '{url}' in local-mode")
            return None

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

        # skip privacy-related network checks if scraping locally
        if spider.local_mode:
            return

        # leaktest
        _do_leaktest = not os.environ.get(ENVVAR_NO_LEAK_TEST, False)
        if not _do_leaktest:
            spider.logger.warning("DNS leak test is disabled!")
        # check proxy
        self.proxy_ip = os.environ.get(ENVVAR_PROXY_IP)
        if self.proxy_ip:
            leaktest_script = os.environ.get(
                ENVVAR_PROXY_LEAKTEST_SCRIPT, "leaktest.sh"
            )
            # check for leaks
            if _do_leaktest and _is_leaking(leaktest_script):
                self.proxy_ip = ""
                spider.logger.warning("Proxy dnsleak test failed!")

        # check regular network interface
        if not self.proxy_ip:
            # check interface
            self.interface_ip = ""
            interface = os.environ.get(ENVVAR_ALLOWED_INTERFACE)
            if interface:
                # REF: split up try/except blocks
                try:
                    is_up = _interface_is_up(interface)
                    self.interface_ip = _interface_ip(interface)
                except ValueError as exc:
                    abort(spider, f"{exc}")
                else:
                    if not is_up:
                        abort(spider, f"Allowed interface '{interface}' is down")
                    if not self.interface_ip:
                        abort(spider, f"Allowed interface '{interface}' has no ip")
            # check for leaks
            leaktest_script = os.environ.get(ENVVAR_LEAKTEST_SCRIPT, "leaktest.sh")
            if _do_leaktest and _is_leaking(leaktest_script):
                abort(spider, "Dnsleak test failed!")


class ResponseSaverDLMW:
    def process_response(self, request, response: Response, spider: Spider):
        # Called with the response returned from the downloader.
        out_dir = spider.settings["OUT_DIR"]
        html_file = spider.html_namer(response)
        with open(out_dir / html_file, "wb") as file:
            file.write(response.body)

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response
