import logging
from queue import Empty
import time
import validators
import asyncio
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urlparse, parse_qs
import requests
import requests.exceptions as exceptions

logging.basicConfig(
    format="%(asctime)s - [%(threadName)s]- [%(levelname)s] - %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# Initiate HTML session
session = HTMLSession()
session.browser
session.max_redirects = 3

# Supress warnings
requests.packages.urllib3.disable_warnings()


class Consumer(object):
    def __init__(self, url_queue, data_queue, result_queue=None, extract=True):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) \
                AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 \
                Safari/537.36"}
        if not extract:
            self.data_queue = data_queue
            self.url_queue = url_queue
        else:
            self.data_queue = data_queue
            self.result_queue = result_queue

    def consume_url(self):
        """Fetches the URL from the URL queue and initiates the file download
        """
        exit_counter = 0
        while exit_counter <= 5:
            try:
                # Fetch URL from the URL queue
                url = self.url_queue.get(block=False)
                # Reset counter if URL fetched
                exit_counter = 0 if url else exit_counter
                log.info("Consuming URL for download: {}".format(url))
                # Initiate download of page
                self.download(url)
                self.url_queue.task_done()
            except Empty:
                # Wait for new URLs in the queue
                time.sleep(5)
                exit_counter += 1

    def consume_data(self):
        """Fetch the data from the Data queue and initiate parsing
        """
        exit_counter = 0
        while exit_counter <= 5:
            try:
                # Fetch data from the Data queue
                data = self.data_queue.get(block=False)
                # Reset counter if URL fetched
                exit_counter = 0 if data else exit_counter
                log.info("Consuming data for parsing: {}".format(data["url"]))
                # Initiate parsing phase on data
                self.extract(data)
                self.data_queue.task_done()
            except Empty:
                # Wait for new URLs in the queue
                time.sleep(5)
                exit_counter += 1

    def validate_url(self, url):
        """Check validity of the url

        Arguments:
            url {string} -- String representing a URL

        Returns:
            {boolean} -- True if URL is valid
        """
        return validators.url(url)

    def parse(self, data):
        """Parse the HTML page to find links with handlers

        Returns:
            {set} -- Set of parsed links
        """
        social_links = set()
        soup = BeautifulSoup(data, "html.parser")
        pattern = "^https?://((www.)?twitter|www\.facebook|play\.google|(apps|itunes)\.apple)+.*"
        selectors = soup.findAll("a", attrs={"href": re.compile(pattern)})
        for link in selectors:
            # Store parsed media URLs
            social_links.add(link.get("href").strip(" /"))
        return social_links

    def download(self, url):
        """Downloads the HTMl page for the given URL and writes
        the parsed social links to the Data queue

        Arguments:
            url {string} -- String representing a URL
        """
        global session
        if self.validate_url(url):
            social_links = set()
            response = None
            log.info("Downloading page for: {}".format(url))

            try:
                # Fetch the page for the given URL
                response = session.get(
                    url, headers=self.headers, verify=False,
                    timeout=10, allow_redirects=True)
                response.raise_for_status()
            except exceptions.Timeout as e:
                log.error("Timeout exception occurred for %s", url)
            except exceptions.ConnectionError as e:
                log.error("Connection error occured for %s", url)
            except exceptions.TooManyRedirects as e:
                log.error("Too many redirects occurred for %s", url)
            except exceptions.HTTPError as e:
                log.error("HTTP exception occurred. %s", e)
            except Exception as e:
                log.error("Exception is %s", e)

            # Parse HTML page for social media URLs for a valid response
            if(response and response.status_code == 200):
                data = response.text
                social_links = self.parse(data)
                # Check condition to render JavaScript
                if len(social_links) == 0:
                    response.html.render()
                    data = response.html.html
                    social_links = self.parse(data)
                # Write extracted media urls to Data queue
                if(len(social_links) != 0):
                    extracted_links = {"url": url,
                                       "extracted_links": list(social_links)}
                    log.info("Pushing to data queue: {}".format(
                        extracted_links))
                    self.data_queue.put(extracted_links, block=True)
        else:
            log.error("Malformed URL obtained: %s", url)
        return

    def extract(self, data):
        """Parse the social media URLs to extract handles and write it to
           Output queue

        Arguments:
            data {Dict} -- Each object has the url and a list of extracted
            media URLs
        """
        handles = {}
        url = data["url"]
        links = data["extracted_links"]
        # Parse each link to find the required handle
        for link in links:
            if "twitter" in link:
                twitter_handle = link.split("/")[-1]
                handles["twitter"] = twitter_handle
            elif "facebook" in link:
                fb_handle = link.split("/")[-1]
                handles["facebook"] = fb_handle
            elif "apple" in link:
                ios_handle = re.findall("id[0-9]+", link)[0]
                handles["ios"] = ios_handle
            elif "google" in link:
                x = urlparse(link)
                query = parse_qs(x.query)
                google_handle = query["id"][0]
                handles["google"] = google_handle
        log.info("Handles for {} are:\n\n{}".format(url, handles))
        output = {url: handles}
        # Write the handles and url to Output queue
        self.result_queue.put(output)
        return
