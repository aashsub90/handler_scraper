import logging

logging.basicConfig(
    format="%(asctime)s - [%(threadName)s] - [%(levelname)s] - %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


class Producer(object):
    def __init__(self, url_queue, filepath):
        self.queue = url_queue
        self.filepath = filepath

    def read_file(self):
        """Reads the URLs from the file and publishes it to the URL queue
        """
        log.info("Reading URLs from file")
        # Read all URLs from file
        with open(self.filepath, "r") as f:
            for url in f.readlines():
                self.publish(url.strip(" \n"))
        return

    def publish(self, url):
        """Writes the URL to the queue

        Arguments:
            url {string} -- String represeting a URL
        """
        log.info("Publish URL to queue: {}".format(url))
        # Write URL to Queue for consumption
        self.queue.put(url)
        return
