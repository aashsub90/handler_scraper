from threading import Thread
from queue import Queue, Empty
import logging
import json
import argparse

from producer import *
from consumer import *

# Initialize Queues
url_queue = Queue()
data_queue = Queue()
result_queue = Queue()


def write_output(output_file):
    """Write contents of Output queue to a file

    Arguments:
        output_file {string} -- Complete path to output file
    """
    output = {"results": []}
    while(not result_queue.empty()):
        output["results"].append(result_queue.get())

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(output, indent=4, separators=(",", ":")))


def main(input_file, output_file, consumer_threads):

    global session, url_queue, data_queue, result_queue

    # Start a thread for the instance of Producer class
    producer = Producer(url_queue, input_file)
    producer_thread = Thread(name="Producer", target=producer.read_file)
    producer_thread.start()

    # Start thread for Consumer instance - URL consumption and parsing
    url_consumer = []
    for i in range(consumer_threads):
        url_consumer = Consumer(url_queue, data_queue, extract=False)
        url_consumer_thread = Thread(
            name="URL Consumer", target=url_consumer.consume_url)
        url_consumer_thread.start()
        url_consumer.append(url_consumer_thread)

    # Start threads for Consumer instance (Data consumption and extraction)
    data_consumers = []
    for i in range(consumer_threads):
        data_consumer = Consumer(url_queue, data_queue, result_queue)
        data_consumer_thread = Thread(name="Data_Consumer_"+str(i),
                                      target=data_consumer.consume_data)
        data_consumer_thread.start()
        data_consumers.append(data_consumer_thread)

    # Ensure threads complete tasks
    producer_thread.join()

    for t in url_consumer:
        t.join()

    for t in data_consumers:
        t.join()

    # Write output to a file
    write_output(output_file)
    # Close session created
    session.close()
    log.info("Scraper execution complete")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - [%(threadName)s] - [%(levelname)s] - %(message)s", level=logging.INFO)
    log = logging.getLogger(__name__)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file", "-i",
        help="Path to input file containing URLs (default=./seed_urls.json)",
        dest="input_file",
        default="seed_urls.txt")
    parser.add_argument(
        "--output_file", "-o",
        help="Path to output file (default=./output.json)",
        dest="output_file",
        default="output.json")
    parser.add_argument(
        "--cthread", "-c",
        help="Number of data consumer threads (default=5)",
        dest="consumer_threads",
        default=5)
    args = parser.parse_args()
    main(args.input_file, args.output_file, args.consumer_threads)
