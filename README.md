# handler_scraper

## Description
Scraper that can extract media handles from a given list of URLs

## Implementation:
* The scraper is implemented using a publisher/subscriber model with a Queue structure to store data.
* Publisher reads urls from a file and writes it to a URL Queue [Instance runs on a single thread since reading the file and writing to the queue is the only task]
* Consumers are of 2 types (parser/extractor):
* Parser consumers read from the URL Queue and make a request to obtain the html page. They parse the page to find potential links and write the data to a Data Queue.
* Extractor consumers read from the Data Queue and extract the handles from the data. They store the results in an output queue.
* The scraper script is the main script that creates instances of the Producer and Consumers, and writes the extracted data in JSON format to an output file

## Execution Steps:

* Download the attached tar file
  * Extract tar -xvf handler_scraper.tar
* Create a virtual environment in python
  * virtualenv <env_name>
  * [OR] python3 -m venv env
  * source <env_name>/bin/activate
* Install dependencies
  * cd handler_scraper/
  * pip3 install -r requirements.txt
* The seed_urls.txt file contains sample urls and the output file defaults to output.json
* Run the scraper:
  * python3 scraper.py (Use -h for options)
* The output is written in JSON format(tested with jsonlint) to output.json
