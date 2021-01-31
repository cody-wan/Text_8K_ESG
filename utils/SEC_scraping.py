# __author__ = "Cody Wan"
# __credits__ = ["Alex Reed/https://github.com/areed1192"]
# __email__ = "codywan71@gmail.com"

import requests
import re
import unicodedata
import json
import time
import pandas as pd
import traceback
import functools
import logging
import os
from bs4 import BeautifulSoup
from glob import glob  # to locate file name with partial wildcard (i.e. omit date in reading permno cik mapper)
from datetime import datetime


def slow_down(_func=None, *, rate=0.1):
    """
        Sleep given amount of seconds before calling the function
        can be used in function definition
    """

    def decorator_slow_down(func):
        @functools.wraps(func)
        def wrapper_slow_down(*args, **kwargs):
            time.sleep(rate)
            return func(*args, **kwargs)

        return wrapper_slow_down

    if _func is None:
        return decorator_slow_down
    else:
        return decorator_slow_down(_func)


def parse_entries(soup, master_dict_xml):
    """
        parse entries/records from SEC Edgar search query's reponse in xml, as given by soup
        args:
            soup: BeautifulSoup object that has read the edgar search result page as lxml
            master_dict_xml: master dictionary to append scraped results to
        returns:
            None

    """
    # find all entry tags, every entry is a record for a filing
    entries = soup.find_all('entry')
    for entry in entries:
        # read accession number as key value
        accession_num = entry.find('accession-number').text
        master_dict_xml[accession_num] = dict()

        # store category info
        category_info = entry.find('category')
        master_dict_xml[accession_num]['category'] = dict()
        master_dict_xml[accession_num]['category']['label'] = category_info['label']
        master_dict_xml[accession_num]['category']['scheme'] = category_info['scheme']
        master_dict_xml[accession_num]['category']['term'] = category_info['term']

        # store file info (file number, date, size etc.)
        master_dict_xml[accession_num]['file_info'] = dict()
        try:  # not always available
            master_dict_xml[accession_num]['file_info']['act'] = entry.find('act').text
        except AttributeError:
            master_dict_xml[accession_num]['file_info']['act'] = ""
        master_dict_xml[accession_num]['file_info']['file_number'] = entry.find('file-number').text
        master_dict_xml[accession_num]['file_info']['file_number_href'] = entry.find('file-number-href').text
        master_dict_xml[accession_num]['file_info']['filing_date'] = entry.find('filing-date').text
        master_dict_xml[accession_num]['file_info']['filing_href'] = entry.find('filing-href').text
        master_dict_xml[accession_num]['file_info']['filing_type'] = entry.find('filing-type').text
        master_dict_xml[accession_num]['file_info']['form_name'] = entry.find('form-name').text
        master_dict_xml[accession_num]['file_info']['file_size'] = entry.find('size').text
        try:  # link to xbrl/interactive data, not always available
            master_dict_xml[accession_num]['file_info']['xbrl_href'] = entry.find('xbrl_href').text
        except AttributeError:
            master_dict_xml[accession_num]['file_info']['xbrl_href'] = ""

            # store request info (for making web request to view all documents as part of a filing)
        master_dict_xml[accession_num]['request_info'] = dict()
        master_dict_xml[accession_num]['request_info']['link'] = entry.find('link')['href']
        master_dict_xml[accession_num]['request_info']['title'] = entry.find('title').text
        master_dict_xml[accession_num]['request_info']['last_update'] = entry.find('updated').text


def parse_documents(soup, master_dict_filing, FILING_TYPE):
    """
        scrape all documents in the filing page (a filing is consisted of multiple documents on SEC Edgar)
        args:
            soup: BeautifulSoup object that has read the filing page as lxml
            master_dict_filing: master dictionary to store information scraped from all documents
        return:
            None
    """
    # initialize new dictionary for storing sec-header information
    master_dict_filing['sec_header_content'] = dict()
    # store sec-header in text
    sec_header_code = soup.find('sec-header')
    try:  # not always available
        # convert encoding to text
        sec_header_text = unicodedata.normalize('NFKD', sec_header_code.get_text())
        sec_header_text = sec_header_text.replace("\n", " ").replace("\t", "").replace("  ", " ")
        master_dict_filing['sec_header_content']['sec_header_text'] = sec_header_text
    except AttributeError:
        master_dict_filing['sec_header_content']['sec_header_text'] = ""
    # # if we want to save output as json, the line below has to be commented out as any Python object (e.g. a
    # BeautifulSoup object) cannot be dumped to a json file
    # master_dict_filing['sec_header_content']['sec_header_code'] = sec_header_code

    # make a space holder for saving master_dict_document later (contains text for every document in a filing)
    master_dict_filing['filing_documents'] = None
    # initialize dictionary to store information on each document in a filing
    master_dict_document = dict()
    for filing_document in soup.find_all('document'):
        # read some parts of document tags
        try:
            document_id = filing_document.find('type').find(text=True, recursive=False).strip()
        except AttributeError:  # e.g. 894253 (CIK)
            document_id = ""
        try:
            document_sequence = filing_document.find('sequence').find(text=True, recursive=False).strip()
        except AttributeError:
            document_sequence = ""
        if (document_id == "") and (document_sequence == ""):
            continue  # skip, if no identifier
        try:
            document_filename = filing_document.find('filename').find(text=True, recursive=False).strip()
        except AttributeError:
            document_filename = ""

        # skip supporting documents (pdf, jpg etc.)
        if not ((FILING_TYPE in document_id) or ("EX-" in document_id)):
            # skip documents not containing FILING_TYPE or EX-
            # this would exclude some of pdf jpg etc.
            continue
        file_type = document_filename.split(".")[-1]
        if file_type != "htm":
            # exclude rest of pdf jpg documents
            continue

        try:
            # not always available (e.g. https://www.sec.gov/Archives/edgar/data/43350/000114420410064250
            # /0001144204-10-064250.txt)
            document_description = filing_document.find('description').find(text=True, recursive=False).strip()
        except AttributeError:
            document_description = ""

        # store document tags
        master_dict_document[document_id + "_" + document_sequence] = dict()
        master_dict_document[document_id + "_" + document_sequence]['document_filename'] = document_filename
        master_dict_document[document_id + "_" + document_sequence]['document_description'] = document_description
        # store document content in raw html; normally not needed
        # master_dict_document[document_id+"_"+document_sequence]['document_code'] = filing_document.extract()

        # read all text in the document
        filing_doc_text = filing_document.find('text').extract()
        # read all thematic breaks as str, if any (similar to a page break in a document)
        all_thematic_breaks = [str(thematic_break) for thematic_break in
                               filing_doc_text.find_all('hr', {'width': '100%'})]
        try:
            filing_doc_str = str(filing_doc_text)
        except RecursionError as e:
            logging.error(f"ERROR:\ndocument filename: {document_filename}\nRecursionError: filing_doc_str = str("
                          f"filing_doc_text)")
            continue

        # split documents by thematic breaks
        if len(all_thematic_breaks) > 0:  # if there is thematic break
            regex_delimited_pattern = "|".join(map(re.escape, all_thematic_breaks))
            split_filing_str = re.split(regex_delimited_pattern, filing_doc_str)
            document_pages = split_filing_str
        else:
            document_pages = [filing_doc_str]

        pages_length = len(document_pages)
        repaired_pages = dict()
        normalized_text = dict()
        for index, page in enumerate(document_pages):  # one-time loop if there is only one page
            # generate repaired text (parse page in str as html5 to BeautifulSoup object)
            page_soup = BeautifulSoup(page, "html5")
            # generate normalized text (extract content in text, remove html/css syntax)
            page_text = page_soup.html.body.get_text(" ", strip=True)
            # convert unicode syntax to corresponding text; 
            # note different encoding error results in different parsed texts; 
            # we can use 'ignore' to remove special characters (bullet point sign etc.); 
            # texts are most likely parsed correctly given SEC's strict rules on encoding for filing documents
            # https://godatadriven.com/blog/handling-encoding-issues-with-unicode-normalisation-in-python/ 
            page_text_norm = str(unicodedata.normalize('NFKD', page_text).encode(encoding='ascii', errors='ignore'))
            page_text_norm = page_text_norm.replace(r"\n", " ").replace(r"\t", " ").replace("  ", " ")

            # define page number
            page_number = index + 1
            # store normalized text
            normalized_text[page_number] = page_text_norm
            # store repaired text
            repaired_pages[page_number] = page_soup

        # add pages length and normalized test to master dictionary
        master_dict_document[document_id + "_" + document_sequence]['pages_length'] = pages_length
        master_dict_document[document_id + "_" + document_sequence]['normalized_text'] = normalized_text
        logging.debug(document_filename)  # logging after processing document_filename complete

    master_dict_filing['filing_documents'] = master_dict_document


def download_filings(T1: str, FILING_TYPE: str, CIK_list: list, OUTPUT_FILE_PATH: str, LOGGING_FILE_PATH: str):
    """
        download SEC Filings to local
        ----
        args:
            T1: str, 'YYYYMMDD'; download all filings with timestamp before T1
            FILING_TYPE: str; type of filing to download, e.g. 8-K
            CIK_list: list; list of CIK numbers (SEC Edgar's stock identifier) to download filings for

            LOGGING_FILE_PATH: str; file path for saving run-time information
        returns:
            None
    """
    logging.basicConfig(filename=f"{LOGGING_FILE_PATH}/download_filings/error.txt",
                        filemode='a',
                        level=logging.INFO,
                        format='%(levelname)s: %(asctime)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    logging.info(f"\nSTARTING")
    for CIK in CIK_list:
        logging.info(f"Processing CIK={CIK}")
        try:
            # define endpoint for making web request
            endpoint = r"https://www.sec.gov/cgi-bin/browse-edgar"
            # define parameters
            # note: "dateb" sets the date we want filings prior to, 
            # doesn't look like it has a corresponding "datea"/"date after" parameter
            param_dict = {'action': 'getcompany',
                          'CIK': CIK,
                          'type': FILING_TYPE,
                          'dateb': T1,
                          'owner': 'exclude',
                          'start': "",
                          'output': 'atom',
                          'count': '100'}  # 100 is the max
            # define response
            response = requests.get(url=endpoint, params=param_dict)
            # save status code; for logging/debugging purposes
            logging.info(f"Response status code: {response.status_code}")
            logging.info(f"Starting url: {response.url}")

            # initialize BeautifulSoup object to process search result
            soup = BeautifulSoup(response.content, 'lxml')
            # initalize master list to store entries information from search result
            master_dict_xml = dict()
            # the following loop scrapes each entry in the search result
            # if its next page tag is non-empty, we read next page until exhauxsted 
            while True:
                # read current page
                parse_entries(soup, master_dict_xml)
                # read link for next page, if any
                link = soup.find_all('link', {'rel': 'next'})
                if link == []:
                    break
                else:
                    next_page_link = link[0]['href']
                    # request next page
                    # attemp to limit sec edgar request by putting program to sleep
                    response = requests.get(url=next_page_link)
                    time.sleep(0.1)  # sleep for 0.1 seconds
                    soup = BeautifulSoup(response.content, 'lxml')
            logging.info(f"Number of {FILING_TYPE} filings scraped in total: {len(master_dict_xml)}")

            # iterate through every filing 
            for i, accession_num in enumerate(master_dict_xml):
                # retrieve url to access the filing page on edgar in text format for parsing purposes
                new_html_text = master_dict_xml[accession_num]['file_info']['filing_href'].replace("-index.htm", ".txt")
                # read the page as beautifulsoup object
                response = requests.get(new_html_text)
                time.sleep(0.1)  # sleep for 0.1 seconds
                soup = BeautifulSoup(response.content, 'lxml')
                # initialize empty dictionary to store filing information
                master_dict_filing = dict()
                # store every document information in a filing to master_dict_filing a 8-K filing may have multiple
                # documents, which include main 8-K text, extension files on taxonomy, etc.
                logging.debug("accession number: " + accession_num)
                parse_documents(soup, master_dict_filing, FILING_TYPE)
                # add master_dict_filing to master_dict_xml
                master_dict_xml[accession_num]['master_dict_filing'] = master_dict_filing

            with open(OUTPUT_FILE_PATH + f"{FILING_TYPE}_{CIK}_{T1}.json", 'w') as fp:
                json.dump(obj=master_dict_xml, fp=fp, indent=4)

        except Exception as e:
            with open("data/Error_CIK.csv", 'a') as f:
                f.write(str(CIK) + "\n")
            logging.exception(f"ERROR: {CIK}")
    logging.info(f"FINISHING\n")


def read_filing_text(CIK_list: list, dateb: str, FILING_DATA_PATH: str, FILING_TYPE: str) -> dict:
    """
        returns a dictionary that contains all filings' date and texts for each CIK in CIK_list
        with the following structure:
            - master_filing_text_dict
                - file_name ("8-K_1177609_20200101.json")
                    - asscession_num
                        - filing_date (Timestamp('2019-12-04 00:00:00'))
                        - filing_text ("...")
        args:
            CIK_list: list
            dateb: str
            FILING_DATA_PATH: str; e.g. "/Users/Data/"
            FILING_TYPE: str; e.g. "8-K", "10-K", etc
        return:
            dict (master_filing_text_dict)

    """
    outdir = "logs/read_filing_text"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    logging.basicConfig(filename="../logs/read_filing_text/error_document_text.txt",
                        filemode='a',
                        level=logging.INFO,
                        format='%(levelname)s: %(asctime)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')
    # list of file path for filings with CIK from CIK_list
    filing_path_list = [f"{FILING_DATA_PATH}{FILING_TYPE}_{str(CIK)}_{dateb}.json" for CIK in CIK_list]
    # read every CIK's filings json files into a master dictionary
    master_filing_dict = dict()
    for file_path in filing_path_list:
        file_name = file_path.split("/")[-1]
        with open(file_path) as f:
            master_filing_dict[file_name] = json.load(f)

    # read every CIK's filings in text into a master dictionary
    master_filing_text_dict = dict()
    # read each filing in a dict of filings for a given CIK
    for filing_file_name in master_filing_dict:  # iterate through every json file (that contains multiple filings by date)
        if not master_filing_dict[filing_file_name]:
            continue  # skip empty files
        # intialize dict for storing filing date and texts
        master_filing_text_dict[filing_file_name] = dict()
        for asscession_num in master_filing_dict[filing_file_name]:  # iterate through every filing
            # initialize dictionary to store date and texts for a filing
            master_filing_text_dict[filing_file_name][asscession_num] = dict()
            # initalize variable to simplify json file dict reference
            filing_dict = master_filing_dict[filing_file_name][asscession_num]
            # read date
            filing_date = pd.to_datetime(filing_dict['file_info']['filing_date'])
            # read and comebine all documents in a single string
            document_dict = filing_dict['master_dict_filing']['filing_documents']
            documents_text = list()
            for document in document_dict:
                if 'normalized_text' in document_dict[document]:
                    documents_text.append(list(document_dict[document]['normalized_text'].values()))
                else:
                    logging.info(f"{filing_file_name}, {asscession_num}, {document}")
                    continue
            # # flatten out documents_text
            documents_text = sum(documents_text, [])

            # store date, filing texts
            master_filing_text_dict[filing_file_name][asscession_num]['filing_date'] = filing_date
            master_filing_text_dict[filing_file_name][asscession_num]['filing_text'] = " ".join(documents_text)

    return master_filing_text_dict


if __name__ == '__main__':
    CIK_list = pd.read_csv("data/Error_CIK.csv", header=None)[0].values
    download_filings(T1="20200101", FILING_TYPE="8-K", CIK_list=CIK_list,
                     OUTPUT_FILE_PATH="/Users/codywan/Data/SEC Edgar Scraping/8-K/",
                     LOGGING_FILE_PATH="data/web_scraping_log2.txt")

    # parse_documents(BeautifulSoup(requests.get(
    # "https://www.sec.gov/Archives/edgar/data/921847/000095012307016001/0000950123-07-016001.txt").content, 'lxml'),
    # dict(), "8-K")
