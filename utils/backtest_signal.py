import multiprocessing
import os
from utils import SEC_scraping
import pandas as pd
import numpy as np
import glob
import time
import json
import logging
import warnings
import subprocess
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tokenize import MWETokenizer
from datetime import timedelta
from nltk.stem.wordnet import WordNetLemmatizer

warnings.filterwarnings("ignore")


def execute(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


def run_command(cmd):
    for path in execute(cmd.split(" ")):
        print(path, end="")


class Backtest(object):

    def __init__(self, model_name_plus=False):
        # self.result = dict()
        # for debugging deadlock, let's see if it's a difference between list and dict
        self.model_name_plus = model_name_plus
        if self.model_name_plus:
            self.result = list()
        else:
            self.result = dict()

    def my_callback(self, result):

        # if result not empty
        if result:
            if not self.model_name_plus:
                # single batch of backtest_model parameter case
                CIK, date = result
                if CIK not in self.result:
                    self.result[CIK] = list()
                self.result[CIK].append(date)
            else:
                # multiple batches of backtest_model parameter case
                self.result.append(result)

    def func(self, args):
        (model_name, model_param, CIK_document) = args
        CIK, document = CIK_document
        document_text = document['filing_text']
        document_date = document['filing_date']
        backtest_model = self.__getattribute__(model_name)
        if backtest_model(model_param, document_text):
            return CIK, document_date
        else:
            return False

    def func_plus(self, args):
        raise NotImplementedError()

    def aggregate_result_plus(self):
        raise NotImplementedError()

    def run_backtest(self, model_name, model_param, CIK_filing_documents):
        pool = multiprocessing.Pool(os.cpu_count())
        for CIK_document in CIK_filing_documents:
            # calling the right version of multi-processing func, depending on if we want to process
            # single model param or batches of model param
            if self.model_name_plus:
                pool.apply_async(self.func_plus, args=((model_name, model_param, CIK_document),),
                                 callback=self.my_callback)
            else:
                pool.apply_async(self.func, args=((model_name, model_param, CIK_document),),
                                 callback=self.my_callback)
        pool.close()
        pool.join()

        if self.model_name_plus:
            # if running batches of model params, the result is stored in a list
            # convert it to a dict for consistency with save_to_local()
            self.aggregate_result_plus()

        return self.result


class My_Backtest(Backtest):
    """
        inherited from Backtest
        the following functions are overridden to allow testing batches of model params
        (function name that ends with "plus" denote versions for multiple batch testing)
             func_plus()
             aggregate_result_plus()
    """

    def __init__(self, model_name_plus=False):
        super(My_Backtest, self).__init__(model_name_plus)
        self.mweTokenizer = MWETokenizer(separator=" ")

    def set_mwe(self, model_param):
        """
            load multi-word expressions
        """
        if self.model_name_plus:
            # initialize tokenizer that detects multi-word expression
            for key_word in [val for list_item in model_param.values() for val in list_item]:
                if " " in key_word:
                    self.mweTokenizer.add_mwe(tuple(key_word.split(" ")))
        else:
            # initialize tokenizer that detects multi-word expression
            for key_word in model_param:
                if " " in key_word:
                    self.mweTokenizer.add_mwe(tuple(key_word.split(" ")))

    def func_plus(self, args):
        """

        """
        (model_name, model_param, CIK_document) = args
        CIK, document = CIK_document
        document_text = document['filing_text']
        document_date = document['filing_date']
        backtest_model = self.__getattribute__(model_name)
        return backtest_model(model_param, document_text, CIK, document_date)

    def contains_key_words(self, key_words, text, quantifier='ANY'):
        """

        """
        # tokenize text as English
        text_tokenized = word_tokenize(text, language='English')
        # convert to lowercases and remove puncutation
        text_tokenized = [word.lower() for word in text_tokenized if word.isalpha()]
        # remvoe English stop words, lematize token
        wordlemmatizer = WordNetLemmatizer()
        text_tokenized = [wordlemmatizer.lemmatize(word) for word in text_tokenized if word not in stopwords.words('english')]

        # if there're multi-word expression in KEY_WORDS, then tokenize the expression
        if self.mweTokenizer.mwes:
            text_tokenized = self.mweTokenizer.tokenize(text_tokenized)

        freq_dist = pd.Series(text_tokenized, dtype="str").unique()

        if quantifier.upper() == 'ALL':
            return all([key_word in freq_dist for key_word in key_words])
        elif quantifier.upper() == 'ANY':
            return any([key_word in freq_dist for key_word in key_words])

    def contains_key_words_plus(self, sub_model_key_words, text, CIK, document_date, quantifier='ANY'):
        """

        """
        # tokenize text as English
        text_tokenized = word_tokenize(text, language='English')
        # convert to lowercases and remove punctuation
        text_tokenized = [word.lower() for word in text_tokenized if word.isalpha()]
        # remove English stop words, e.g.
        wordlemmatizer = WordNetLemmatizer()
        text_tokenized = [wordlemmatizer.lemmatize(word) for word in text_tokenized if word.lower() not in stopwords.words('english')]

        # if there're multi-word expression in key words, then tokenize the expression
        if self.mweTokenizer.mwes:
            text_tokenized = self.mweTokenizer.tokenize(text_tokenized)

        freq_dist = list(pd.Series(text_tokenized, dtype="str").unique())

        res = dict()
        for word in freq_dist:
            # if every sub model has indicated this text contained its keywords, no need to check further
            if len(res) == len(sub_model_key_words):
                break
            for basket in sub_model_key_words:
                # get keywords specific to a basket
                key_words = sub_model_key_words[basket]
                # save to res, if text contains word from keywords
                if word in key_words:
                    # add to res
                    if basket not in res:
                        res[basket] = dict()
                    if CIK not in res[basket]:
                        res[basket][CIK] = list()
                    res[basket][CIK].append(document_date)
        return res

    def aggregate_result_plus(self):
        """
            convert [{'basket_name': {CIK_number: [signal_event_date]}}] to a dictionary of
            {'basket_name': {CIK_number1: [signal_event_date1, signal_event_date2, ..., ]},...}
        """
        res = dict()
        for result in self.result:
            for basket in result:
                if basket not in res:
                    # initialize dict to store signal events for each basket
                    # e.g. res := { 'A' : dict(), 'B' : dict()}
                    res[basket] = dict()
                for CIK in result[basket]:
                    if CIK not in res[basket]:
                        # initialize list to store signal events indexed by CIK
                        # e.g. res[basket] := {1410636: [Timestamp('2019-05-01 00:00:00')], ...}
                        res[basket][CIK] = list()
                    res[basket][CIK].extend(result[basket][CIK])
        self.result = res

    @classmethod
    def save_to_local(cls, signal_dict, model_name_plus, model_name, model_param, sector):
        """
            save signal events from backtest to local
            handle two cases:
                1. a single set of model_param to run through
                2. batches of model_param each with own identifier to run through
        """
        if not model_name_plus:
            # make dir for saving output if not existing
            if not os.path.exists(f"backtests/{model_name}/{sector}/"):
                os.mkdir(f"backtests/{model_name}/{sector}/")
            # save keywords
            pd.DataFrame(model_param, columns=['key words']).to_csv(f"backtests/{model_name}/key_words.csv",
                                                                    index=False)
            # save signal event dictionary as DataFrame and save it to local "signal.csv"
            df = pd.DataFrame.from_dict(signal_dict, orient='index').T
            if not df.empty:
                df.to_csv(f"backtests/{model_name}/{sector}/signal.csv", index=False)

        else:
            if not os.path.exists(f"backtests/{model_name}/"):
                os.mkdir(f"backtests/{model_name}/")
            for basket_name in signal_dict:
                if not os.path.exists(f"backtests/{model_name}/{basket_name}/"):
                    os.mkdir(f"backtests/{model_name}/{basket_name}/")
                # save keywords
                pd.DataFrame(model_param[basket_name], columns=['key words']).to_csv(
                    f"backtests/{model_name}/{basket_name}/key_words.csv", index=False)
                if not os.path.exists(f"backtests/{model_name}/{basket_name}/{sector}/"):
                    os.mkdir(f"backtests/{model_name}/{basket_name}/{sector}/")
                # save signal event dictionary as DataFrame and save it to local "signal.csv"
                df = pd.DataFrame.from_dict(signal_dict[basket_name], orient='index').T
                if not df.empty:
                    df.to_csv(f"backtests/{model_name}/{basket_name}/{sector}/signal.csv", index=False)


if __name__ == '__main__':

    # uncomment below code to test multiple batches of keywords each with an associated basket name
    TEST_FLAG = False
    MODEL = "key_word_search"
    MODEL_NAME = "contains_key_words_plus"
    MODEL_PARAM = {
        # "A": ['weather'],
        "B": ['artificial intelligence', 'cloud based', 'machine learning', 'big data'],
        # "C": ['eyeball', 'view'],
        # "D": ['total addressable market', 'go-to market'],
        # "E": ['ecosystems'],
        # "F": ['disruptor']
    }
    MODEL_NAME_PLUS = ((MODEL_NAME.split("_")[-1]).lower() == 'plus')

    # # uncomment below code to test a single batch of keywords
    # TEST_FLAG = False
    # MODEL = "ESG_baseline"
    # MODEL_NAME = "contains_key_words"
    # MODEL_PARAM = ['carbon neutral', 'carbon footprint', 'clean water', 'waste management',
    #                'pollution mitigation', 'climate change', 'global warming', 'corporate social responsibility',
    #                'diverse workforce', 'labor standard', 'customer privacy', 'community impact',
    #                'executive compensation', 'sustainability']
    # MODEL_NAME_PLUS = ((MODEL_NAME.split("_")[-1]).lower() == 'plus')

    """ logging logistics """
    if not os.path.exists(f"backtests/{MODEL}/"):
        os.mkdir(f"backtests/{MODEL}/")
    logging.basicConfig(filename=f"backtests/{MODEL}/backtest_log.txt",
                        filemode='a',
                        level=logging.INFO,
                        format='%(levelname)s: %(asctime)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')
    if TEST_FLAG:
        logging.info(f"\tTest Started")
        print(f"Test Started")

    # filing json file hyper-parameters
    FILING_TYPE = "8-K"
    FILING_DATA_PATH = f"/Users/codywan/Data/SEC Edgar Scraping/{FILING_TYPE}/"
    DATEB = "20200101"
    # construct a list of CIK from all filings
    CIK_list = [int(filing_path.split("/")[-1].split("_")[1]) for filing_path in
                glob.glob(FILING_DATA_PATH + "*.json")]
    logging.info(f"number of CIK: {len(CIK_list)}")

    # linking file for CIK and NAICS sector classification code
    ccmlinktable = pd.read_csv("/Users/codywan/Data/WRDS Data/crspa_ccmlinktable.csv").replace("", np.NaN)
    CIK_NAICS_mapping = ccmlinktable.dropna(subset=['cik', 'naics'])[['cik', 'naics']].astype('int').set_index('cik')[
        'naics'].to_dict()
    logging.info(f"total number of corresponding NAICS: {len([str(CIK_NAICS_mapping[CIK]) for CIK in CIK_list])}")

    # generate pair of NAICS sector code and name
    with open("data/NACIS_sectors.json", "r") as f:
        NAICS_sectors = json.load(f)
    NAICS_sector_code = list({key: key.split(",") for key in NAICS_sectors}.values())
    NAICS_sector_name = list(NAICS_sectors.values())

    time_count = list()
    for NACIS_code, SECTOR in zip(NAICS_sector_code, NAICS_sector_name):
        """ e.g. 
            SECTOR = "Professional, Scientific, and Technical Services"
            NACIS_code = ['54']
        """

        # uncomment below code to test sector with large (4k) number of documents
        # if NACIS_code[0] == '11' or NACIS_code[0] == '21':
        #     continue
        t0 = time.time()
        # get CIKs in this sector
        sector_CIK_list = [CIK for CIK in CIK_list if str(CIK_NAICS_mapping[CIK])[:2] in NACIS_code]
        # print how many CIKs in this sector
        logging.info(f"{len(sector_CIK_list)} CIK(s) for {NACIS_code}, {SECTOR}")
        print(f"{len(sector_CIK_list)} CIK(s) for {NACIS_code}, {SECTOR}")
        # read filing text
        master_filing_text_dict = SEC_scraping.read_filing_text(sector_CIK_list, DATEB, FILING_DATA_PATH, FILING_TYPE)
        logging.info(f"\t{len(master_filing_text_dict)}/{len(sector_CIK_list)} non-empty file read")
        print(f"\t{len(master_filing_text_dict)}/{len(sector_CIK_list)} non-empty file read")

        CIK_filing_document = list()
        # iterate through every master filing json file (contains a CIK's all historical filing)
        for file in master_filing_text_dict:
            CIK = int(file.split('_')[1])  # extract CIK number
            for accession_num in master_filing_text_dict[file]:
                # (CIK, filing_document); filing_document has key 'filing_text', 'filing_date'
                CIK_filing_document.append((CIK, master_filing_text_dict[file][accession_num]))
        print(f"\t{len(CIK_filing_document)} documents")

        # initialize backtestor
        # when model_name_plus is set to True, backtest model runs on batches of model parameters
        my_backtest = My_Backtest(model_name_plus=MODEL_NAME_PLUS)
        my_backtest.set_mwe(model_param=MODEL_PARAM)
        res = my_backtest.run_backtest(
            model_name=MODEL_NAME,
            model_param=MODEL_PARAM,
            CIK_filing_documents=CIK_filing_document
        )

        # res := {"A":{CIK_1:[], CIK_2:[]}, "B":{CIK_1:[],} ,...}
        My_Backtest.save_to_local(signal_dict=res,
                                  model_name_plus=MODEL_NAME_PLUS,
                                  model_name=MODEL,
                                  model_param=MODEL_PARAM,
                                  sector=SECTOR
                                  )

        t1 = time.time()
        elapsed = t1 - t0
        time_count.append(elapsed)
        logging.info(f"\ttime elapsed: {str(timedelta(seconds=elapsed))}")
        print(f"\ttime elapsed: {str(timedelta(seconds=elapsed))}")

        if TEST_FLAG:
            logging.info(f"\tTest Complete")
            break
    total_time_elapsed = str(timedelta(seconds=sum(time_count)))
    logging.info(f"total time elapsed: {total_time_elapsed}")
    print(f"'{MODEL}' completed, total time elapsed: {total_time_elapsed}")
