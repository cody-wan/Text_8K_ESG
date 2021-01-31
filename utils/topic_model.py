import multiprocessing
import os
import unicodedata
import pickle
import logging
import numpy as np
import pandas as pd
import json
import gensim
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from gensim.test.utils import common_texts
from gensim import corpora, models
from gensim.corpora.dictionary import Dictionary
from pathlib import Path


logging.basicConfig(filename=f"../logs/LDA/logs.txt",
                    filemode='a',
                    level=logging.INFO,
                    format='%(levelname)s: %(asctime)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S')

wordlemmatizer = WordNetLemmatizer()


class my_LDA(object):
    def __init__(self):
        self.low_corpus = list()
        self.bow_corpus = list()
        self.dictionary = None

    @classmethod
    def tokenize_text(cls, text):
        text = unicodedata.normalize("NFKD", text).replace("\n", " ").replace("\t", "").replace("  ", " ")
        # tokenize text as English
        text_tokenized = word_tokenize(text, language='English')
        # convert to lowercases and remove puncutation
        text_tokenized = [word.lower() for word in text_tokenized if word.isalpha()]
        # lemmatize token
        text_tokenized = [wordlemmatizer.lemmatize(word) for word in text_tokenized if
                          word not in stopwords.words('english')]
        return text_tokenized

    def callback(self, text_tokenized):
        if len(text_tokenized) > 10:
            self.low_corpus.append(text_tokenized)

    def text_to_low(self, texts):
        """
        convert texts to list of words, concurrency-enabled
        args:
            text: list of string
        """
        logging.info('running text_to_low')
        pool = multiprocessing.Pool(os.cpu_count())
        for text in texts:
            pool.apply_async(self.tokenize_text, args=(text,), callback=self.callback)
        pool.close()
        pool.join()

    def low_to_bow(self):
        """
        list of words to bag of words
        """
        logging.info('running low_to_bow')
        if not self.low_corpus:
            raise ValueError("Run text_to_low First")
        self.dictionary = Dictionary(self.low_corpus)
        self.dictionary.filter_extremes(no_below=15, no_above=0.9)
        self.bow_corpus = [self.dictionary.doc2bow(doc) for doc in self.low_corpus]

    def run_lda(self):
        logging.info('running run_lda')
        if not self.bow_corpus or self.dictionary is None:
            raise ValueError("Run low_to_bow First")
        lda_model = models.LdaMulticore(self.bow_corpus, alpha=0.001,
                                        num_topics=10,
                                        id2word=self.dictionary,
                                        workers=os.cpu_count())
        lda_model.save("data/LDA/my_LDA/topic_model.model")
        self.dictionary.save("data/LDA/my_LDA/dictionary.dict")
        corpora.MmCorpus.serialize("data/LDA/my_LDA/corpus.mm", self.bow_corpus)


class backtest_LDA_multicore(object):
    def __init__(self, filing_file_path, backtest_LDA_path, sector_name):
        self.LDA_signal_dict = dict()
        self.backtest_LDA_path = backtest_LDA_path
        self.sector_name = sector_name
        self.filing_file_path = filing_file_path

    def callback(self, res):
        cik, signal_list = res
        self.LDA_signal_dict[cik] = signal_list

    def func_multicore(self, signal_dates, cik, lda_model, dictionary):
        """
        apply LDA to selected filings of a cik
        """
        signal_list = list()
        count = 0
        count_LDA = 0
        # read master file that contains all filings for this cik
        with open(f"{self.filing_file_path}/8-K_{cik}_20200101.json") as f:
            filing_text = json.load(f)

        # iterate filing
        for a_num in filing_text:
            # only read filing with a date match
            date = filing_text[a_num]['file_info']['filing_date']
            if not (date in signal_dates):
                continue
            else:
                count += 1  # count # of signal filings for this cik
                LDA_flag = False  # if set to true, then this filing is identified a signal by LDA
                # iterate document (8_K_1, EXE_2 etc.) in filing
                for document in filing_text[a_num]['master_dict_filing']['filing_documents']:
                    document_dict = filing_text[a_num]['master_dict_filing']['filing_documents'][document]
                    try:
                        normalized_text = document_dict['normalized_text']
                    except KeyError as e:
                        logging.error(e)
                        continue
                    # iterate each page in a document
                    for page in normalized_text:
                        text = normalized_text[page]
                        low_corpus = my_LDA.tokenize_text(text)
                        if len(low_corpus) < 50:
                            continue
                        for index, score in sorted(lda_model[dictionary.doc2bow(low_corpus)],
                                                   key=lambda topic: -1 * topic[1]):
                            if score > 0.75:
                                # topic model significant
                                LDA_flag = True
                            break  # check topic with highest probability
                        # break # check 1 page (in a document)
                    # break # check 1 document (in a filing)
                if LDA_flag:
                    count_LDA += 1
                    signal_list.append(date)

        logging.info(f"\t{cik}: {np.round(count_LDA / count * 100, 2)}%")
        return cik, signal_list

    def backtest_LDA(self, BUY_SIGNAL, lda_model, dictionary):
        """
        apply LDA on filings
        """
        pool = multiprocessing.Pool(os.cpu_count()-1)
        for cik in BUY_SIGNAL:
            pool.apply_async(self.func_multicore, args=(BUY_SIGNAL[cik], cik, lda_model, dictionary),
                             callback=self.callback)
        pool.close()
        pool.join()

        df = pd.DataFrame.from_dict(self.LDA_signal_dict, orient='index').T
        if not df.empty:
            df.to_csv(f"{self.backtest_LDA_path}/{self.sector_name}/signal.csv", index=False)


if __name__ == "__main__":
    # load fitted lda model from local
    path = "../data/LDA/lda_model"
    lda_model = gensim.models.LdaModel.load(f'{path}/topic_model.model')
    dictionary = gensim.corpora.Dictionary.load(f'{path}/dictionary.dict')
    bow_corpus = gensim.corpora.MmCorpus(f'{path}/corpus.mm')

    # generate pair of NAICS sector code and name, needed as index to iterate filings
    with open("../data/industry_classification_and_portfolio/NACIS_sectors.json", "r") as f:
        NAICS_sectors = json.load(f)
    NACIS_sector_code = list({key: key.split(",") for key in NAICS_sectors}.values())
    NACIS_sector_name = list(NAICS_sectors.values())

    # choose which signal events to use
    basket_name = "ESG_baseline"
    repository_path = f"backtests/key_word_search/{basket_name}"
    # file paths
    filing_file_path = "/Users/codywan/Data/SEC Edgar Scraping/8-K"
    backtest_LDA_path = "/backtests/LDA"
    for sector_name in NACIS_sector_name:
        file_path = f"{repository_path}/{sector_name}/signal.csv"
        if not Path(file_path).is_file():
            continue
        logging.info(sector_name)
        print(sector_name)

        # get signal events
        df = pd.read_csv(file_path)
        BUY_SIGNAL = {int(col): df[col].dropna().to_list() for col in df}

        # make repository to store LDA signal events
        if not os.path.exists(f"backtests/LDA/{sector_name}/"):
            os.mkdir(f"backtests/LDA/{sector_name}/")

        ba_lda = backtest_LDA_multicore(filing_file_path=filing_file_path,
                                        backtest_LDA_path=backtest_LDA_path,
                                        sector_name=sector_name)
        ba_lda.backtest_LDA(BUY_SIGNAL=BUY_SIGNAL, lda_model=lda_model, dictionary=dictionary)
        # break # check 1 sector
