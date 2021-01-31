import re
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
# user defined module
from my_scraper.items import MyScraperItem
from dragnet import extract_content
from scrapy.exceptions import DropItem
import pymongo
import unicodedata
import json

# environment can lead to off topics: enhancing security environment in digital products. X
# "Learn more about which VNet-to-VNet connectivity option makes sense in your environment"

whitelist = ['sustainability', 'inclusive-environment', 'social-justice',
             'carbon-footprint', 'clean-energy', 'carbon-neutral',
             'corporate-governance', 'diverse-workforce', 'climate-change',
             'community-impact', 'clean-water', 'social-impact', 'global-warming',
             'social-responsibility', 'corporate-responsibility', 'customer-privacy']

blacklist = ['document', 'blog', 'product', 'profit', 'revenue', 'archive', 'search', 'login',
             'accessories', 'shop', 'support', 'developer', 'dmg', 'forums', 'investor', 'news']

absoluteNo = []



class MyWebSpider(Spider):
    name = "my_web_spider_bot"

    def __init__(self, *args, **kwargs):
        super(MyWebSpider, self).__init__(*args, **kwargs)
        self.file_path = f"/Users/codywan/Dropbox/MS MathFin/Project_Presentation/MathFin_Project_20Fall/data" \
                         f"/web_scraping/seeds_test.txt"
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.absoluteNO = absoluteNo
        self.extractor = LinkExtractor()

    def start_requests(self):
        with open(self.file_path) as f:
            for line in f:
                [company, url] = line.split(',')
                if 'https://' not in url:
                    url = 'https://' + url
                try:
                    url = url.strip()
                    request = Request(url)
                    request.meta.update(company=company.strip())
                    yield request
                except Exception as e:
                    continue

    def parse(self, response):
        if not isinstance(response, HtmlResponse):
            return

        domain_origin = urlparse(response.url).netloc
        # url = response.url

        if response.meta.get('keywords', ()):
            yield self.process_item(response)

        for link in self.extractor.extract_links(response):
            domain_this = urlparse(link.url).netloc
            # Go to the next loop if it goes outside the current domain
            # special case with google and alphabet
            if (domain_this != domain_origin) and (('google' not in domain_this) or domain_origin != 'abc.xyz'):
                continue
            link_str = ' '.join([link.text.lower(), link.url.lower()])
            keywords = list(set(re.findall("|".join(self.whitelist), link_str, flags=re.I)))
            flashcards = list(set(re.findall("|".join(self.blacklist), link_str, flags=re.I)))
            if (not keywords) and flashcards:
                continue
            request = Request(url=link.url)
            request.meta.update(link_text=link.text)
            request.meta.update(keywords=keywords)
            request.meta.update(company=response.meta['company'])
            yield request

    def process_item(self, response):

        item = MyScraperItem()

        item['url'] = response.url
        item['link_text'] = response.meta['link_text']
        item['company'] = response.meta['company']
        item['content'] = response.body
        item['keywords'] = response.meta['keywords']

        return item


class MyWebSpider_multiprocess(MyWebSpider):
    name = "my_web_spider_bot_multiprocess"

    def __init__(self, part, *args, **kwargs):
        super(MyWebSpider_multiprocess, self).__init__(*args, **kwargs)
        self.part = part
        self.file_path = f"/Users/codywan/Dropbox/MS MathFin/Project_Presentation/MathFin_Project_20Fall/data" \
                         f"/web_scraping/seeds_part{self.part}.txt"
        # environment can lead to off topics: enhancing security environment in digital products. X
        # "Learn more about which VNet-to-VNet connectivity option makes sense in your environment"
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.absoluteNo = []
        self.extractor = LinkExtractor()

    def start_requests(self):
        with open(self.file_path) as f:
            for line in f:
                [company, url] = line.split(',')
                if 'https://' not in url:
                    url = 'https://' + url
                try:
                    url = url.strip()
                    request = Request(url)
                    request.meta.update(company=company.strip())
                    yield request
                except Exception as e:
                    continue

    def process_item(self, response):

        item = MyScraperItem()

        item['url'] = response.url
        item['link_text'] = response.meta['link_text']
        item['company'] = response.meta['company']
        item['content'] = response.body
        item['keywords'] = response.meta['keywords']

        fullHTML = item['content']
        content = extract_content(fullHTML)
        item['content'] = unicodedata.normalize("NFKD", content).replace("\n", " ").replace("\t", "").replace("  ", " ")
        item['link_text'] = unicodedata.normalize("NFKD", item['link_text']).replace("\n", " ").replace("\t",
                                                                                                        "").replace(
            "  ", " ")

        valid = True
        if not item['url']:
            valid = False
            raise DropItem("Missing url!")

        if not item['company']:
            valid = False
            raise DropItem("Missing company!")

        if item['content'] == '':
            valid = False
            raise DropItem("empty content")

        # save to local mongodb database
        if valid:
            res_dict = {key: item[key] for key in item}

            connection = pymongo.MongoClient(port=27017,
                                             username='codywan',
                                             password='password',
                                             authSource="admin")
            db = connection['admin']
            collection = db[item['company']]
            collection.insert_one(res_dict)
            connection.close()

        return item
