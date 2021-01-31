import scrapy
from multiprocessing import Process
from scrapy.crawler import CrawlerProcess
from my_scraper.spiders.my_scraper_spider import MyWebSpider_multiprocess


class TestSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://blog.scrapinghub.com']

    def parse(self, response):
        for title in response.css('.post-header>h2'):
            print('my_data -> ', self.settings['my_data'])
            yield {'title': title.css('a ::text').get()}


def start_spider(spider, part, settings: dict = {}, data: dict = {}):
    all_settings = {**settings, **{'my_data': data, 'TELNETCONSOLE_ENABLED': False}}

    def crawler_func():
        crawler_process = CrawlerProcess(all_settings)
        crawler_process.crawl(spider, part=part)
        crawler_process.start()

    process = Process(target=crawler_func)
    process.start()
    return process


map(lambda x: x.join(), [
    start_spider(MyWebSpider_multiprocess, part=i + 1) for i in range(14)
])
