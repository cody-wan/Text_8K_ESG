# Define your item pipelines here; they are meant to process items right after scraping.
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# # useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
import unicodedata
import json
from dragnet import extract_content
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem


class ContentExtractor(object):

    def process_item(self, item, spider):
        fullHTML = item['content']
        content = extract_content(fullHTML)
        item['content'] = unicodedata.normalize("NFKD", content).replace("\n", " ").replace("\t", "").replace("  ", " ")
        item['link_text'] = unicodedata.normalize("NFKD", item['link_text']).replace("\n", " ").replace("\t", "").replace("  ", " ")
        return item


class JsonPipeline(object):

    def process_item(self, item, spider):
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

        if valid:
            res_dict = {key: item[key] for key in item}
            with open(f"/Users/codywan/Data/web scraping/{item['company']}.json", 'w') as f:
                json.dump({item['url']: res_dict}, f, indent=4)
        return item
