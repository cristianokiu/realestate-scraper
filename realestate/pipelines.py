# -*- coding: utf-8 -*-

import pymongo
import jmespath

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class RealestatePipeline(object):
    def __init__(self, settings):
        expr = settings.get('JMESPATH')
        self.jmes = jmespath.compile(expr) if expr else None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_item(self, item, spider):
        scraped = spider.crawler.stats.get_value('scraped_pages')
        total = spider.crawler.stats.get_value('selected_pages')

        spider.logger.info('Pages: [{}/{}] - {:.0%}'.format(
            scraped, total, scraped/total))
        spider.logger.info('Item[{}]: {}'.format(
            spider.crawler.stats.get_value('item_scraped_count') or 0,
            item.get('TituloPagina')))

        return self.jmes.search(item) if self.jmes else item


class MongoDBPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT'])
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        # TODO [romeira]: update if same id {25/04/17 20:14}
        self.collection.insert(item)
        return item
