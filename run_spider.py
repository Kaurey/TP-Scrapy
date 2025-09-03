from scrapy.crawler import CrawlerProcess
from entreprises.spiders.kbo_spider import KboSpider
from entreprises import settings  # importe tes settings

process = CrawlerProcess(settings={
    "ITEM_PIPELINES": settings.ITEM_PIPELINES,
    "MONGO_URI": settings.MONGO_URI,
    "MONGO_DATABASE": settings.MONGO_DATABASE,
    "USER_AGENT": "Mozilla/5.0",
})

process.crawl(KboSpider)
process.start()
