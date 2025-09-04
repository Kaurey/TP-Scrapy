from scrapy.crawler import CrawlerProcess
from entreprises.spiders.kbo_spider import KboSpider
from entreprises.spiders.ejustice_spider import EJusticeSpider
from entreprises import settings

process = CrawlerProcess(settings={
    "ITEM_PIPELINES": settings.ITEM_PIPELINES,
    "MONGO_URI": settings.MONGO_URI,
    "MONGO_DATABASE": settings.MONGO_DATABASE,
    "USER_AGENT": "Mozilla/5.0",
    "LOG_LEVEL": "INFO",
})

# Fonction qui sera appelée quand KboSpider termine
def run_ejustice(_):
    return process.crawl(EJusticeSpider)

# Lancer KboSpider, puis enchaîner avec EJusticeSpider
d = process.crawl(KboSpider)
d.addCallback(run_ejustice)

# Démarrer le process (bloquant jusqu’à la fin du dernier spider)
process.start()
