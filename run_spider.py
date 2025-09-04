from scrapy.crawler import CrawlerProcess
from entreprises.spiders.kbo_spider import KboSpider
from entreprises.spiders.ejustice_spider import EJusticeSpider
from entreprises.spiders.consult_spider import ConsultSpider
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

# Fonction qui sera appelée quand EJusticeSpider termine
def run_consult(_):
    return process.crawl(ConsultSpider)

# Lancer KboSpider, puis EJusticeSpider, puis ConsultSpider
d = process.crawl(KboSpider)
d.addCallback(run_ejustice)
d.addCallback(run_consult)

# Démarrer le process (bloquant jusqu’à la fin du dernier spider)
process.start()
