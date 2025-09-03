import scrapy

class EntrepriseItem(scrapy.Item):
    kbo = scrapy.Field()
    publications = scrapy.Field()
    comptes = scrapy.Field()
