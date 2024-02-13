import scrapy
from realestate_scraper.items import PortalInmobiliarioItem

class PortailInmobiliarioSpider(scrapy.Spider):
    name = "PortalInmobiliario"
    start_urls = [
        'https://www.portalinmobiliario.com/venta/departamento/providencia-metropolitana',
    ]
    

    def parse(self, response):
        
        for item in response.css('li.ui-search-layout__item'):
            
            # Titulo
            titulo_raw = item.css('.ui-search-item__title-label-grid::text').get()
            titulo = titulo_raw.strip() if titulo_raw is not None else None
            # Moneda
            moneda = item.css('.andes-money-amount__currency-symbol::text').get().strip()
            # Precio
            precio = item.css('.andes-money-amount__fraction::text').get().strip()
            
            yield {'titulo': titulo, 'moneda': moneda, 'precio': precio}
            
        # Usa el enlace a la siguiente página encontrado en el elemento <li> de la paginación
        siguiente_pagina_url = response.css('li.andes-pagination__button--next a::attr(href)').get()

        if siguiente_pagina_url:
            yield response.follow(siguiente_pagina_url, callback=self.parse)
