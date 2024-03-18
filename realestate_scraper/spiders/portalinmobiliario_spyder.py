import re
import json
import scrapy
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient
from datetime import datetime

class PortailInmobiliarioSpider(scrapy.Spider):
    name = "PortalInmobiliario"

    def __init__(self, *args, **kwargs):
        super(PortailInmobiliarioSpider, self).__init__(*args, **kwargs)
        # Variables de argumento
        self.to = kwargs.get('tipo_operacion')
        self.tp = kwargs.get('tipo_propiedad')
        self.m = kwargs.get('modalidad')
        self.r = kwargs.get('region')
        self.c = kwargs.get('comuna')
        self.b = kwargs.get('barrio')
        # Configuración MongoDB
        self.mongo_uri = get_project_settings().get('MONGO_URI')
        self.mongo_db = get_project_settings().get('MONGO_DATABASE')
        self.mongo_collection = get_project_settings().get('MONGO_COLLECTION')
        # fecha_horaen 
        self.dt = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        # urls to parse
        self.collected_urls = []

    def start_requests(self):
        url = f'https://www.portalinmobiliario.com/{self.to}/{self.tp}/{self.m}/{self.b}-{self.c}-santiago-{self.r}'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Obtiene todas las URLs de la paginación actual
        page_urls = response.css('.ui-search-result__content-wrapper.ui-search-link::attr(href)').getall()
        self.collected_urls.extend(response.urljoin(url) for url in page_urls)

        # Recorre la siguiente paginación y repite el proceso
        next_page = response.css('li.andes-pagination__button--next a::attr(href)').get()
        if next_page != '':
            yield response.follow(next_page, callback=self.parse)
        else:
            # Se han recolectado todas las URLs
            self.collected_urls = self.preprocessed_urls(self.collected_urls)
            self.collected_urls = self.filter_urls(self.collected_urls)
            for url in self.collected_urls:
                yield scrapy.Request(url, callback=self.parse_url)

    def preprocessed_urls(self, urls):
        # Limpia URL solo con info. necesaria
        urls_split = [url.split('#')[0] for url in urls]
        return urls_split
    
    def filter_urls(self, urls):
        # Conexión MongoDB
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db[self.mongo_collection]

        # Consulta MongoDB, obtiene todas las URLs dado el filtro
        query = {'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': 
                 self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b}
        urls_db = collection.find(query, {'_id': 0, 'url': 1})
        urls_db = [doc['url'] for doc in urls_db]

        # Retorna solo las URLs que no están en la base de datos
        return [url for url in urls if url not in urls_db]
    
    def get_info_zonas(self, content):
        # Obtiene el string del json de la data de informacion de la zona
        pattern = r'"REGULAR"},"categories":(.*?),"heading_label":'
        filtered_match = re.search(pattern, content, re.DOTALL)

        if filtered_match:
            # Obtiene el primer valor y convierte en json
            selected_text = filtered_match.group(1)
            data = json.loads(selected_text)

            # Obtiene un json limpio solo con datos de interés
            clean_data = {}
            for categoria in data:
                titulo_categoria = categoria['title']['text']
                clean_data[titulo_categoria] = {}
                
                for subcategoria in categoria.get('subcategories', []):
                    titulo_subcategoria = subcategoria['title']['text']
                    items_nuevos = {}
                    
                    for item in subcategoria.get('items', []):
                        titulo_item = item['title']['text']
                        texto_subtitle = item.get('subtitle', {}).get('label', {}).get('text', '')
                        items_nuevos[titulo_item] = texto_subtitle
                    
                    clean_data[titulo_categoria][titulo_subcategoria] = items_nuevos

            return clean_data

    def parse_url(self, response):
        # Obtiene todas las caracteristicas de las propiedades
        titulo = response.css('h1.ui-pdp-title::text').get()
        simbolo_moneda = response.css('span.andes-money-amount__currency-symbol::text').get()
        precio = response.css('span.andes-money-amount__fraction::text').get()
        ubicacion = response.css('.ui-vip-location__subtitle .ui-pdp-media__title::text').get()
        map_image_url = response.css('div#ui-vip-location__map img::attr(src)').get()
        coordenadas = re.search(r'center=(-?\d+\.\d+)%2C(-?\d+\.\d+)', map_image_url)
        if coordenadas:
            latitud = coordenadas.group(1)
            longitud = coordenadas.group(2)
        caracteristicas = response.css('.ui-vpp-striped-specs__table')
        caracteristicas_dict = {}
        for caracteristica in caracteristicas:
            titulo_caracteristica = caracteristica.css('h3.ui-vpp-striped-specs__header::text').get().strip()
            campos_valores = {}
            filas = caracteristica.css('.andes-table__row')
            for fila in filas:
                campo = fila.css('.andes-table__header .andes-table__header__container::text').get().strip()
                valor = fila.css('.andes-table__column--value::text').get().strip()
                campos_valores[campo] = valor
            caracteristicas_dict[titulo_caracteristica] = campos_valores
        descripcion_fragmentos  = response.css('p.ui-pdp-description__content ::text').getall()
        descripcion_completa = ' '.join(descripcion_fragmentos).strip()
        script_content = response.xpath('//script[contains(., "window.__PRELOADED_STATE__")]/text()').get()
        info_zona = self.get_info_zonas(script_content)
        refs = response.css('.ui-pdp-price-comparison__extra-info-element-value::text').getall()
        ref_precio = {'esta_propiedad': refs[0], 'promedio_zona': refs[1]}
        corredora = response.css('h3.ui-pdp-color--BLACK.ui-pdp-size--XSMALL.ui-pdp-family--REGULAR::text').get()

        yield {
            'dt_process': self.dt,
            'url': response.url,
            'tipo_operacion': self.to, 
            'tipo_propiedad': self.tp, 
            'modalidad': self.m, 
            'region': self.r, 
            'comuna': self.c, 
            'barrio': self.b,
            'titulo': titulo,
            'simbolo_moneda': simbolo_moneda,
            'precio': precio,
            'ubicacion': ubicacion,
            'latitud': latitud,
            'longitud': longitud,
            'caracteristicas': caracteristicas_dict,
            'descripcion': descripcion_completa,
            'info_zona': info_zona,
            'ref_precio': ref_precio,
            'corredora': corredora
        }
