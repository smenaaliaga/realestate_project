import re
import uuid
import json
import scrapy
from scrapy import signals
from scrapy.signalmanager import dispatcher
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient
from datetime import datetime
import logging

class PortailInmobiliarioSpider(scrapy.Spider):
    name = 'PortalInmobiliario'

    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        super(PortailInmobiliarioSpider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.close, signal=signals.spider_closed)

        try:
            self.process_uuid = str(uuid.uuid4())
            # fecha_hora 
            self.dt = datetime.now()
            # Variables de argumento
            self.to = kwargs.get('tipo_operacion')
            self.tp = kwargs.get('tipo_propiedad')
            self.m = kwargs.get('modalidad')
            self.r = kwargs.get('region')
            self.c = kwargs.get('comuna')
            self.b = kwargs.get('barrio')
            self.tipo_url = kwargs.get('tipo_url')
            # Conexión MongoDB
            self.client = MongoClient(get_project_settings().get('MONGO_URI'))
            self.db = self.client[get_project_settings().get('MONGO_DATABASE')]
            self.collection_propiedades = self.db[get_project_settings().get('MONGO_COLLECTION')]
            self.collection_log = self.db[get_project_settings().get('MONGO_COLLECTION_LOG')]
            record_log = {
                'uuid': self.process_uuid,
                'detalle': {
                    'tipo_operacion': self.to, 
                    'tipo_propiedad': self.tp, 
                    'modalidad': self.m, 
                    'region': self.r, 
                    'comuna': self.c, 
                    'barrio': self.b
                },
                'fecha_inicio': self.dt,
                'status': 'procesando',
                'fecha_fin': None,
                'duracion': None,
                'resultado': None,
                'summary': None,
                'error': None
            }
            self.collection_log.insert_one(record_log).inserted_id
            # Cantidad
            self.n_paginaciones = 1
            self.n_propiedades = None
            self.n_novedades = None
            self.n_procesados = 0
            self.n_actualizados_vigente = 0
            self.n_actualizados_no_vigente = 0
            # urls to parse
            self.collected_urls = []
            # Error
            self.error = None
            self.msg_error = None

            logging.info(f'({self.process_uuid}) Inicia spyder')
            logging.info({'uuid': self.process_uuid, 'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b})

        except Exception as e:
            self.error = e
            self.msg_error = 'Error al inicializar Spider'
            logging.error(f'({self.process_uuid}) Error al inicializar Spider: {e}')
            self.crawler.engine.close_spider(self, 'exception_found')

    def start_requests(self):
        try:
            url = None
            if self.r == 'metropolitana':
                if self.tipo_url == "1":
                    url = f'https://www.portalinmobiliario.com/{self.to}/{self.tp}/{self.m}/{self.b}-{self.c}-santiago-{self.r}'
                    if self.b == 'plaza-nunoa':
                        url = f'https://www.portalinmobiliario.com/{self.to}/{self.tp}/{self.m}/{self.b}-santiago-{self.r}'
                if self.tipo_url == "2":
                    url = f'https://www.portalinmobiliario.com/{self.to}/{self.tp}/{self.m}/rm-{self.r}/{self.c}/{self.b}'
            yield scrapy.Request(url=url, callback=self.parse)
        except Exception as e:
            self.error = e
            self.msg_error = f'Error al generar solicitud para {url}'
            logging.error(f'({self.process_uuid}) Error al generar solicitud para {url}: {e}')
            self.crawler.engine.close_spider(self, 'exception_found')

    def parse(self, response):
        try:
            logging.info(f'({self.process_uuid}) Procesando paginación {self.n_paginaciones}: {response.url}')

            # Obtiene todas las URLs de la paginación actual
            page_urls = response.css('.ui-search-result__content-wrapper.ui-search-link::attr(href)').getall()
            self.collected_urls.extend(response.urljoin(url) for url in page_urls)

            # Recorre la siguiente paginación y repite el proceso
            next_page = response.css('li.andes-pagination__button--next a::attr(href)').get()
            next_page = next_page if next_page != '' else None
            if next_page is not None:
                self.n_paginaciones = self.n_paginaciones + 1
                yield response.follow(next_page, callback=self.parse)
            # Se han recolectado todas las URLs
            else:
                # Limpia las URL solo con informacion util
                self.collected_urls = self.preprocessed_urls(self.collected_urls)
                self.n_propiedades = len(self.collected_urls)

                # Escribe todas las URL scrapeadas en log alternativo
                with open('log/urls.log', 'a') as archivo:
                    for url in self.collected_urls:
                        archivo.write(f"({self.process_uuid}) {url}\n")

                # Actualiza la vigencia de las URL ya existentes
                self.update_properties(self.collected_urls)
                
                # Filtra solo las URLs que representan novedades a agregar a la BD
                self.collected_urls_news = self.filter_urls(self.collected_urls)
                self.n_novedades = len(self.collected_urls_news)

                logging.info(f'({self.process_uuid}) Total propiedades a procesar: {self.n_novedades}')

                # Inicia procesamiento de novedades de URL
                for url in self.collected_urls_news:
                    yield scrapy.Request(url, callback=self.parse_url)

        except Exception as e:
            self.error = e if response.status != 403 else "Acceso denegado: HTTP 403"
            self.msg_error = f'Error al procesar paginación {response.url}'
            logging.error(f'({self.process_uuid}) Error al procesar paginación {response.url}: {e}')
            self.crawler.engine.close_spider(self, 'exception_found')

    def preprocessed_urls(self, urls):
        # Limpia URL solo con info. necesaria
        urls_split = [url.split('#')[0] for url in urls]
        return urls_split
    
    def filter_urls(self, urls):
        try: 
            # Consulta MongoDB, obtiene todas las URLs dado el filtro
            query = {'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': 
                    self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b}
            urls_db = self.collection_propiedades.find(query, {'_id': 0, 'url': 1})
            urls_db = [doc['url'] for doc in urls_db]

            # Retorna solo las URLs que no están en la base de datos
            return [url for url in urls if url not in urls_db]
        
        except Exception as e:
            self.error = e
            self.msg_error = 'Error durante el filtro de URLS'
            logging.error(f'({self.process_uuid}) Error durante el filtro de URLS: {e}')
            self.crawler.engine.close_spider(self, 'exception_found')
    
    def update_properties(self, urls):
        try:
            # Obtener URLs en la lista con publicacion_vigente en 0
            active_urls = self.collection_propiedades.find(
                {
                    'url': {'$in': urls},  # URLs que están en la lista proporcionada
                    'publicacion_vigente': 0, 
                    'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b
                },
                {'url': 1, '_id': 0}
            )
            active_urls = [doc['url'] for doc in active_urls]
            # Obtener URLs no en la lista con publicacion_vigente en 1
            inactive_urls = self.collection_propiedades.find(
                {
                    'url': {'$nin': urls},  # URLs que no están en la lista proporcionada
                    'publicacion_vigente': 1, 
                    'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b
                },
                {'url': 1, '_id': 0} 
            )
            inactive_urls = [doc['url'] for doc in inactive_urls]  # Crear lista de URLs
            # Actualizar las URLs que están en la lista y tienen publicacion_vigente en 0
            self.collection_propiedades.update_many(
                {
                    'url': {'$in': urls},  # URLs que están en la lista proporcionada
                    'publicacion_vigente': 0, 
                    'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b
                },
                {
                    '$set': {
                        'publicacion_vigente': 1,  # Cambiar a 1
                        'fecha_actualizacion': self.dt  # Establecer fecha actual
                    }
                }
            )
            # Actualizar las URLs que no están en la lista y tienen publicacion_vigente en 1
            self.collection_propiedades.update_many(
                {
                    'url': {'$nin': urls},  # URLs que no están en la lista proporcionada
                    'publicacion_vigente': 1, 
                    'tipo_operacion': self.to, 'tipo_propiedad': self.tp, 'modalidad': self.m, 'region': self.r, 'comuna': self.c, 'barrio': self.b
                },
                {
                    '$set': {
                        'publicacion_vigente': 0,  # Cambiar a 0
                        'fecha_actualizacion': self.dt  # Establecer fecha actual
                    }
                }
            )
            
            self.n_actualizados_vigente = len(active_urls)
            logging.info(f'({self.process_uuid}) Total propiedades que han pasado a estar vigente: {self.n_actualizados_vigente}')
            for active_url in active_urls:
                logging.info(f'({self.process_uuid}) Propiedad pasa a vigente: {active_url}')

            self.n_actualizados_no_vigente = len(inactive_urls)
            logging.info(f'({self.process_uuid}) Total propiedades que han dejado de estar vigente: {self.n_actualizados_no_vigente}')
            for inactive_url in inactive_urls:
                logging.info(f'({self.process_uuid}) Propiedad pasa a no estar vigente: {inactive_url}')
                
        except Exception as e:
            self.error = e
            self.msg_error = 'Error durante la actualización'
            logging.error(f'({self.process_uuid}) Error durante la actualización: {e}')
            self.crawler.engine.close_spider(self, 'exception_found')
    
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
        try:
            self.n_procesados = self.n_procesados + 1
            logging.info(f'({self.process_uuid}) Procesando propiedad {self.n_procesados}: {response.url}')

            # Obtiene todas las caracteristicas de las propiedades
            titulo = response.css('h1.ui-pdp-title::text').get(default=None)
            simbolo_moneda = response.css('span.andes-money-amount__currency-symbol::text').get(default=None)
            precio = response.css('span.andes-money-amount__fraction::text').get(default=None)
            ubicacion = response.css('.ui-vip-location__subtitle .ui-pdp-media__title::text').get(default=None)
            map_image_url = response.css('div#ui-vip-location__map img::attr(src)').get(default=None)
            coordenadas = re.search(r"center=(-?\d+\.\d+)%2C(-?\d+\.\d+)", map_image_url) if map_image_url else None
            latitud = coordenadas.group(1) if coordenadas else None
            longitud = coordenadas.group(2) if coordenadas else None
            caracteristicas = response.css('.ui-vpp-striped-specs__table')
            caracteristicas_dict = {}
            for caracteristica in caracteristicas:
                titulo_caracteristica = caracteristica.css('h3.ui-vpp-striped-specs__header::text').get(default='').strip()
                campos_valores = {}
                filas = caracteristica.css('.andes-table__row')
                for fila in filas:
                    campo = fila.css('.andes-table__header .andes-table__header__container::text').get(default='').strip()
                    valor = fila.css('.andes-table__column--value::text').get(default='').strip()
                    campos_valores[campo] = valor
                if titulo_caracteristica: 
                    caracteristicas_dict[titulo_caracteristica] = campos_valores
            descripcion_fragmentos = response.css('p.ui-pdp-description__content ::text').getall()
            descripcion_completa = ' '.join(descripcion_fragmentos).strip()
            script_content = response.xpath("//script[contains(., 'window.__PRELOADED_STATE__')]/text()").get(default=None)
            info_zona = self.get_info_zonas(script_content) if script_content else None
            refs = response.css('.ui-pdp-price-comparison__extra-info-element-value::text').getall()
            if refs and len(refs) >= 2:
                ref_precio = {'esta_propiedad': refs[0], 'promedio_zona': refs[1]}
            else:
                ref_precio = None
            corredora = response.css('h3.ui-pdp-color--BLACK.ui-pdp-size--XSMALL.ui-pdp-family--REGULAR::text').get(default=None)

            yield {
                'uuid': self.process_uuid,
                'fecha_obtencion': datetime.now(),
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
                'corredora': corredora,
                'publicacion_vigente': 1,
                'fecha_actualizacion': None
            }
        except Exception as e:
            self.error = e if response.status != 403 else "Acceso denegado: HTTP 403"
            self.msg_error = f'Error al procesar propiedad {response.url}'
            logging.error(f'({self.process_uuid}) Error al procesar propiedad {response.url}: {e}')
            self.crawler.engine.close_spider(self, 'exception_found')

    def close_process_log(self, result):
        fecha_fin = datetime.now()
        duracion = fecha_fin - self.dt
        duracion_str = str(duracion).split('.')[0]
        self.collection_log.update_one(
            {'uuid': self.process_uuid},
            {'$set': {
                'status': 'terminado',
                'fecha_fin': fecha_fin,
                'duracion': duracion_str,
                'resultado': result,
                'summary': {
                    'n_paginaciones': self.n_paginaciones,
                    'n_propiedades': self.n_propiedades,
                    'n_novedades': self.n_novedades,
                    'n_actualizados_vigente': self.n_actualizados_vigente,
                    'n_actualizados_no_vigente': self.n_actualizados_no_vigente
                } if self.error is None else None,
                'error': {'e1': self.msg_error, 'e2': str(self.error)} if self.error is not None else None
            }}
        )

    def close(self, reason):
        result = 'exitoso' if reason == 'finished' and self.error is None else 'fallido'
        self.close_process_log(result)
        self.client.close()
        logging.info(f'({self.process_uuid}) Finaliza spyder. Status {result}')