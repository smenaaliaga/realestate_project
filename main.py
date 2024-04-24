import subprocess
import json
import time
from smtp.status import send_status_email

if __name__ == '__main__':
    # Variables
    tipo_operaciones = ['venta'] #, 'arriendo']
    tipo_propiedades = ['departamento', 'casa']
    modalidades = ['propiedades-usadas'] #, 'proyectos']
    with open('resources/ubicaciones.json', 'r') as file:
        ubicaciones = json.load(file)

    # # Recorre tipos de operaciones
    # for tipo_operacion in tipo_operaciones:
    #     # Recorre tipos de propiedades
    #     for tipo_propiedad in tipo_propiedades:
    #         # Recorre modalidades
    #         for modalidad in modalidades: 
    #             # Recorre ubicaciones
    #             for region, comunas in ubicaciones.items():
    #                 for comuna, barrios in comunas.items():
    #                     for barrio, tipo_url in barrios.items():
    #                         subprocess.run(['scrapy', 'crawl', 'PortalInmobiliario',
    #                                         '-a', f'tipo_operacion={tipo_operacion}',
    #                                         '-a', f'tipo_propiedad={tipo_propiedad}',
    #                                         '-a', f'modalidad={modalidad}',
    #                                         '-a', f'region={region}',
    #                                         '-a', f'comuna={comuna}',
    #                                         '-a', f'barrio={barrio}',
    #                                         '-a', f'tipo_url={tipo_url}'
    #                                         ])
    #                         time.sleep(15)
    
    send_status_email()