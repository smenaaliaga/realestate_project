import subprocess

if __name__ == '__main__':
    # Variables
    tipo_operaciones = ['venta'] #, 'arriendo']
    tipo_propiedades = ['departamento', 'casa'] #, 'parcela']
    modalidades = ['propiedades-usadas', 'proyectos']
    ubicaciones = {
        'metropolitana': {
            'providencia': ['barrio-italia', 'bellavista', 'campus-oriente', 'ines-de-suarez', 'las-lilas', 'los-leones', 'manuel-montt', 'metro-bilbao', 'metro-tobalaba-mall-costanera', 'pedro-de-valdivia', 'pedro-de-valdivia-norte', 'plaza-italia', 'salvador']
        }
    }

    # Recorre  tipo de operaciones
    for tipo_operacion in tipo_operaciones:
        # Recorre tipo de propiedad
        for tipo_propiedad in tipo_propiedades:
            # Recorre modalidad
            for modalidad in modalidades: 
                # Recorre ubicacion
                for region, comunas in ubicaciones.items():
                    for comuna, barrios in comunas.items():
                        for barrio in barrios:
                            subprocess.run(['scrapy', 'crawl', 'PortalInmobiliario',
                                            '-a', f'tipo_operacion={tipo_operacion}',
                                            '-a', f'tipo_propiedad={tipo_propiedad}',
                                            '-a', f'modalidad={modalidad}',
                                            '-a', f'region={region}',
                                            '-a', f'comuna={comuna}',
                                            '-a', f'barrio={barrio}'
                                            ])