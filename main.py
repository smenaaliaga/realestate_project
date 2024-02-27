import subprocess
from postprocess.postprocess_portalinmob import postprocess_portalinmob

if __name__ == '__main__':
    # Scrapeo
    subprocess.run(['scrapy', 'crawl', 'PortalInmobiliario'])
    # Post-procesamiento
    postprocess_portalinmob()
    