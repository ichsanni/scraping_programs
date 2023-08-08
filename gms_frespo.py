from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
    from google.cloud import storage
except ModuleNotFoundError:
    pass

class GmsFrespo(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/gms/frespo'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.url = "REDACTED URL"
        self.start = time.time()
        self.get_data(self.url)
        self.end = time.time()
        
        x = pd.DataFrame(self.content)
        if len(x) == 0:
            raise ValueError('Empty df')
        
        x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
        x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
        x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
        x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA
        x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
        x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
        x['url_tenant'] = None
        try:
            self.df_clean = self.clean_data(x)
        except:
            raise
        if True:
            if from_main:
                self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
            else:
                client = storage.Client()
                bucket = client.get_bucket('scrapingteam')
                bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

    def get_data(self, url):
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        link = "REDACTED URL"
        cards = soup.find_all('tr')
        for card in cards:
            _store = dict()
            title = card.find('td', {'class': 'x1'})
            if title:
                if title.find('a'):
                    path = title.find('a').get('href')
                    _store['url_store'] = link + path
                else:
                    _store['url_store'] = url
                _store['store_name'] = title.text.replace('\n', '')address = card.find('td', {'class': 'x2'})
                
_store['address'] = address.text.replace('\u3000', '')
                _store['lat'], _store['lon'] = '', ''
                _store['open_hours'] = ''
                _store['tel_no'] = ''
                _store['gla'] = 'Null'
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                # print(_store)
                self.content.append(_store)
                self.save_data()
        print(len(self.content))

    def get_map_data(self, url):
        url = url.replace('hhttps', 'https')
        page_2 = self.session.get(url)
        soup = BeautifulSoup(page_2.html.html, 'html.parser')
        div = soup.find('div', {'class': 'base_map'})
        if div:
            src = div.find('iframe').get('src')
            iframe = re.findall('\\d{2,3}\\.\\d{5,}', src)
            if len(iframe) == 2:
                return iframe
            else:
                return None

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    GmsFrespo(True)

class AsyncHTMLSessionFixed(AsyncHTMLSession):
  """
  pip3 install websockets==6.0 --force-reinstall
  """

  def __init__(self, **kwargs):
    super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
    self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])

  @property
  async def browser(self):
    if not hasattr(self, "_browser"):
      self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
                                             handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

    return self._browser
