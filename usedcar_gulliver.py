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

class UsedcarGulliver(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/usedcar/gulliver'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.url = "REDACTED URL"
        self.start = time.time()
        self.get_page(self.url)
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

    def get_page(self, url):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call get_data(self.url) and visit individual pages.
  """
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        domain = "REDACTED URL"
        prefecture_lists = soup.find('div', {'class': 'japanMap'}).find_all('dl')
        for prefs in prefecture_lists:
            city_lists = prefs.find_all('li')
            for city in city_lists:
                link = city.find('a').get('href')
                page_2 = self.session.get(domain + link)
                soup_2 = BeautifulSoup(page_2.html.html, 'html.parser')
                store_lists = soup_2.find_all('div', {'class': 'shopInformation'})
                for store in store_lists:
                    _store = dict()
                    details = store.find_all('dd')
                    _store['url_store'] = domain + store.find('h3').find('a').get('href')
                    _store['store_name'] = store.find('h3').text.replace('\n', '').replace('\t', '')
_store['address'] = details[0].text.replace('\r', '').replace('\n', '').replace('\t', '').replace('\u3000', '').replace(' ', '').replace('\xa0', '').replace('地図を見る', '')
                    _store['lat'], _store['lon'] = '',''
                    _store['tel_no'] = details[1].text.replace('\r', '').replace('\n', '').replace('\t', '').replace('-', '').replace(' ', '')
                    try:
                        _store['open_hours'] = details[3].text.replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
                    except IndexError:
                        _store['open_hours'] = ''
                    _store['gla'] = 'Null'
                    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                    self.content.append(_store)
                    self.save_data()
                    print(len(self.content))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    UsedcarGulliver(True)

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
