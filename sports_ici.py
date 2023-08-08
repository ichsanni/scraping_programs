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

class SportsIci(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/sports/ici'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.start = time.time()
        self.url = "REDACTED URL"
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
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers, verify=False)
        soup = BeautifulSoup(page.html.html.replace('<![CDATA[', '').replace(']]>', ''), 'lxml')
        domain = "REDACTED URL"
        content = list()
        store_lists = soup.find_all('item')
        for store in store_lists:
            _store = dict()
            _store['url_store'] = domain + store.find('url').text
            _store['store_name'] = store.find('shop_name').text
_store['address'] = '〒' + store.find('postnum').text + store.find('address').text.replace('\u3000', '')
            maps_link = "REDACTED URL"+ 
_store['address']
            maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
            try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
            except:
              time.sleep(5)
              maps_link = "REDACTED URL"+ 
_store['address']
              maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
              location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
            try:
                lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
                _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
            except:
                lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
                _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]

            _store['tel_no'] = store.find('tel').text.replace('-', '')
            _store['open_hours'] = store.find('time').text
            _store['gla'] = ''
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
    SportsIci(True)

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