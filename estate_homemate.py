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

class EstateHomemate(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/estate/homemate'.replace("/", "_")
        self.content = list()
        self.master_list = []
        self.start_time = time.time()
        self.getdata()

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

    def getdata(self):
        rand_agent = {'user-agent': UserAgent().random.strip()}
        url = "REDACTED URL"
        sess = HTMLSession()
        req = sess.get(url, headers=rand_agent)
        soup = BeautifulSoup(req.content, 'html.parser')
        cards = soup.select('ul[class="list_city"] li')
        for card in cards:
            rand_agent = {'user-agent': UserAgent().random.strip()}
            store_name = ' '.join(card.find('a').get_text(strip=True, separator=' ').split())
            address = ' '.join(card.find('p', 'address').get_text(strip=True, separator=' ').split())
            tel_no = ' '.join(card.find('span', 'icon_tel').text.split())
            print(store_name)
            try:
                url_map = card.find('a', 'blue ov')['href']
                lat_lon = url_map.split("P', '")[1].split("', 'h")[0].split("', '")
                (lat, lon) = (lat_lon[0], lat_lon[1])
            except:
                (lat, lon) = ('', '')
            try: url_store = card.find('a')['href']
            except KeyError: continue
            req_store = sess.get(url_store, headers=rand_agent)
            soup_store = BeautifulSoup(req_store.content, 'html.parser')
            open_hours = ' '.join(soup_store.find('th', text='営業時間').find_next('td').get_text(strip=True, separator=' ').split())

            _store = dict()
            _store['url_store'] = req_store.url
            _store['store_name'] = store_name
_store['address'] = address
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''

            _store['tel_no'] = tel_no
            _store['open_hours'] = open_hours
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
            print(len(self.content))
            self.save_data()

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    EstateHomemate(True)

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
