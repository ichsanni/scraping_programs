import asyncio

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

class SuperHanamasa(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/super/hanamasa'.replace("/", "_")
        self.content = list()
        self.asession = None
        self.session = HTMLSession()
        self.url = "REDACTED URL"
        self.start = time.time()
        loop = asyncio.new_event_loop()
        
        loop.run_until_complete(self.get_page(self.url))
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

    async def get_page(self, url):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call self.get_data(self.url) and visit individual pages.
  """
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        page = await self.asession.get(url)
        await page.html.arender(timeout=0)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        HOMEDELIVERY = '宅配サービス'
        FCTEN = 'FC店'
        areas = soup.find_all('div', {'class': 'shop_list'})
        for area in areas:
             = area.find('h2', {'class': 'firstChild'}).text
            if HOMEDELIVERY not in  and FCTEN not in :
                store_lists = area.find_all('h3')
                for store in store_lists:
                    link = store.find('a').get('href')
                    self.content.append(await self.get_data(link))
                    self.save_data()
                    print(len(self.content))
                    time.sleep(1)

    async def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        page = await self.asession.get(url)
        await page.html.arender(timeout=0)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find_all('td')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h3').text.replace('\n', '')
_store['address'] = details[1].text.replace('\n', '').replace('\r', '').replace('\u3000', '')
        maps = soup.find_all('small')[-1].find('a').get('href')
        location = re.findall('\\d{2,3}\\.\\d{3,}', maps)
        if location:
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = details[2].text.replace('（', '').replace('）', '').replace('-', '').replace('\u3000', '')
        _store['open_hours'] = details[3].text.replace('\n', '').replace('\r', '').replace('\u3000', '')
        _store['gla'] = 'Null'
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    SuperHanamasa(True)

