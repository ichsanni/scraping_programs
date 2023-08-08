import asyncio

import requests
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

class PcAbstore(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/pc/abstore'.replace("/", "_")
        self.content = list()
        self.asession = None
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
  If no, call self.get_data(self.url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        store_lists = soup.find_all('div', 'thumbnail')
        loop = asyncio.new_event_loop()
        

        for store in store_lists:
            link = store.parent['href']
            self.content.append(loop.run_until_complete(self.get_data(link, store.find('h4').text)))
            self.save_data()
            print(len(self.content))

    async def get_data(self, url, store_name):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individually by calling get_map_data(self.url)
  """
        print(url)
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        headers = {'user-agent': UserAgent().random.strip()}
        try:
            page = await self.asession.get(url, headers=headers)
            address = None
        except requests.exceptions.MissingSchema:
            page = await self.asession.get("REDACTED URL"+ url, headers=headers)
            address = '京都府京都市伏見区深草西浦町8-113 西陣ビル4F'
        await page.html.arender(timeout=0, sleep=1)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = page.url
        _store['store_name'] = store_nameif 'mitsui--park' in page.url:
            address_fix = '〒470-0186愛知県愛知郡東郷町\u3000東郷中央土地区画整理事業62街区1・3'
        elif address:
            address_fix = address
        elif 'toyosaki' in page.url:
            address_fix = '〒901-0225沖縄県豊見城市豊崎3番35'
        elif 'lect.izumi' in page.url:
            address_fix = str(soup.find(text=re.compile('〒')).parent.text + soup.find(text=re.compile('〒')).parent.find_next().text.split('\n')[0]).replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '').replace(' ', '').replace('GoogleMap', '')
        else:
            address1 = soup.find(text=re.compile('〒')).parent.text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '').replace(' ', '') if soup.find(text=re.compile('〒')) else ''
            address2 = soup.find(text=re.compile('県')).parent.text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '').replace(' ', '') if soup.find(text=re.compile('県')) and '{' not in soup.find(text=re.compile('県')) else ''
            address_fix = address1 if address2 in address1 else address1 + address2
        
_store['address'] = address_fix.split('TEL')[0]
        print(store_name, 
_store['address'])
        # maps_link = "REDACTED URL"+ 
_store['address']
        # maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
        # # print(coords)
        # if coords:
        #   _store['lat'] = coords[0].split(',')[0].replace('@', '')
        #   _store['lon'] = coords[0].split(',')[1]
        # else:
        #   try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   except:
        #     time.sleep(5)
        #     maps_link = "REDACTED URL"+ 
_store['address']
        #     maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        #     location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   try:
        #       lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
        #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
        #   except:
        #       lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
        _store['lat'], _store['lon'] = '',''

        _store['tel_no'] = ''
        _store['open_hours'] = ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    PcAbstore(True)
