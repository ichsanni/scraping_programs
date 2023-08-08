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

class SuperTamade(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/super/tamade'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.asession = None
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
  If no, call self.get_data(self.url) and visit individual pages.
  """
        self.session = HTMLSession()
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        content = list()
        areas = soup.find_all('table')
        loop = asyncio.new_event_loop()
        

        for area in areas:
            store_lists = area.find_all('tr')
            for store in store_lists:
                try:
                    link = store.find('a').get('href')
                    self.content.append(loop.run_until_complete(self.get_data(domain + link)))
                    self.save_data()
                    print(len(self.content))
                except AttributeError as err:
                    pass

    async def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        headers = {'cookie': '_ga=GA1.3.1728115803.1630551443; _gid=GA1.3.1029018382.1630551443', 'host': 'www.supertamade.co.jp', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        page = await self.asession.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        await page.html.arender(timeout=0)
        details = soup.find_all('td')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = details[0].text
_store['address'] = details[1].text.replace('\n', '').replace(' ', '')
        # location = re.findall('\\d{2,3}\\.\\d{3,}', page.html.html)
        # if location:
        #     maps_link = "REDACTED URL"+ 
_store['address']
        #     maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        #     coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
        #     # print(coords)
        #     if coords:
        #       _store['lat'] = coords[0].split(',')[0].replace('@', '')
        #       _store['lon'] = coords[0].split(',')[1]
        #     else:
        #       try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #       except:
        #         time.sleep(5)
        #         maps_link = "REDACTED URL"+ 
_store['address']
        #         maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        #         location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #       try:
        #           lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
        #           _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
        #       except:
        #           lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = details[2].text.replace('-', '')
        numbers = re.findall('\\d', details[4].text)
        if numbers:
            _store['open_hours'] = details[4].text.replace('\n', '').replace(' ', '')
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
    SuperTamade(True)
