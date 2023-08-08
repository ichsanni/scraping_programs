import asyncio
import json

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

class CafeColorado(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cafe/colorado'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.asession = None
        self.pg_code = list()
        self.content = list()
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
        self.session = HTMLSession()
        page = self.session.get(url, headers=headers)
        domain = "REDACTED URL"
        home_json = json.loads(page.html.html)
        loop = asyncio.new_event_loop()
        
        for pref in home_json['data']:
            pref_lat = pref['lat']
            pref_lon = pref['lng']
            pref_domain = f"REDACTED URL"
            self.session = HTMLSession()
            area_page =self.session.get(pref_domain, headers=headers)
            area_json = json.loads(area_page.html.html)
            for area in area_json['data']:
                area_lat = area['lat']
                area_lon = area['lng']
                area_domain = f"REDACTED URL"
                print(area_domain)
                time.sleep(0.5)
                self.session = HTMLSession()
                point_page =self.session.get(area_domain, headers=headers)
                point_json = json.loads(point_page.html.html)
                for point in point_json['items']:
                    if 'コロラド' in point['name'] and point['key'] not in self.pg_code:
                        link = domain + point['key']
                        self.content.append(loop.run_until_complete(self.get_data(link, point['latitude'], point['longitude'])))

                        self.save_data()
                        print(len(self.content))
                        self.pg_code.append(point['key'])

    async def get_data(self, url, lat, lon):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        page = await self.asession.get(url, headers=headers)
        await page.html.arender(sleep=1)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h1').text
_store['address'] = soup.find('span', text=re.compile('Postal')).find_next().text + soup.find('span', text=re.compile('Address')).find_next().text
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''
        _store['tel_no'] = soup.find('span', text=re.compile('TEL')).find_next().text
        _store['open_hours'] = soup.find('span', text=re.compile('Business hours')).find_next().text.replace('\n', '').replace('\u3000', '')
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CafeColorado(True)

