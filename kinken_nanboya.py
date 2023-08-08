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

class KinkenNanboya(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/kinken/nanboya'.replace("/", "_")
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
        self.session = HTMLSession()
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        content = list()
        store_lists = soup.select('div.store-guide-all a')
        loop = asyncio.new_event_loop()
        
        for store in store_lists:
            if 'syuttyou-kaitori' in store['href'] or 'hakkuodo' in store['href']:
                continue
            link = store['href'] if 'http' in store['href'] else domain + store['href']
            data = loop.run_until_complete(self.get_data(link))
            if data:
                self.content.append(data)
                self.save_data()
                print(len(self.content))

    async def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individually by calling get_map_data(self.url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        page = await self.asession.get(url, headers=headers)
        await page.html.arender(timeout=0)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        try:
            _store['url_store'] = url
            _store['store_name'] = soup.find_all('li', {'itemprop': 'itemListElement'})[-1].text.strip()try:
                if soup.find('th', text=re.compile('住所')).find_next_sibling('td').find('img'):
                    address = soup.find('th', text=re.compile('住所')).find_next_sibling('td').find('img')['alt']
                else:
                    address = soup.find('th', text=re.compile('住所')).find_next_sibling('td').text.replace('Googleマップをみる', '').strip()
                
_store['address'] = address.split('»')[0]
            except AttributeError:
                return
            # maps_link = "REDACTED URL"+ 
            # maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
            # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
            # print(coords)
            # if coords:
            _store['lat'] = ''
            _store['lon'] = ''
            # else:
            #   while True:
            #     try:
            #         maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
            #         print(maps_link)
            #         location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
            #         break
            #     except:
            #         time.sleep(15)
            #   try:
            #       lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
            #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
            #   except:
            #       lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
            #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]

            _store['tel_no'] = ''
            _store['open_hours'] = soup.find('th', text='営業時間').find_next_sibling('td').text
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            time.sleep(1)
            return _store
        except IndexError:
            return

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    KinkenNanboya(True)
