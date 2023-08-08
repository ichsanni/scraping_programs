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

class SuperMaxvalu(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/super/maxvalu'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.asession = None
        self.content = list()
        self.url = ["REDACTED URL"
                    'https://www.aeon.com/store/list/%E3%82%B9%E3%83%BC%E3%83%91%E3%83%BC%E3%83%9E%E3%83%BC%E3%82%B1%E3%83%83%E3%83%88/%E3%83%9E%E3%83%83%E3%82%AF%E3%82%B9%E3%83%90%E3%83%AA%E3%83%A5%E3%82%A8%E3%82%AF%E3%82%B9%E3%83%97%E3%83%AC%E3%82%B9/']
        self.start = time.time()
        loop = asyncio.new_event_loop()

        for url in self.url:
            loop.run_until_complete(self.get_page(url))
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
        print(url)
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36 Edg/94.0.992.38'}
        page = await self.asession.get(url, headers=headers, allow_redirects=False)
        domain = "REDACTED URL"
        await page.html.arender(timeout=0)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        store_lists = soup.find_all('a', {'class': 'storeName'})
        for store in store_lists:
            if 'マックスバリュ' in store.text:
                link = store['href'] if 'http' in store['href'] else domain + store['href']
            self.content.append(self.get_data(link))
            self.save_data()
            print(len(self.content))
        last_page_btn = soup.find('div', {'class': 'pager'}).find_all('li')
        print(last_page_btn[-1].text)
        if '>' in last_page_btn[-1].text:
            next_button = domain + last_page_btn[-1].find('a')['href']
            await self.get_page(next_button)
        else:
            pass

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        while True:
            try:
                page = self.session.get(url)
                break
            except:
                time.sleep(1)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find_all('td')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h1').text
_store['address'] = details[0].text.replace('\n', '').replace(' ', '')
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = details[1].text.replace('-', '').replace('\n', '').replace(' ', '')
        _store['open_hours'] = ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        time.sleep(1)
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    SuperMaxvalu(True)


