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

class GlassesParismiki(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/glasses/parismiki'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.asession = None
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
        headers = {'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        try:
            page =self.session.get(url, headers=headers)
        except ConnectionError:
            time.sleep(2)
            page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        content = list()
        store_lists = soup.find_all('a', {'class': 'store-list-item-a'})
        loop = asyncio.new_event_loop()
        for store in store_lists:
            link = domain + store['href']

            

            data = loop.run_until_complete(self.get_data(link))
            if data != '':
                self.content.append(data)
                self.save_data()
                print(len(self.content))

    async def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling self.get_map_data(self.url)
  """
        print(url)
        if not self.asession: self.asession = AsyncHTMLSessionFixed()
        headers = {'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        page = await self.asession.get(url, headers=headers)
        await page.html.arender(timeout=0)

        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('span', 'rt_cf_s_class').text + soup.find('span', 'rt_cf_s_name').texttry:
            
_store['address'] = soup.find('th', text='住所').find_next_sibling('td').find('div', 'fw_bold').text.replace('\n', '').replace('MAP', '').strip()
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''

            _store['tel_no'] = soup.find('th', text='TEL').find_next_sibling('td').text.replace('\n', '')
            _store['open_hours'] = soup.find('th', text='営業時間').find_next_sibling('td').text.replace('\n', '')
        except AttributeError:
            try:
                other_link = soup.find('a', 'js-opm-link')['href']
                try:
                    other_page = await self.asession.get(other_link)
                except ConnectionError:
                    time.sleep(2)
                    other_page = await self.asession.get(other_link)
                other_soup = BeautifulSoup(other_page.html.html, 'html.parser')
                _store['url_store'] = other_page.url
                address = [x.text for x in other_soup.find('dt', text='住所').find_next_sibling('dd').find_all('p')]
                
_store['address'] = ''.join(address).replace('MAP', '').strip()
                maps_link = "REDACTED URL"+ 
_store['address']

                _store['lat'], _store['lon'] = '', ''

                _store['tel_no'] = other_soup.find('dt', text='TEL').find_next_sibling('dd').text.replace('\n', '')
                _store['open_hours'] = other_soup.find('dt', text='営業時間').find_next_sibling('dd').text.replace('\n', '')
            except TypeError:
                return ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        return _store

    def get_map_data(self, url):
        headers = {'referer': "REDACTED URL"Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        page = self.session.get(url, headers=headers, allow_redirects=True)
        return page

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    GlassesParismiki(True)


