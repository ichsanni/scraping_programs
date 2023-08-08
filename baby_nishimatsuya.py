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

class BabyNishimatsuya(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/baby/nishimatsuya'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.count = 0
        while True:
            self.url = "REDACTED URL"+ str(self.count)
            self.start = time.time()
            self.res = self.get_page(self.url)
            if self.res == 0:
                break
            self.count += 1
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
  If no, call self.get_data(url) and visit individual pages.
  """
        print(url)
        headers = {'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        while True:
            try:
                page =self.session.get(url, headers=headers)
                time.sleep(1)
                soup = BeautifulSoup(page.html.html, 'html.parser')
                pages = soup.select_one('div.pagenaviarea-center').text.split('/')
                break
            except:
                time.sleep(5)
        domain = "REDACTED URL"
        store_lists = soup.find_all('tr')
        for store in store_lists:
            try:
                link = domain + store.find('a')['href']
                print(link)
                try:
                    self.content.append(self.get_data(link))
                except AttributeError:
                    time.sleep(5)
                    self.content.append(self.get_data(link))
                self.save_data()
                print(len(self.content))
                time.sleep(1)
            except TypeError:
                pass
        if len(store_lists) == 0 and pages[0] < pages[1]:
            x = self.get_page(self.url)
            return x
        return len(store_lists)

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(url)
  """
        headers = {'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        try:
            page =self.session.get(url, headers=headers)
            time.sleep(0.5)
        except ConnectionError:
            time.sleep(2)
            page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('div', {'id': 'shop-title'}).text.replace('\r', '').replace('\n', '').replace('\t', '')
_store['address'] = soup.find('th', text='住\u3000所').find_next('td').text
        _store['lat'], _store['lon'] = '',''
        _store['tel_no'] = soup.find('th', text='TEL').find_next('td').text
        _store['open_hours'] = soup.find('th', text='営業時間').find_next('td').text
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    BabyNishimatsuya(True)

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
