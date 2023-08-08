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

class BabyAkachanhonpo(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/baby/akachanhonpo'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
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
        print(url)
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        link = "REDACTED URL"
        areas = soup.find_all('li', {'class': 'Directory-listItem'})
        if len(areas) == 0:
            store_lists = soup.find_all('a', {'class': 'Teaser-titleLink'})
            for store in store_lists:
                path = store.get('href').replace('../', '')
                if re.sub('\\d', '', path) != '':
                    self.get_page(link + path)
                else:
                    print(link + path)
                    data = self.get_data(link + path)
                    if data:
                        self.content.append(data)
                        self.save_data()
                        print(len(self.content))
        else:
            for store in areas:
                path = store.find('a')['href'].replace('../', '')
                if re.sub('\\d', '', path) != '':
                    self.get_page(link + path)
                else:
                    print(link + path)
                    data = self.get_data(link + path)
                    if data:
                        self.content.append(data)
                        self.save_data()
                        print(len(self.content))

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individually by calling get_map_data(self.url)
  """
        page_3 = self.session.get(url)
        soup_3 = BeautifulSoup(page_3.html.html, 'html.parser')
        details = soup_3.find('div', {'class': 'Core-table'}).find_all('div', {'class': 'Core-tableUnit--white'})
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup_3.find('p', {'class': 'c-bread-crumbs-item--storeName'}).text.replace('\u3000', '')
_store['address'] = details[0].text.replace('\u3000', '')
       
       
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        try:
            _store['tel_no'] = details[1].find('div', {'id': 'phone-main'}).text.replace('-', '')
        except AttributeError:
            _store['tel_no'] = ''
        try:
            _store['open_hours'] = details[2].text.replace('Store Hours:Day of the WeekHours', '').replace('\n', '')
        except IndexError:
            return None
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
    BabyAkachanhonpo(True)

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
