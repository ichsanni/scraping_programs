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

class FurnitureSmilekan(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/furniture/smilekan'.replace("/", "_")
        self.content = list()
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
        content = list()
        for p in range(1, 48):
            print(url + str(p) + '&f%5B%5D=furniture')
            page =self.session.get(url + str(p) + '&f%5B%5D=furniture')
            soup = BeautifulSoup(page.html.html, 'html.parser')
            domain = "REDACTED URL"
            store_lists = soup.find_all('div', {'class': 'shop-box'})
            for store in store_lists:
                name = store.find('h3').text.replace('\n', '')
                link = store.find('h3').find('a')['href'].replace('./', '/')
                if store.find('div', {'class': 'shop-summary-address'}):
                    addr = store.find('div', {'class': 'shop-summary-address'}).text
                else:
                    addr = ''
                if store.find('div', {'class': 'shop-summary-businesshours'}):
                    hours = store.find('div', {'class': 'shop-summary-businesshours'}).text.replace('\n', '').split('営業時間:')[1]
                else:
                    hours = ''
                if store.find('div', {'class': 'shop-summary-telephone'}):
                    phone = store.find('div', {'class': 'shop-summary-telephone'}).text.split(':')[1].replace('-', '')
                else:
                    phone = ''
                if 'http' in link:
                    self.content.append(self.get_data(link, name, addr, hours, phone, same=False))
                else:
                    self.content.append(self.get_data(domain + link, name, addr, hours, phone, same=True))
                self.save_data()
                print(len(self.content))

    def get_data(self, url, name, address, open_hours, phone_num, same):
        """
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individuually by calling get_map_data(self.url)
    """
        print(url)
        if same:
            page = self.session.get(url)
            soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = name
_store['address'] = address
        if same:
            maps = soup.find('div', {'class': 'shop-map-app'}).find('a')['href']
            location = re.findall('\\d{2,3}\\.\\d{3,}', maps)
            if location:
                maps_link = "REDACTED URL"+ 
_store['address']

                _store['lat'], _store['lon'] = '', ''

        else:
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = phone_num
        _store['open_hours'] = open_hours
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        time.sleep(1)
        # print(_store)
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    FurnitureSmilekan(True)

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
