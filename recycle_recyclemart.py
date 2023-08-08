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

class RecycleRecyclemart(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/recycle/recyclemart'.replace("/", "_")
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
        PROXIES = {"REDACTED URL" 'REDACTED URL}
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers, proxies=PROXIES)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        areas = soup.find_all('h3', 'map-area')
        for area in areas:
            area_url = area.find('a')['href']
            print(area_url)
            area_page =self.session.get(area_url, headers=headers, )    # proxies=PROXIES)
            area_soup = BeautifulSoup(area_page.html.html, 'html.parser')
            store_lists = area_soup.find_all('h3', 'shopList-title')
            for store in store_lists:
                same_web = True
                try:
                    if '/shop/' not in store.find('a')['href']:
                        same_web = False
                except TypeError:
                    continue
                store_url = store.find('a')['href']
                store_name = store.text
                address = store.parent.find('dt', text='住所').find_next_sibling('dd').text
                tel_no = store.parent.find('dt', text='電話番号').find_next_sibling('dd').text
                self.content.append(self.get_data(store_url, store_name, address, tel_no, same_web))
                self.save_data()
                print(len(self.content))
                time.sleep(0.5)

    def get_data(self, url, store_name=None, address=None, tel_no=None, same_web=True):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individually by calling self.get_map_data(self.url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        _store = dict()
        if same_web:
            page =self.session.get(url + 'shop', headers=headers)
            soup = BeautifulSoup(page.html.html, 'html.parser')
            try:
                details = [x for x in soup.find('section', {'id': 'store'}).text.split('\n')]
            except AttributeError:
                details = list()
            try:
                _store['open_hours'] = [x for x in details if '営業時間' in x][0].replace('【営業時間】', '').strip()
            except IndexError:
                _store['open_hours'] = ''
        else:
            _store['open_hours'] = ''
        _store['url_store'] = url
        _store['store_name'] = store_name.replace('\u3000', ' ').replace('\n', '').replace('\t', '').replace('\r', '')
_store['address'] = address.replace('\u3000', ' ').replace('\n', '').replace('\t', '').replace('\r', '')
        _store['tel_no'] = tel_no.replace('\u3000', ' ').replace('\n', '').replace('\t', '').replace('\r', '')
        _store['lat'], _store['lon'] = '', ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        return _store

    def get_map_data(self, url):
        headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers, allow_redirects=True)
        return page

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    RecycleRecyclemart(True)

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
