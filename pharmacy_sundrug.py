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

class PharmacySundrug(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/pharmacy/sundrug'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        self.url = "REDACTED URL"
        self.get_area(self.url)
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

    def get_area(self, url):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call self.get_data(self.url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        areas = soup.find('table', {'class': 'mapListTable'}).find_all('a')
        for area in areas:
            area_link = domain + area['href']
            self.get_page(area_link)
        try:
            next_button = soup.find('table', {'id': 'searchAddrTablePage'}).find('a', text='次へ')['href']
            self.get_area(domain + next_button)
        except TypeError:
            pass

    def get_page(self, url):
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        area_page =self.session.get(url, headers=headers)
        area_soup = BeautifulSoup(area_page.html.html, 'html.parser')
        domain = "REDACTED URL"
        store_lists = area_soup.find_all('div', {'class': 'shopBox'})
        for store in store_lists:
            if '薬局' in store.find('h3').text:
                link = store.find('a')['href']
                self.content.append(self.get_data(link))
                self.save_data()
                print(len(self.content))
                time.sleep(1)
        try:
            next_button_area = domain + area_soup.find('li', 'btNext').find('a')['href']
            self.get_page(next_button_area)
        except AttributeError:
            pass

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find('div', 'topTableBox')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h1').text.strip().replace('\u3000', ' ').replace('\xa0', ' ').replace('\n', '')
_store['address'] = details.find('div', 'addressInfo').text.replace('\u3000', ' ').replace('\xa0', ' ')
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        try:
            _store['tel_no'] = details.find('dt', text=re.compile('電話番号')).find_next_sibling('dd').find('span').text.replace('電話 ', '')
        except AttributeError:
            _store['tel_no'] = ''
        try:
            _store['open_hours'] = details.find('dt', text='営業時間').find_next_sibling('dd').text.replace('\u3000', '').replace('\xa0', '')
        except AttributeError:
            _store['open_hours'] = ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    PharmacySundrug(True)

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
