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

class PharmacyEleven(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/pharmacy/eleven'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        self.params = (('cm', 'srch'),)
        self.data = [('kensaku', '1'), ('search_shopmng10', '1'), ('search_op8', '1')]
        self.get_page("REDACTED URL" self.params, self.data, 'post')
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

    def get_page(self, url, param='', data='', method='get'):
        """
  Visit the link given from the gsheet,
  see if there's self.data there.
  If yes, scrape immediately.
  If no, call self.get_data(url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        if method == 'post':
            page =self.session.post(url, headers=headers, params=param, data=data, allow_redirects=True)
        else:
            page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        store_lists = soup.find('ul', {'class': 'archive'}).find_all('li')
        for store in store_lists:
            link = domain + store.find('a')['href'].replace('./', '/')
            self.content.append(self.get_data(link))
            self.save_data()
            print(len(self.content))
        try:
            next_button = domain + soup.find('a', 'page-navigation__pager-link', text='次へ')['href']
            self.get_page(next_button)
        except TypeError:
            pass

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find('h4', text='調剤店情報')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h3').find('span').text
_store['address'] = soup.find('th', text='住所').find_next_sibling('td').text.replace('\n', '')
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        try:
            _store['tel_no'] = details.parent.find('th', text='電話番号').find_next_sibling('td').text.replace('\n', '')
        except AttributeError:
            _store['tel_no'] = soup.find('th', text='電話番号').find_next_sibling('td').text.replace('\n', '')
        try:
            _store['open_hours'] = details.parent.find('th', text='営業時間').find_next_sibling('td').text.replace('\n', '').replace('\u3000', '')
        except AttributeError:
            _store['open_hours'] = soup.find('th', text='営業時間').find_next_sibling('td').text.replace('\n', '').replace('\u3000', '')
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
    PharmacyEleven(True)

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
