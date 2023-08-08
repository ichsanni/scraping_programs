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

class GlassesYonezawa(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/glasses/yonezawa'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
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
        page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        store_lists = soup.find_all('tr')[1:]
        for store in store_lists:
            self.content.append(self.get_data(store['data-href']))
            self.save_data()
            print(len(self.content))
            time.sleep(0.5)
        try:
            next_page = soup.find('i', 'fa-angle-right').parent['href']
            time.sleep(3)
            print(next_page)
            self.get_page(next_page)
        except AttributeError:
            pass

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individually by calling get_map_data(self.url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h3').textaddress = soup.find('th', text=re.compile('住所')).find_next_sibling('td')
        
_store['address'] = address.text.replace('グーグルマップでみる', '').replace('\n', '').replace('\u3000', ' ').replace('\r', '')
        try:
            maps_link = "REDACTED URL"+ 
_store['address']
            maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        except TypeError:
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''
        try:
            _store['tel_no'] = soup.find('th', text=re.compile('電話番号')).find_next_sibling('td').text.replace('\n', '').replace('\u3000', '').replace('\r', '')
        except AttributeError:
            _store['tel_no'] = ''
        try:
            _store['open_hours'] = soup.find('th', text=re.compile('営業時間')).find_next_sibling('td').text.replace('\n', '').replace('\u3000', '').replace('\r', '')
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
    GlassesYonezawa(True)

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
