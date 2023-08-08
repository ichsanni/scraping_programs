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

class ComicWondergoo(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/comic/wondergoo'.replace("/", "_")
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
        store_lists = soup.find('table', 'shop__shop-list').find_all('a')
        for store in store_lists:
            link = "REDACTED URL"+ store['href']
            self.content.append(self.get_data(link))
            self.save_data()
            print(len(self.content))
        next_page = soup.find('div', 'pager').find_all('a')[-1]['href']
        if next_page.replace('#', '') != '':
            link = "REDACTED URL"+ next_page
            print(link)
            self.get_page(link)

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individually by calling self.get_map_data(self.url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        while True:
            try:
                page =self.session.get(url, headers=headers)
                soup = BeautifulSoup(page.html.html, 'html.parser')
                time.sleep(2)
                soup.find('strong', 'shop__name--shop').text
                break
            except AttributeError:
                time.sleep(5)
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('strong', 'shop__name--shop').text
_store['address'] = soup.find('dt', text='所在地').find_next_sibling('dd').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', ' ')
        _store['lat'], _store['lon'] = '', ''
        _store['tel_no'] = soup.find('dt', text='電話番号').find_next_sibling('dd').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
        _store['open_hours'] = soup.find('dt', text='営業時間').find_next_sibling('dd').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
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
    ComicWondergoo(True)

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
