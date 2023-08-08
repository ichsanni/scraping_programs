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

class YakinikuAmiyakitei(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/yakiniku/amiyakitei'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        urls = ["REDACTED URL" 'http://www.amiyakitei.co.jp/amiyaki-tenpo/index.html']
        for url in urls:
            self.get_page(url)
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
  If no, call get_data(url) and visit individual pages.
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        store_lists = list()
        if 'amiyaki-kantou' in url:
            store_lists += soup.find_all('table', 'c-list_news')
            link = url
        areas = soup.find('div', {'id': 'a-billboard'}).find_all('a')
        for area in areas:
            if 'amiyaki-tenpo' not in area['href']:
                continue
            link = "REDACTED URL"+ area['href'].replace('../', '')
            print(link)
            store_page =self.session.get(link, headers=headers)
            store_soup = BeautifulSoup(store_page.html.html, 'html.parser')
            store_lists += store_soup.find_all('table', 'c-list_news')
        for store in store_lists:
            _store = dict()
            _store['url_store'] = link
            try:
                _store['store_name'] = store.find('h2').text.strip()
            except AttributeError:
                _store['store_name'] = store.find('span', 'd-bold').text.strip()try: 
_store['address'] = store.find(text='住所').parent.parent.parent.find_next_sibling('td').text.replace('\n', '').replace('\t', '').replace('\r', '').strip()
            except AttributeError: continue

            _store['lat'], _store['lon'] = '', ''
            try:
                _store['tel_no'] = store.find(text=re.compile('電話番号')).parent.parent.find_next_sibling('td').text.replace('tel:', '').replace('\n', '').replace('\t', '').replace('\r', '').strip()
            except AttributeError:
                _store['tel_no'] = ''
            _store['open_hours'] = store.find(text='営業時間').parent.parent.find_next_sibling('td').text.replace('\n', '').replace('\t', '').replace('\r', '').strip()
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
            # print(_store)
        self.save_data()
        print(len(self.content))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    YakinikuAmiyakitei(True)

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
