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

class HomecenterHomac(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/homecenter/homac'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        for pref in range(1, 48):
            pref = str(pref).zfill(2)
            print('pref:', pref)
            self.url = f"REDACTED URL"
            self.get_page(self.url)
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
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        store_lists = soup.find('ul', {'class': 'shop-favList'}).find_all('li')
        for store in store_lists:
            if 'dcm-hc' in store.find('a')['href']:
                link = store.find('a')['href']
                print(store.find('a').text)
                self.content.append(self.get_data_homac(link, store.find('a').text))
            else:
                link = domain + store.find('a')['href']
                self.content.append(self.get_data(link))
            self.save_data()
            print(len(self.content))

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling self.get_map_data(url)
  """
        print(url)
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find_all('td')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h1').find('span').text.replace('\u3000', '').strip()
_store['address'] = details[0].text.replace(' 地図・混雑情報を見る', '').replace('\n', '')
        maps_link = details[0].find('a')['href']

        _store['lat'], _store['lon'] = '', ''
        tel_no = re.findall('(\\d+)[^\\d\\w\\s]{0,1}', details[1].text)
        _store['tel_no'] = ''.join(tel_no[:3])
        _store['open_hours'] = details[2].text.replace('\n', '').replace('\t', '')
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        return _store

    def get_data_homac(self, url, title):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling self.get_map_data(url)
  """
        print(url)
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = title.strip()try:
            details = soup.find('h2', text=re.compile(title.replace('ホーマック', '').replace('DCM', '').strip())).find_next_sibling('div')
        except AttributeError:
            details = soup.find('h2', text=re.compile(title.split(' ')[-1].strip())).find_next_sibling('div')
        
_store['address'] = details.find('dt', text='住所').find_next_sibling('dd').text
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''
        try:
            _store['tel_no'] = details.find('dt', text='電話番号').find_next_sibling('dd').text
        except AttributeError:
            _store['tel_no'] = ''
        try:
            _store['open_hours'] = details.find('dt', text='営業時間').find_next_sibling('dd').text
        except AttributeError:
            _store['open_hours'] = ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        return _store

    def get_map_data(self, url_map):
        headers = {'referer': "REDACTED URL"Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        page =self.session.get(url_map, headers=headers, allow_redirects=True)
        return page

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    HomecenterHomac(True)

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
