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

class PetAeonpet(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/pet/aeonpet'.replace("/", "_")
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
  If no, call get_data(self.url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        areas =self.session.get(url, headers=headers).json()
        content = list()
        for area in areas:
            cities = area['cities']
            for city in cities:
                store_lists = city['stores']
                for store in store_lists:
                    _store = dict()
                    _store['url_store'] = "REDACTED URL"
                    _store['store_name'] = store['name']
_store['address'] = store['postalCode'].replace('\r', '').replace('\n', '') + ' ' + store['address'].replace('\r', '').replace('\n', '').strip()
                    maps_link = "REDACTED URL"+ 
_store['address']

                    _store['lat'], _store['lon'] = '', ''

                    page =self.session.get(_store['url_store'], headers=headers)
                    print(page.url)
                    soup = BeautifulSoup(page.html.html, 'html.parser')
                    try:
                        details = soup.find('img', {'src': '/assets/ap_base_store_logo/ap_base_store_logo-logo_img-1.jpg'}).parent.parent
                    except AttributeError:
                        continue
                    try:
                        _store['tel_no'] = ''.join([x.string for x in details.find('span', text=re.compile('TEL')).parent.parent.find('dd').contents]).replace('\r', '').replace('\n', '')
                    except AttributeError:
                        _store['tel_no'] = ''
                    try:
                        _store['open_hours'] = ''.join([x.string for x in details.find('span', text=re.compile('営業時間')).parent.parent.find('dd').contents]).replace(' ', '').replace('\r', '').replace('\n', '')
                    except TypeError:
                        _store['open_hours'] = ''
                    _store['gla'] = ''
                    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                    # print(_store)
                    self.content.append(_store)
                    self.save_data()
                    print(len(self.content))
                    time.sleep(0.5)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    PetAeonpet(True)

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
