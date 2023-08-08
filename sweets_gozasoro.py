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

class SweetsGozasoro(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/sweets/gozasoro'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.start = time.time()
        self.get_page()
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

    def get_page(self):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call get_data(url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        params = (('shops', 's'),)
        data = [('forsearch', 'kara'), ('add[]', '北海道'), ('add[]', '埼玉県'), ('add[]', '千葉県'), ('add[]', '東京都'), ('add[]', '神奈川県'), ('add[]', '静岡県'), ('add[]', '岐阜県'), ('add[]', '三重県'), ('add[]', '愛知県'), ('add[]', '京都府'), ('add[]', '滋賀県'), ('add[]', '和歌山県'), ('add[]', '奈良県'), ('add[]', '大阪府'), ('add[]', '兵庫県'), ('add[]', '岡山県'), ('add[]', '広島県'), ('add[]', '徳島県')]
        url = "REDACTED URL"
        page =self.session.post(url, headers=headers, params=params, data=data)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        store_lists = soup.find_all('dl')
        for store in store_lists:
            _store = dict()
            _store['url_store'] = url
            _store['store_name'] = store.find('dt').text.replace(' ', '').replace('\r\n', '').replace('MAP', '')
_store['address'] = store.find('p').text
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''
            _store['tel_no'] = [x.text for x in store.find_all('p') if 'Tel' in x.text][0].replace('Tel: ', '')
            _store['open_hours'] = ''
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            # print(_store)
            self.content.append(_store)
            print(len(self.content))
        self.save_data()

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    SweetsGozasoro(True)

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
