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

class DrugGodai(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/drug/godai'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.data = {'exec': 'search', 'shop_area': '', 'shop_shisetsu_4': '1', 'x': '140', 'y': '19'}
        self.headers = {'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
        self.url = "REDACTED URL"
        self.start = time.time()
        self.get_page(url, data, headers)
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

    def get_page(self, url, data, headers):
        """
    Visit the link given from the gsheet,
    see if there's self.data there.
    If yes, scrape immediately.
    If no, call get_data(self.url) and visit individual pages.
    """
        domain = "REDACTED URL"
        page =self.session.post(url, data=data, headers=headers, verify=False)
        maps_link = "REDACTED URL"+ 
_store['address']
        maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        store_lists = soup.find_all('div', 'storeDetail')
        for x in range(len(store_lists)):
            _store = dict()
            details = store_lists[x].find_all('td')
            _store['url_store'] = url
            _store['store_name'] = store_lists[x].find('h4').text.replace('\n', '').replace('\t', '').replace('\xa0', '')
_store['address'] = details[2].text + details[3].text
            location = re.findall('\\d{2,3}\\.\\d{3,}', maps[x])
            if location:
                maps_link = "REDACTED URL"+ 
_store['address']

                _store['lat'], _store['lon'] = '', ''

            _store['tel_no'] = details[1].text.replace('-', '')
            _store['open_hours'] = details[0].text.replace('\u3000', '')
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            # print(_store)
            self.content.append(_store)
            print(len(self.content))
        self.save_data()
        next_button = soup.find('p', 'alignCenter').find_all('a')[-2]['href']
        offset_button = re.sub('\\D', '', next_button)
        offset_url = re.sub('\\D', '', self.url)
        if int(offset_url) < int(offset_button):
            print(domain + next_button)
            time.sleep(3)
            self.get_page(domain + next_button, self.data, self.headers)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    DrugGodai(True)

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
