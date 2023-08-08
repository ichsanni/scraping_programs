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

class PharmacyKirindo(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/pharmacy/kirindo'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.url = "REDACTED URL"
        self.start = time.time()
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
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        content = list()
        prefects = soup.find('section').find_all('li')
        for pref in prefects:
            link = "REDACTED URL"+ pref.find('a').get('href').replace('./', '')
            print(link)
            page_2 = self.session.get(link)
            soup = BeautifulSoup(page_2.html.html, 'html.parser')
            rows = soup.find_all('td')[::2]
            for row in rows:
                link = row.find('a')
                if '(調剤)' in link.text:
                    try:
                        store_dict = self.get_data(link.get('href'))
                    except AttributeError:
                        continue
                    # print(store_dict)
                    self.content.append(store_dict)
                    self.save_data()
                    print(len(self.content))

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        link = "REDACTED URL"+ url.replace('./', '')
        page_3 = self.session.get(link)
        soup = BeautifulSoup(page_3.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = link
        _store['store_name'] = soup.find('h2').text.replace('\u3000', '')try:
            
_store['address'] = soup.find('dd').text.replace('\n', '').replace('\u3000', '')
            map = soup.find('a', {'class': 'map-btn'}).get('href')
            location = re.findall('\\d{2,3}\\.\\d{3,}', map)
            if location:
                maps_link = "REDACTED URL"+ 
_store['address']

                _store['lat'], _store['lon'] = '', ''

            tables = soup.find('tbody').find_all('td')
            _store['tel_no'] = str(tables[0].text.replace('-', '').replace('\n', ''))
            _store['open_hours'] = tables[2].text.replace('\u3000', '').replace('\n', '').replace('\r', '').replace('\t', '')
            _store['gla'] = 'Null'
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            return _store
        except AttributeError:
            raise AttributeError()

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    PharmacyKirindo(True)

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
