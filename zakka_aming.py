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

class ZakkaAming(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/zakka/aming'.replace("/", "_")
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
        prefectures = soup.find('div', {'class': 'p-sec-shoplist__list'}).find_all('a')
        for pref in prefectures:
            link = pref.get('href')
            page_2 = self.session.get(link)
            soup_2 = BeautifulSoup(page_2.html.html, 'html.parser')
            store_lists = soup_2.find('div', {'class': 'p-sec-shoplist__txt'}).find_all('a')
            for store in store_lists:
                store_link = store.get('href')
                self.content.append(self.get_data(store_link))
                self.save_data()
                print(len(self.content))

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find_all('dd')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h3').text
_store['address'] = details[1].text.replace('\r', '').replace('\n', '')
        # maps_link = "REDACTED URL"+ 
_store['address']
        # maps = self.session.get(maps_link, allow_redirects=True)
        # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
        # # print(coords)
        # if coords:
        #   _store['lat'] = coords[0].split(',')[0].replace('@', '')
        #   _store['lon'] = coords[0].split(',')[1]
        # else:
        #   try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   except:
        #     time.sleep(5)
        #     maps_link = "REDACTED URL"+ 
_store['address']
        #     maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        #     location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   try:
        #       lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
        #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
        #   except:
        #       lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
        #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
        _store['lat'] = ''
        _store['lon'] = ''
        _store['tel_no'] = details[0].text.replace('-', '')
        _store['open_hours'] = details[2].text.replace('\r', '').replace('\n', '')
        _store['gla'] = 'Null'
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    ZakkaAming(True)

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
