import csv

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

class CardealerMitsubishi(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cardealer/mitsubishi'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.visited_links = list()
        self.PROXIES = {"REDACTED URL" 'REDACTED URL}
        self.start = time.time()
        try:
            with open('jp-cities-coordinates/jp-coord-complete.csv', 'r', encoding='utf8') as f:
                reader = csv.DictReader(f)
                index_city = 1
                for city in reader:
                    print('city:', index_city, city['region'])
                    data = {'centerZhyo_lat': city['lat'], 'centerZhyo_lon': city['lng'], 'current_lat': '', 'current_lon': '', 'mapTopIdo': float(city['lat']) + 0.407423, 'mapButtomIdo': float(city['lat']) - 0.407423, 'mapLeftKeido': float(city['lng']) - 0.602985, 'mapRightKeido': float(city['lng']) + 0.602985, 'method': 'mapDragSearch', 'mapZoom': '10', 'mode': 'dealer', 'hanshaCD': '', 'todofukenCD': '', 'urlModeFlg': '4', 'urlMeigara_cd': '', 'map_size_y': '561', 'map_size_x': '806', 'url_data': "REDACTED URL" 'dealer': ''}
                    url = "REDACTED URL"
                    self.get_page(url, data)
                    index_city += 1
                    time.sleep(1)
        except:
            raise
        if True:
            pass
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

    def get_page(self, url, data):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call self.get_data(url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        while True:
            try:
                page =self.session.post(url, headers=headers, data=data, timeout=3)
                soup = BeautifulSoup(page.html.html, 'html.parser')
                break
            except: time.sleep(10)
        domain = "REDACTED URL"
        store_lists = soup.select('div#dealer_details_sp > div#dealer_details > div')
        for store in store_lists:
            name = store.find('div', 'dealer__header__name').find('p')
            coord = store.find('a', 'mod-link-arr')['href'].split('/')[-1].split(',')
            link = name.find('a')['href']
            relink = re.findall('\\(.*\\)', link)[0].replace("'", '').replace('+', '').replace(' ', '').replace('(', '').replace(')', '')
            if relink not in self.visited_links:
                self.content.append(self.get_data(domain + relink, coord))
                self.save_data()
                print(len(self.content))
                self.visited_links.append(relink)
                time.sleep(1)

    def get_data(self, url, coord):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        while True:
            try:
                page =self.session.get(url, headers=headers, timeout=3)
                soup = BeautifulSoup(page.html.html, 'html.parser')
                break
            except: time.sleep(10)
        details = soup.find('table', 'dealer__info')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('div', 'dealer__header__name').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
_store['address'] = details.find('td', text='住所').find_next_sibling('td').text.replace('地図アプリで見る', '').replace('\n', '').replace(' ', '')

        _store['lat'], _store['lon'] = '',''

        _store['tel_no'] = details.find('div', 'tel_area').text.replace('\n', '')
        try:
            _store['open_hours'] = details.find('td', text='営業日時').find_next_sibling('td').text.replace('\n', '').replace('\u3000', '').split('．')[0].split('休業')[0]
        except AttributeError:
            _store['open_hours'] = ''
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CardealerMitsubishi(True)

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
