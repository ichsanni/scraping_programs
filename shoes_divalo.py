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

class ShoesDivalo(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
        self.session = HTMLSession()
        self.file_name = '/shoes/divalo'.replace("/", "_")
        self.content = list()
        self.headers = {'user-agent': UserAgent().random.strip()}
        self.url = "REDACTED URL"
        self.master_list = []
        self.start_time = time.time()
        self.getdata()

        x = pd.DataFrame(self.content)
        if len(x) == 0:
            raise ValueError('Empty df')
        x.columns = [y.lower() for y in x.columns]
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

    def getdata(self, step=0):
        print(step)

        req =self.session.get(self.url + 'step=' + str(step), headers=self.headers)
        soup = BeautifulSoup(req.html.html, 'html.parser')
        cards = soup.find('ul', {'class': 'results'}).find_all('li')
        for card in cards:
            data_dict = {}
            try:
                store_name = card.find('dt').text
                address = card.find('li', {'class': 'address'}).text.replace('\u3000', '')
                tel_no = card.find('li', {'class': 'tel'}).text.strip('電話：').replace('-', '')
                url_store = "REDACTED URL"+ card.find('dt').find_next('a')['href']
            except AttributeError:
                continue
            req_store =self.session.get(url_store, headers=self.headers)
            soup_store = BeautifulSoup(req_store.html.html, 'html.parser')
            open_hours = soup_store.find('dl', {'class': 'business_hours'}).find_next('dd').text.strip().replace('\u3000', '')
            lat_lon = re.findall('[0-9]+\\.[0-9]+', req_store.html.html)
            (lat, lon) = (lat_lon[6], lat_lon[7])
            data_dict['store_name'] = store_name
            data_dict['-'] = 'ディバロ'
            data_dict['-'] = 'SS'
            data_dict['-'] = '/shoes/divalo'
            data_dict['-'] = 'Divalo'
            data_dict['-'] = 'shoes'
            data_dict['-'] = 'ショッピング'
            data_dict['-'] = 'シューズ'
            data_dict['address'] = address
            data_dict['URL_store'] = url_store
            data_dict['URL_Tenant'] = ''
            data_dict['Open_Hours'] = open_hours
            maps_link = "REDACTED URL"+ data_dict['address']
            maps = self.session.get(maps_link, allow_redirects=True)
            coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
            # print(coords)
            if coords:
                data_dict['lat'] = coords[0].split(',')[0].replace('@', '')
                data_dict['lon'] = coords[0].split(',')[1]
            else:
                data_dict['lat'] = ''
                data_dict['lon'] = ''
            data_dict['Tel_No'] = tel_no
            data_dict['GLA'] = 'null'
            today = datetime.date.today()
            data_dict['scrape_date'] = today.strftime('%m/%d/%y')
            self.content.append(data_dict)
            # print(data_dict)
            # df = pd.DataFrame(self.content)
            # df.to_csv('/mnt/c/dags/src2/_shoes_divalo.csv', index=False)
            print(len(self.content))
        last_page = soup.find('menu', {'id': 'prev_next'}).find_all('button')
        if last_page[-3].text != str(step):
            step += 1
            self.getdata(step)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    ShoesDivalo(True)

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
