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

class SuperQueens(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
        self.session = HTMLSession()
        self.file_name = '/super/queens'.replace("/", "_")
        self.content = list()
        self.rand_agent = {'user-agent': UserAgent().random.strip()}
        self.start_time = time.time()
        self.getdata()
        self.end_time = time.time()

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

    def getdata(self):
        url = "REDACTED URL"
        session = HTMLSession()
        request =self.session.get(url, headers=self.rand_agent)
        idx = 0
        store_dict = {}
        store_column = ['store_name', '-', '-', '-', '-', '-', '-', '-', 'Address', 'URL_store', 'URL_Tenant', 'Open_Hours', 'lat', 'lon', 'Tel_No', 'GLA', 'scrape_date']
        - = 'クイーンズ伊勢丹'
        - = 'SM'
        - = '/super/queens'
        - = "QUEEN'S ISETAN"
        - = 'super'
        bhs_jepang1 = 'ショッピング'
        bhs_jepang2 = 'スーパー'
        URL_Tenant = ''
        gla = 'null'
        scrape_date = datetime.date.today().strftime('%m/%d/%Y')
        soup = BeautifulSoup(request.html.html, 'html.parser')
        stores = soup.select('table.shopTbl01:not(:last-child) tr:not(:first-child)')
        for store in stores:
            idx += 1
            store_name = store.find('a').get_text(strip=True)
            URL_store = "REDACTED URL"+ store.find('a')['href']
            print(URL_store)
            request_store =self.session.get(URL_store)
            soup_store = BeautifulSoup(request_store.html.html, 'html.parser')

            address = ''.join(soup_store.find('div', 'c-shopDetailMainContents').get_text(strip=True, separator='\n').split('\n')[0])
            telf = ' '.join(soup_store.find('dt', text=re.compile('電話番号')).find_next('dd').get_text(strip=True, separator=' ').split())
            open_time = ' '.join(soup_store.find('dt', text=re.compile('営業時間')).find_next('dd').get_text(strip=True, separator=' ').split())
            maps_link = "REDACTED URL"+ address
            maps = self.session.get(maps_link, allow_redirects=True)
            coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
            if coords:
                lat = coords[0].split(',')[0].replace('@', '')
                lon = coords[0].split(',')[1]
            else:
                lat = ''
                lon = ''
            print(f'{store_name} == {address} == {telf} == {open_time}')
            print(lat, lon)
            store_dict[idx] = [store_name, -, -, -, -, -, bhs_jepang1, bhs_jepang2, address, URL_store, URL_Tenant, open_time, lat, lon, telf, gla, scrape_date]

            self.content = pd.DataFrame.from_dict(store_dict, orient='index', columns=store_column)
            # df.to_csv('/mnt/c/dags/hasil/_super_queens.csv', index=False)
            print(idx)
            time.sleep(2)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    SuperQueens(True)

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
