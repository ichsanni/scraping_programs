import asyncio

import pyppeteer
from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession, AsyncHTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


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

class PharmacyCreate(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '/pharmacy/create'.replace('/', '_')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.asession = None
    self.content = list()

    start = time.time()
    loop = asyncio.new_event_loop()
    
    loop.run_until_complete(self.getdata())
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    x = pd.DataFrame(self.content)
    x.columns = [y.lower() for y in x.columns]

    # CLEANING 1: PERIKSA KOORDINAT
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
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)

  async def getdata(self):
    for x in range(1,100):
        url = f"REDACTED URL"
        print("lagi di page: ", url)
        r = self.session.get(url)
        soup =  BeautifulSoup(r.html.html,'html.parser')
        page_lists = soup.find_all('li', {'class':"last"})
        print(len(page_lists))
        if len(page_lists) == 2:
            cards = soup.find('table',{'class':'storeList'}).find_all('a')
            if not self.asession: self.asession = AsyncHTMLSessionFixed()
            for card in cards:
                data_dict = {}
                url_tokos = card['href']
                print(url_tokos)
                res = await self.asession.get(url_tokos)
                await res.html.arender(timeout=0)
                soupi =  BeautifulSoup(res.html.html,'html.parser')
                data_dict['store_name'] = soupi.find('h2').text

                data_dict['-'] = 'CREATE'

                data_dict['-'] = 'Drg'

                data_dict['-'] = '/pharmacy/create'

                data_dict['-'] = 'CREATE'

                data_dict['-'] = 'pharmacy'

                data_dict['-'] = 'サービス'

                data_dict['-'] = '調剤薬局'

                data_dict['Address'] = soupi.find('div',{'class':'searchDetail_right'}).find('td').text

                data_dict['URL_store'] = url_tokos

                data_dict['URL_Tenant'] = ''

                data_dict['Open_Hours'] = soupi.find('div',{'class':'searchDetail_right'}).find_all('tbody')[1].find_all('td')[2].text

                maps_link = "REDACTED URL"+ data_dict['Address']
                maps = self.session.get(maps_link, allow_redirects=True)
                coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
                # print(coords)
                if coords:
                  data_dict['lat'] = coords[0].split(',')[0].replace('@', '')
                  data_dict['lon'] = coords[0].split(',')[1]
                else:
                  data_dict['lat'] = ''
                  data_dict['lon'] = ''

                data_dict['Tel_No'] = soupi.find('div',{'class':'searchDetail_right'}).find_all('tbody')[1].find_all('td')[0].text

                data_dict['GLA'] = ''


                today = datetime.date.today()

                data_dict['scrape_date'] = today.strftime("%m/%d/%y")

                self.content.append(data_dict)
                # review_df = pd.DataFrame(self.content)
                # review_df.to_csv('/mnt/c/dags/hasil/_pharmacy_create.csv',index = False)
                print(len(self.content))
                # print(data_dict)
        else:
            cards = soup.find('table', {'class': 'storeList'}).find_all('a')
            for card in cards:
                data_dict = {}
                url_tokos = card['href']
                print(url_tokos)
                res = await self.asession.get(url_tokos)
                await res.html.arender(timeout=0)
                soupi = BeautifulSoup(res.html.html, 'html.parser')
                data_dict['store_name'] = soupi.find('h2').text

                data_dict['-'] = 'CREATE'

                data_dict['-'] = 'Drg'

                data_dict['-'] = '/pharmacy/create'

                data_dict['-'] = 'CREATE'

                data_dict['-'] = 'pharmacy'

                data_dict['-'] = 'サービス'

                data_dict['-'] = '調剤薬局'

                data_dict['Address'] = soupi.find('div', {'class': 'searchDetail_right'}).find('td').text

                data_dict['URL_store'] = url_tokos

                data_dict['URL_Tenant'] = ''

                data_dict['Open_Hours'] = \
                soupi.find('div', {'class': 'searchDetail_right'}).find_all('tbody')[1].find_all('td')[2].text

                maps_link = "REDACTED URL"+ data_dict['Address']
                maps = self.session.get(maps_link, allow_redirects=True)
                coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
                # print(coords)
                if coords:
                  data_dict['lat'] = coords[0].split(',')[0].replace('@', '')
                  data_dict['lon'] = coords[0].split(',')[1]
                else:
                  data_dict['lat'] = ''
                  data_dict['lon'] = ''

                data_dict['Tel_No'] = \
                soupi.find('div', {'class': 'searchDetail_right'}).find_all('tbody')[1].find_all('td')[0].text

                data_dict['GLA'] = ''

                today = datetime.date.today()

                data_dict['scrape_date'] = today.strftime("%m/%d/%y")

                self.content.append(data_dict)
                # review_df = pd.DataFrame(self.content)
                # review_df.to_csv('/mnt/c/dags/hasil/_pharmacy_create.csv', index=False)
                print(len(self.content))
                # print(data_dict)
            break

if __name__ == '__main__':
  PharmacyCreate(True)
