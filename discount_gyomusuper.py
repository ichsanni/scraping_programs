from requests.exceptions import ConnectionError
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import AsyncHTMLSession, HTMLSession
import pandas as pd
import re, os
from scraping.scraping import CleaningData
from google.cloud import storage
import asyncio, pyppeteer

# Kelas baru untuk override AsyncHTMLSession, 
# terutama di method browser() agar tidak bentrok dengan
# async milik Flask
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
            self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not(self.verify), headless=True, handleSIGINT=False, handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

        return self._browser


class DiscountGyomusuper(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.asession = None
    self.content = list()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'scapingteam-a2d07bd2f068.json'
    
    url = "REDACTED URL"
    start = time.time()
    # Tambahan untuk async render
    loop = asyncio.new_event_loop()
    
    loop.run_until_complete(self.get_page(url))
    end = time.time()
    print("============ ", (end - start)/60, " minute(s) ============")

    # ===== MULAI KONEKSI KE BIGQUERY
    client = storage.Client()
    bucket = client.get_bucket('scrapingteam')
    x = pd.DataFrame(self.content)

    # CLEANING 1: PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
    x['url_tenant'] = None

    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    try:
      self.df_clean = self.clean_data(x)
          
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_discount_gyomusuper.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_discount_gyomusuper.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
      raise

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))
        
  async def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    print(url)
    if not self.asession: self.asession = AsyncHTMLSessionFixed()
    page = await self.asession.get(url)
    try:
      await page.html.arender(timeout=0, sleep=1)
    except ConnectionError:
      time.sleep(5)
      await page.html.arender(timeout=0, sleep=1)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    directory = "REDACTED URL"

    links = soup.find_all('div', {'class':'list_box_l'})

    for link in links:
      subdir = link.find('a').get('href')
      data = await self.get_data(directory + subdir)
      if data is not None:
        self.content.append(data)
        print(len(self.content))

  async def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individuually by calling get_map_data(url)
    '''
    print(url)
    try:
      page = await self.asession.get(url)
      await page.html.arender(timeout=0, sleep=1)
    except ConnectionError:
      time.sleep(5)
      page = await self.asession.get(url)
      await page.html.arender(timeout=0, sleep=1)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    _store = dict()

    _store['url_store'] = url
    
    try:
      _store['store_name'] = soup.find('div', {'class':'detail_title'}).find_all('p', {'class':'left'})[-1].text.replace('\n', '').replace('\r', '').replace('\t', '').replace('\u3000', '')
_store['address'] = soup.find('div', {'class':'dboxright'}).text.replace('\n', '').replace('\r', '').replace('\t', '').replace('\u3000', '')

      maps = soup.find('iframe').get('src')
      location = re.findall(r'\d{2,3}\.\d{3,}', maps)
      if location:
        if float(location[0]) < 90.0:
          maps_link = "REDACTED URL"+ 
_store['address']

          _store['lat'], _store['lon'] = '', ''

        else:
          maps_link = "REDACTED URL"+ 
_store['address']

          _store['lat'], _store['lon'] = '', ''


      _store['tel_no'] = soup.find_all('div', {'class':'col-xs-9'})[0].text.replace('-', '').replace('\n', '').replace('\r', '').replace('\t', '').replace('\u3000', '')

      _store['open_hours'] = soup.find_all('div', {'class':'col-xs-9'})[2].text.replace('\n', '').replace('\r', '').replace('\t', '').replace('\u3000', '')

      _store['gla'] = 'Null'
      
      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
      
      # print(_store)
      return _store
    except AttributeError:
      not_found_404 = soup.find('div', {'class':'bread_cramb'}).text

      if '404' in not_found_404:
        return None

if __name__ == '__main__':
    DiscountGyomusuper()