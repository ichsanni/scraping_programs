import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import AsyncHTMLSession, HTMLSession
import pandas as pd
import re, json, os
from scraping.scraping import CleaningData
from google.cloud import storage
import pyppeteer, asyncio


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
      self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
                                             handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

    return self._browser


class HandcraftTokai(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.asession = None
    start = time.time()
    self.domain = "REDACTED URL"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'scapingteam-a2d07bd2f068.json'
    # Tambahan untuk async render
    loop = asyncio.new_event_loop()
    
    loop.run_until_complete(self.get_api(self.domain))

    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

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

    try:
      self.df_clean = self.clean_data(x)

      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_handcraft_tokai.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')
    except:
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_handcraft_tokai.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')
      raise

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  async def get_api(self, url):
    headers = {
      'Connection': 'keep-alive',
      'sec-ch-ua': '"Chromium";v="94", "Microsoft Edge";v="94", ";Not A Brand";v="99"',
      'Accept': 'application/json, text/javascript, */*; q=0.01',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36 Edg/94.0.992.38',
      'Origin': "REDACTED URL"
      'Referer': "REDACTED URL"
    }
    for x in range(1, 48):
      data = {
        'id': x
      }

      # Tambahan untuk async render
      if not self.asession: self.asession = AsyncHTMLSessionFixed()
      response = await self.asession.post("REDACTED URL" headers=headers, data=data)
      res = json.loads(response.content.decode('utf8'))
      for r in res:
        if 'トーカイ' in r['name']:
          link = url + r['id']
          await self.get_data(link)

  # Pake API, di line bawah
  async def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individuually by calling get_map_data(url)
    '''
    print(url)
    page = await self.asession.get(url)
    await page.html.arender(timeout=0)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find_all('h1')[-1].text
_store['address'] = soup.find('dd', {'class': 'detail'}).find('dd').text.replace('\u3000', '')

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup.find('span', {'class': 'phoneNum'}).text.replace('-', '')

    _store['open_hours'] = soup.find('span', {'class': 'openTime1'}).text.replace('\t', '').replace('\r', '').replace(
      '\n', '').replace('\u3000', '')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    self.content.append(_store)

    print(len(self.content))

  def get_map_data(self, url):
    headers2 = {
      'referer': "REDACTED URL"
      'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
    page = self.session.get(url, headers=headers2, allow_redirects=True)
    return page

if __name__ == '__main__':
  HandcraftTokai()