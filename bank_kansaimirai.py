from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class BankKansaimirai(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

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
    except:
      raise
    if True:
      if from_main:
        self.df_clean.to_csv('D:/dags/csv/_bank_kansaimirai.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_bank_kansaimirai.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_bank_kansaimirai.csv', index=False)

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    data = {
      'nelat': '55.0',
      'swlat': '20.0',
      'nelng': '155.0',
      'swlng': '120.0',
      'shopids': '',
    }
    store_lists = self.session.post("REDACTED URL" headers=headers, data=data).json()
    for store in store_lists['features']:
      if '関西みらい' not in store['properties'][1]: continue # 関西 == kansai
      link = f"REDACTED URL"
      self.get_data(link, store['properties'][1], store['properties'][2],
                    store['geometry']['coordinates'][1], store['geometry']['coordinates'][0])
      time.sleep(0.3)

  def get_data(self, url, store_name, address, lat, lon):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = store_name
_store['address'] = address

    # maps_link = "REDACTED URL"+ 
_store['address']
    # maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
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
    _store['lat'], _store['lon'] = '',''

    try: _store['tel_no'] = soup.find('span', text=re.compile('電話番号')).parent.find_next('td').text.strip().replace('\r', '').replace('\n', '').replace('\u3000', ' ')
    except AttributeError: _store['tel_no'] = ''

    try: open_hours = soup.find('span', text=re.compile('窓口営業時間')).parent.find_next('td').text.strip().replace('\r', '').replace('\n', ' ').replace('\u3000', ' ').split('※')[0] # .split('・')[0]
    except AttributeError:
      try: open_hours = soup.find('span', text=re.compile('ATM営業時間')).parent.find_next('td').text.strip().replace('\r', '').replace('\n', ' ').replace('\u3000', ' ').split('※')[0] # .split('・')[0]
      except AttributeError: open_hours = ''
    _store['open_hours'] = open_hours

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    self.content.append(_store)
    print(len(self.content))
    self.save_data()


if __name__ == '__main__':
  BankKansaimirai(True)
