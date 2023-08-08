from fake_useragent import UserAgent
import datetime, time
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re, json
from scraping.scraping import CleaningData
from google.cloud import storage


class KadenEdion(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
    index = 0
    limit = 100
    while True:
      offset = limit * index
      url = f"REDACTED URL"\
            f"datum=wgs84&limit={limit}&offset={offset}&ex-code=only.prior.notgroupby&ignore-i18n=true&exclude-i18n=detail-text&timeStamp={timestamp}"
      item_count = self.get_page(url)
      print(len(self.content))
      index += 1
      if item_count == 0:
        break
      time.sleep(3)
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
      bucket.blob('/ichsan/_kaden_edion.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')
    except:
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_kaden_edion.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')
      raise

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)

    store_lists = json.loads(page.html.html)
    for store in store_lists['items']:
      _store = dict()

      _store['url_store'] = "REDACTED URL"+ str(store['code'])

      _store['store_name'] = store['name']
_store['address'] = "〒" + store['postal_code'][0:3] + '-' + store['postal_code'][3:] + ' ' + \
                          store['address_name'].split('<')[0]

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

      try:
        _store['tel_no'] = store['phone'].replace('-', '')
      except KeyError:
        _store['tel_no'] = ''

      texts = store['details'][0]['texts']
      open_hours = ", ".join(["".join([x['label'], x['value']]) for x in texts if '営業時間' in x['label']])
      rmv_tags = re.sub(r'<[^>]*>', '', open_hours)
      _store['open_hours'] = rmv_tags

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
    return len(store_lists['items'])

