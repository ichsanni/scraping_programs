from fake_useragent import UserAgent
import datetime, time
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re, json
from scraping.scraping import CleaningData
from google.cloud import storage


class DenkiyaEdion(CleaningData):
  def __init__(self, from_main):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    from_main = from_main
    self.file_name = '_denkiya_edion'
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

    x = pd.DataFrame(self.content)

    # CLEANING 1: PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
    x['url_tenant'] = None

    self.df_clean = self.clean_data(x)
    if True:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

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

if __name__=='__main__':
  DenkiyaEdion(True)