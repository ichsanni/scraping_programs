from fake_useragent import UserAgent
import datetime, time
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try: from google.cloud import storage
except ModuleNotFoundError: pass


class FamiresTonyroma(CleaningData):
  def __init__(self, from_main=False):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
    self.get_page(url, from_main)
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
        self.df_clean.to_csv('D:/dags/csv/_famires_tonyroma.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_famires_tonyroma.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    df = pd.DataFrame(self.content)
    df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                             '-', '-', '-', 'address', 'url_store', 'url_tenant',
                             'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
    df.to_csv('D:/dags/csv/_famires_tonyroma.csv', index=False)

  def get_page(self, url, from_main):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    data = {
      'rs_area_value[]': [
        '48', '47', '45', '43', '42', '36', '74', '35', '41', '40', '39', '38', '37', '34', '33', '32', '26', '25',
        '24', '22', '75',
        '27', '23', '21', '20', '17', '16', '13', '11', '10', '9', '7', '6', '5', '4', '2',
      ],
      'rs_brand_value[]': '274',
      'action': 'rs_ajax_get_posts',
    }

    store_lists = self.session.post("REDACTED URL" headers=headers, data=data).json()
    for x in store_lists.keys():
      _store = dict()

      try:
        _store['url_store'] = store_lists[x]['permalink']
      except TypeError:
        continue

      _store['store_name'] = store_lists[x]['title']
_store['address'] = store_lists[x]['address']

      maps_link = "REDACTED URL"+ 
_store['address']

      _store['lat'], _store['lon'] = '', ''

      _store['tel_no'] = store_lists[x]['phone']

      _store['open_hours'] = re.sub(r'<.*>|\n|\t|\r', '', store_lists[x]['business_hours']).replace('\u3000', ' ')

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)

    if from_main: self.save_data()
    print(len(self.content))


if __name__ == '__main__':
  FamiresTonyroma(True)

