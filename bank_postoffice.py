import random
from fake_useragent import UserAgent
import datetime, time, csv
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from urllib3.exceptions import ProtocolError
from requests.exceptions import SSLError, ProxyError

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class ServiceBankPostoffice(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.file_name = "ichsan_service_bank_postoffice"
    self.session = HTMLSession()
    self.content = list()
    self.visited_stores = list()
    self.PROXIES = {}

    start = time.time()
    # koordinat_5km.csv
    # points_5_10km_terbaru.csv
    with open('D:/rokesuma-db/GIS/koordinat_20km.csv', 'r', encoding='utf8') as f:
      reader = csv.DictReader(f)
      for x in reader:
        # if int(x['index']) < 800: continue
        print('--', x['index'])
        self.get_page(x['lat'], x['lon'])
        time.sleep(2)
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
      mask = self.df_clean.columns.str.contains('%')
      if from_main:
        self.df_clean.loc[:, ~mask].to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/all_data/{self.file_name}.csv').upload_from_string(
          self.df_clean.loc[:, ~mask].to_csv(index=False), 'text/csv')
    except:
      # (╯°□°）╯︵ ┻━┻
      raise

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/service_bank_postoffice.csv', index=False)

  def get_page(self, lat, lon):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {
      'Accept': '*/*',
      'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
      'Connection': 'keep-alive',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'Origin': 'https://map.japanpost.jp',
      'Referer': 'https://map.japanpost.jp/',
      'Sec-Fetch-Dest': 'empty',
      'Sec-Fetch-Mode': 'cors',
      'Sec-Fetch-Site': 'same-origin',
      'X-Requested-With': 'XMLHttpRequest',
      'user-agent': UserAgent().random}

    params = {
      'zdccnt': random.randint(1, 20),
      'enc': 'EUC',
    }
    data = 'target=http%3A%2F%2F127.0.0.1%2Fcgi%2Fnkyoten.cgi%3F%26' \
           'key%3D71nQuP95lgvvBwnAPvBvnAyf9hmgdPBVidc5SFXuMmA0fhxzTgrx2zTQrxrzT5ngMzFHlwhbF2lxe8JMlQfvDmoQwT6foQHv6roRkL0DngozF0lw0bFmlhq8JmlQIvDToQ2T6eoQcv7zoRLLTd%26' \
           'cid%3Dsearch%26opt%3Dsearch%26pos%3D1%26cnt%3D1000%26enc%3DEUC%26' \
           f'lat%3D{lat}%26lon%3D{lon}%26' \
           'jkn%3D(COL_17%3At+AND+(COL_19!%3A%40%40NULL%40%40+OR+COL_27!%3A%40%40NULL%40%40+OR+COL_36!%3A%40%40NULL%40%40))%26' \
           'rad%3D50000%26knsu%3D1000%26exkid%3D%26hour%3D1%26cust%3D%26exarea%3D%26polycol%3D%26encodeflg%3D0%26PARENT_HTTP_HOST%3Dmap.japanpost.jp'
    try:
      store_lists = self.session.post('https://map.japanpost.jp/p/search/zdcemaphttp.cgi', params=params,
                             headers=headers, data=data)  # , proxies=self.PROXIES, timeout=3
    except (ConnectionError, TimeoutError, ProtocolError, SSLError, ProxyError):
      # rotate_VPN()
      time.sleep(5)
      store_lists = self.session.post('https://map.japanpost.jp/p/search/zdcemaphttp.cgi', params=params,
                             headers=headers, data=data)  # , proxies=self.PROXIES, timeout=3

    store_lists = re.search("ZdcEmapHttpResult\[\d*] = '(.*)';", store_lists.content.decode('euc-jp')).group(1)
    store_lists = store_lists.split('\\n')[1:]

    for store in store_lists:
      store = store.split('\\t')
      # UNTUK MELIHAT OUTPUT DARI API,
      # SILAHKAN PRINT store
      if store[0] in self.visited_stores: continue
      self.visited_stores.append(store[0])
      _store = dict()

      _store['url_store'] = f'https://map.japanpost.jp/p/search/dtl/{store[0]}/?&cond200=1'

      try: _store['store_name'] = store[6]
      except IndexError: continue

      _store['address'] = store[13]

      _store['lat'] = ''

      _store['lon'] = ''

      _store['tel_no'] = ''

      open_hours_dict = {'郵便窓口': {  # mail window
                            '平日': store[25] + '~' + store[26],        # weekday
                            '土曜日': store[27] + '~' + store[28],      # weekend
                            '日曜日・休日': store[29] + '~' + store[30],  # national holiday
                            },
                          '貯金窓口': { # teller window
                            '平日': store[31] + '~' + store[32],
                            '土曜日': store[33] + '~' + store[34],
                            '日曜日・休日': store[35] + '~' + store[36],
                            },
                          'ATM': {    # atm
                            '平日': store[37] + '~' + store[38],
                            '土曜日': store[39] + '~' + store[40],
                            '日曜日・休日': store[41] + '~' + store[42],
                            },
                          '保険窓口': { # insurance window
                            '平日': store[43] + '~' + store[44],
                            '土曜日': store[45] + '~' + store[46],
                            '日曜日・休日': store[47] + '~' + store[48],
                            },
                          'ゆうゆう窓口': { # teller / yuyu window
                            '平日': store[49] + '~' + store[50],
                            '土曜日': store[51] + '~' + store[52],
                            '日曜日・休日': store[53] + '~' + store[54],
                            }
                        }
      open_hours = [f'[{key}] ' + ' '.join([key_day +' '+ ':'.join(value_day.replace(':00~', '~').split(':')[:-1])
                                      for key_day, value_day in value.items() if value_day != '~'])
                   for key, value in open_hours_dict.items()
                   ]
      # output open_hours yang diinginkan:
      # [郵便窓口] 平日 09:00~17:00 [貯金窓口] 平日 09:00~16:00 [ATM] 平日 08:45~18:00 土曜日 09:00~17:00 日曜日・休日 09:00~17:00 [保険窓口] 平日 09:00~16:00

      _store['open_hours'] = ' '.join([x for x in open_hours if not x.endswith(' ')])

      _store['定休日'] = ''  # Regular holiday

      _store['駐車場'] = '有 (' + store[14] + '台)' if store[14] != 0 and store[14] != '' else '無' # Parking lot Yes ( 有 ) , No ( 無 )

      _store['禁煙・喫煙'] = ''  # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )

      _store['取扱'] = ', '.join(re.findall('\[([^\]]+)\]', _store['open_hours']))  # Handling

      _store['備考'] = re.sub('<[^>]+>', '', store[15])  # Remarks

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)

    print(len(self.content))
    self.save_data()


if __name__ == '__main__':
  ServiceBankPostoffice(True)
