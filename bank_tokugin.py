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


class BankTokugin(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_bank_tokugin.py'.replace('/', '_').replace('.py', '')
        self.from_main = from_main
        self.session = HTMLSession()
        self.content = list()

        start = time.time()
        self.get_page()
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
        finally:
            if from_main:
                self.df_clean.to_csv(f'{self.file_name}.csv', index=False)
            else:
                # ======== UPLOAD KE BUCKET >>>>>>>>>
                client = storage.Client()
                bucket = client.get_bucket('scrapingteam')
                bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
                    self.df_clean.to_csv(index=False), 'text/csv')

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

    def get_page(self):
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
            'Connection': 'keep-alive',
            'Referer': "REDACTED URL"
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': UserAgent().random,
            'X-Requested-With': 'XMLHttpRequest',
        }
        limit = 100
        offset = 0
        total = 0

        while total >= offset:
            params = {
                'category': '01.02',
                'use-group': 'self',
                'limit': limit,
                'address': '',
                'add': 'detail.detail_group',
                'device': 'pc',
                'sort': 'default',
                'random-seed': '611318665',
                'datum': 'wgs84',
                'offset': offset,
                'timeStamp': datetime.datetime.now().strftime('%Y%m%d%H%M'),
            }

            store_lists = self.session.get(
                "REDACTED URL"
                params=params,
                headers=headers,
            ).json()
            total = store_lists['count']['total']
            offset += limit

            for store in store_lists['items']:
                details = store['details'][0]['texts']
                _store = dict()
                _store['store_name'] = store['name']
_store['address'] = '〒' + store['postal_code'][:3] +'-'+ store['postal_code'][3:] + store['address_name']
                _store['url_store'] = "REDACTED URL"+ store['code']
                _store['url_tenant'] = ''
                _store['営業時間'] = ''.join([x['value'] for x in details if x['label'] == '窓口営業時間']).replace('<br>', ' ').replace('\n','') # Open hours / Business Hours
                _store['lat'] = store['coord']['lat']
                _store['lon'] = store['coord']['lon']
                try: _store['tel_no'] = store['phone']
                except KeyError: _store['tel_no'] = ''
                _store['gla'] = ''
                _store['定休日'] = '' # Regular holiday
                _store['駐車場'] = '' # Parking lot Yes ( 有 ) , No ( 無 )
                _store['禁煙・喫煙'] = '' # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
                _store['取扱'] = '' # Handling
                _store['備考'] = '[ATM] ' + ' '.join([x['value'] for x in details if x['label'] == 'キャッシュサービスコーナー営業時間']).replace('<br>', ' ').replace('\n','') \
                    if [x['value'] for x in details if x['label'] == 'キャッシュサービスコーナー営業時間'] else ''  # Remarks
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

                print(_store)
                self.content.append(_store)
            print(len(self.content))

    def geo_loc_embed(self, url):
        import json
        reqmap = self.session.get(url, allow_redirects=True)
        if 'maps/embed?' in url:
            raw = re.search(r'initEmbed\((.*\])', reqmap.text)
            r = json.loads(raw[1])
            try:
                latlon = r[21][3][0][2]
                lat, lon = latlon
            except (IndexError, TypeError):
                lat, lon = '', ''
        else:
            raw = re.search(r'window\.APP_INITIALIZATION_STATE=\[(.+)\];window', reqmap.text)
            try:
                r = json.loads(raw[0].replace('window.APP_INITIALIZATION_STATE=', '').split(';window')[0])
                gmaps_array = [x for x in r[3] if x]
                latlon = json.loads(gmaps_array[1].split('\n')[1])[4][0]
                lat = latlon[2]
                lon = latlon[1]
            except (IndexError, TypeError):
                try:
                    latlon = json.loads(gmaps_array[1].split('\n')[1])[0][1][0][14][9]
                    lat = latlon[2]
                    lon = latlon[3]
                except (IndexError, TypeError):
                    lat, lon = '', ''
        return lat, lon

if __name__ == '__main__':
    BankTokugin(True)


# PINDAHKAN INI KE ATAS JIKA MENGGUNAKAN RENDER
# from requests_html import AsyncHTMLSession
# import pyppeteer, asyncio
# class AsyncHTMLSessionFixed(AsyncHTMLSession):
#   def __init__(self, **kwargs):
#     super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
#     self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])
#   @property
#   async def browser(self):
#     if not hasattr(self, "_browser"):
#       self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
#                                              handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)
#     return self._browser

# TAMBAHKAN LINE INI UNTUK DEF YANG MENGGUNAKAN RENDER
# loop = asyncio.new_event_loop()
# loop.run_until_complete()