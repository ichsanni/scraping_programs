import csv

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


class ApparelZara(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_apparel_zara.py'.replace('/', '_').replace('.py', '')
        self.from_main = from_main
        self.session = HTMLSession()
        self.visited = list()
        self.content = list()

        start = time.time()
        with open('jp-cities-coordinates/jp_coord.csv', 'r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            for pref in reader:
                print(pref['city'])
                url = f"REDACTED URL"\
                      f"lat={pref['lat']}&lng={pref['lng']}&isGlobalSearch=false&ajax=true"
                self.get_page(url)
                time.sleep(0.5)
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

    def get_page(self, url):
        headers = {'user-agent': UserAgent().random}
        error_count = 0
        while True:
            try:
                store_lists = self.session.get(url, headers=headers).json()
                break
            except Exception as e:
                if error_count == 3: return
                print(e.args)
                self.session.close()
                time.sleep(15)
                self.session = HTMLSession()
                error_count += 1
        for store in store_lists:
            if store['id'] in self.visited: continue
            self.visited.append(store['id'])
            _store = dict()
            if 'name' in store.keys():
                _store['store_name'] = store['name'] if store['name'] != ' ' else store['addressLines'][0].replace(',', '')
            else:
                _store['store_name'] = store['addressLines'][0].replace(',', '')
_store['address'] = store['addressLines'][0] # ada zipcode di APInya, tapi kalo pake zipcode alamatnya jadi kepanjangan
            _store['url_store'] = url
            _store['url_tenant'] = ''
            _store['営業時間'] = '' # Open hours / Business Hours
            _store['lat'] = store['latitude']
            _store['lon'] = store['longitude']
            try: _store['tel_no'] = store['phones'][0]
            except IndexError: _store['tel_no'] = ''
            _store['gla'] = ''
            _store['定休日'] = '' # Regular holiday
            _store['駐車場'] = '' # Parking lot Yes ( 有 ) , No ( 無 )
            _store['禁煙・喫煙'] = '' # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
            _store['取扱'] = '' # Handling
            _store['備考'] = '' # Remarks
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

            # print(_store)
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
    ApparelZara(True)


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