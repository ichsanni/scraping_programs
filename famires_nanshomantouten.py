from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re, json
from scraping.scraping import CleaningData

try:
    from google.cloud import storage
except ModuleNotFoundError:
    pass


class FamiresNanshomantouten(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_famires_nanshomantouten.py'.replace('/', '_').replace('.py', '')
        self.from_main = from_main
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
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')

        script_txt = soup.find('script', text=re.compile('shops_json'))
        shops_json = re.findall('\{.+}]}', script_txt.text, re.DOTALL)[0]\
            .replace('\/', "/").replace('\\"', "\\'").replace('\\n', '').replace('\\r', '')\
            .replace(';','') \
            .encode().decode('unicode-escape').replace('	', '')
            # .replace('\\"', '"')\
        shops_json = re.sub("FISHERMAN.+'S", "FISHERMAN'S", shops_json)
        shops_json = re.sub("('|\")?(rest_api_[^']+)('|\")?", '"\g<2>"', shops_json)
        store_lists = json.loads(shops_json)
        for store in store_lists['rest_api_country_5']:
            if '南翔饅頭' not in store['name_jp']: continue
            page = self.session.get(store['store_url'], headers=headers)
            soup = BeautifulSoup(page.html.html, 'html.parser')

            _store = dict()
            _store['store_name'] = store['name_jp']
_store['address'] = store['post_code'] + store['address1_jp'] + store['address2_jp']
            _store['url_store'] = store['store_url']
            _store['url_tenant'] = ''
            _store['営業時間'] = soup.find('dt', text='営業時間').find_next_sibling('dd').text # Open hours / Business Hours
            _store['lat'] = store['latitude']
            _store['lon'] = store['longitude']
            _store['tel_no'] = store['tel_number']
            _store['gla'] = ''
            _store['定休日'] = soup.find('dt', text='定休日').find_next_sibling('dd').text.strip() # Regular holiday
            _store['駐車場'] = soup.find('dt', text='駐車場').find_next_sibling('dd').text.strip() # Parking lot Yes ( 有 ) , No ( 無 )
            _store['禁煙・喫煙'] = '' # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
            _store['取扱'] = '' # Handling
            _store['備考'] = '' # Remarks
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
    FamiresNanshomantouten(True)


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