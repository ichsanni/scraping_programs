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


class ParkingFlata(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_parking_flata.py'.replace('/', '_').replace('.py', '')
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

        iframe = soup.select_one('iframe[title=gmap]')['src']
        self.get_data(iframe)

    def get_data(self, url):
        print(url)
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        lists = re.sub('.+(_pageData = ".+");.*', '\g<1>', page.text)
        lists = '{"data":' + lists.split('pageData = ')[-1].replace('\\"', '"')[1:-1].replace('\\\\"',
                                                                                              "'").strip() + '}'
        lists = json.loads(lists)
        for loop1 in lists['data'][1][-24]:
            for x in loop1[12]:
                for loop2 in x[-1]:
                    for data in loop2:
                        _store = dict()
                        _store['url_store'] = "REDACTED URL"
                        try: _store['store_name'] = data[-2][1][0]
                        except TypeError: continue
_store['address'] = ''
                        _store['lat'] = data[1][0][0][0]
                        _store['lon'] = data[1][0][0][1]
                        _store['tel_no'] = ''
                        _store['open_hours'] = ''
                        _store['gla'] = ''
                        _store['定休日'] = ''  # Regular holiday
                        _store['駐車場'] = ''.join([x for x in data[5][1][1][0].split('\\n') if '台' in x]).replace('駐車台数', '').replace('【', '').replace('】', '')  # Parking lot Yes ( 有 ) , No ( 無 )
                        _store['禁煙・喫煙'] = ''  # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
                        try: _store['取扱'] = ''.join([x for x in data[5][1][1][0].split('\\n') if '分' in x]).replace(_store['store_name'], '').replace(_store['駐車場'], '')   # Handling
                        except IndexError: _store['取扱'] = ''
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
    ParkingFlata(True)


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