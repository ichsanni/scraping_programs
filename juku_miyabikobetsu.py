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


class JukuMiyabikobetsu(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_juku_miyabikobetsu.py'.replace('/', '_').replace('.py', '')
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

        areas = soup.find('div', {'id':'kouMapSec'}).find_all('a')
        for area in areas:
            if area['href'].endswith('schools/'): continue
            area_link = "REDACTED URL"+ area['href'].replace('../','')
            print(area_link)
            area_page = self.session.get(area_link, headers=headers)
            area_soup = BeautifulSoup(area_page.html.html, 'html.parser')

            try:
                city_a = area_soup.find('div', 'sub_nav').find_all('a')
                for city in city_a:
                    city_link = area_link + city['href'].replace('../','')
                    print(city_link)
                    city_page = self.session.get(city_link, headers=headers)
                    if '404' in city_page.text:
                        remove_redundant = '/'.join(area_link.split('/')[:-2])
                        city_link = remove_redundant +'/'+ city['href'].replace('../','')
                        print(city_link, 'new link')
                        city_page = self.session.get(city_link, headers=headers)
                    city_soup = BeautifulSoup(city_page.html.html, 'html.parser')
                    self.get_miyabi_only(city_soup)
            except AttributeError:
                # ini artinya pagenya langsung nampilin semua school yang ada di area tsb.
                self.get_miyabi_only(area_soup)

    def get_miyabi_only(self, soup:BeautifulSoup):
        areas = soup.select('div.stBox')
        for area in areas:
            store_lists = area.find_all('a')
            for store in store_lists:
                if 'miyabi-kobetsu' not in store['href']: continue
                self.content.append(self.get_data(store['href']))
                print(len(self.content))



    def get_data(self, url):
        print(url)
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['store_name'] = soup.find('h2').text.replace(soup.find('h2').find('span').text, '').strip()
_store['address'] = soup.find('dt', text=re.compile('住\s*所')).find_next_sibling('dd').text
        _store['url_store'] = url
        _store['url_tenant'] = ''
        try: _store['営業時間'] = soup.find('dt', text='開校情報').find_next_sibling('dd').text # Open hours / Business Hours
        except AttributeError: _store['営業時間'] = ''
        maps_link = soup.select_one('iframe[src*="/maps/"]')['src']
        coords = self.geo_loc_embed(maps_link)
        _store['lat'] = coords[0]
        _store['lon'] = coords[1]
        _store['tel_no'] = soup.find('dt', text='電話番号').find_next_sibling('dd').text
        _store['gla'] = ''
        _store['定休日'] = '' # Regular holiday
        _store['駐車場'] = '' # Parking lot Yes ( 有 ) , No ( 無 )
        _store['禁煙・喫煙'] = '' # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
        _store['取扱'] = '' # Handling
        _store['備考'] = '' # Remarks
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        print(_store)
        return _store

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
    JukuMiyabikobetsu(True)


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