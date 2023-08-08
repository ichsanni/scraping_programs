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


class EsthesalonSbc(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_esthesalon_sbc.py'.replace('/', '_').replace('.py', '')
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

        store_lists = soup.select('div.mapBox a')
        for store in store_lists:
            if "REDACTED URL"not in store['href']: continue
            link = f"REDACTED URL"
            self.content.append(self.get_data(link))
            print(len(self.content))


    def get_data(self, url):
        print(url)
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['store_name'] = soup.find('h1').texttry: 
_store['address'] = soup.find('th', text='所在地').find_next_sibling('td').text
        except AttributeError:
            try: 
_store['address'] = soup.find('p', 'address').text
            except AttributeError: 
_store['address'] = soup.find('p', '-adress').text
        _store['url_store'] = url
        _store['url_tenant'] = ''
        try: _store['営業時間'] = soup.find('th', text='診療時間').find_next_sibling('td').text
        except AttributeError:
            try: _store['営業時間'] = soup.find('p', 'footerTime').text
            except AttributeError: _store['営業時間'] = soup.find('div', 'time').find('p', '-text').text
        try: maps_link = soup.find('div', 'toko_map').find('iframe')['src']
        except AttributeError:
            try: maps_link = soup.find('div', 'googlemap').find('iframe')['src']
            except AttributeError: maps_link = ''
        coords = self.geo_loc_embed(maps_link)
        _store['lat'] = coords[0]
        _store['lon'] = coords[1]
        try: _store['tel_no'] = soup.find('a', 'toko_clitel').text
        except AttributeError:
            try: _store['tel_no'] = soup.find('p', 'footerTel').text
            except AttributeError: _store['tel_no'] = soup.find('p', '-telnum').text
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
        if 'maps/embed?' in url:
            reqmap = self.session.get(url, allow_redirects=True)
            raw = re.search(r'initEmbed\((.*\])', reqmap.text)
            r = json.loads(raw[1])
            try:
                latlon = r[21][3][0][2]
                lat, lon = latlon
            except (IndexError, TypeError):
                lat, lon = '',''
            return lat, lon
        else: return ['', '']

if __name__ == '__main__':
    EsthesalonSbc(True)


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