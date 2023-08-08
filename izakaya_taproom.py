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


class IzakayaTaproom(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_izakaya_taproom.py'.replace('/', '_').replace('.py', '')
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

        visited = list()
        store_lists = soup.select('a[title="詳細"]')
        for store in store_lists:
            if store['href'] not in visited and '/taprooms/' in store['href']:
                self.content.append(self.get_data(store['href']))
                print(len(self.content))
                visited.append(store['href']) #   
                time.sleep(3)

    def get_data(self, url):
        print(url)
        while True:
            try:
                headers = {'user-agent': UserAgent().random}
                page = self.session.get(url, headers=headers)
                soup = BeautifulSoup(page.html.html, 'html.parser')
                soup.find('span', 'bt_bb_headline_content').text
                break
            except AttributeError:
                print('diblok cloudflare, bentar ya~')
                time.sleep(5)
        _store = dict()
        _store['store_name'] = soup.find('span', 'bt_bb_headline_content').text.strip()
_store['address'] = soup.find('span', {'data-ico-icon7stroke':''}).text.strip()
        _store['url_store'] = url
        _store['url_tenant'] = ''
        _store['営業時間'] = soup.find('span', {'data-ico-icon7stroke':''}).text.strip() # Open hours / Business Hours
        maps_link = soup.select_one('a[href*="/maps/"]')['href']
        coords = self.gmaps_embed_coords(self.session, maps_link)
        _store['lat'] = coords[0]
        _store['lon'] = coords[1]
        _store['tel_no'] = soup.find('span', {'data-ico-basic':''}).text.strip()
        _store['gla'] = ''
        _store['定休日'] = '' # Regular holiday
        _store['駐車場'] = '' # Parking lot Yes ( 有 ) , No ( 無 )
        _store['禁煙・喫煙'] = '' # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
        _store['取扱'] = '' # Handling
        _store['備考'] = '' # Remarks
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        print(_store)
        return _store

if __name__ == '__main__':
    IzakayaTaproom(True)


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