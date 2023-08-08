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


class HighbrandChanel(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_highbrand_chanel.py'.replace('/', '_').replace('.py', '')
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
        # HOW: kasih boundary JP,
        # list koordinat dibawah jadi titik tengah untuk manggil API, karena radiusnya terbatas
        coord_lists = [[43.84299486160122, 144.123182708621], [42.86798565674312, 143.322450082383],
                       [43.122009100142535, 141.38332649354174], [40.4906379970427, 141.47980021952853],
                       [38.87218343982683, 140.66942014130382], [37.23906531441151, 139.42490790042146],
                       [35.67956955900607, 139.86868742821596], [35.992412588711886, 137.8620321721016],
                       [35.08177923933083, 136.66575678503833], [33.80889092408848, 135.42124463100586],
                       [34.92373121570329, 133.84872152164704], [33.78483955424952, 132.81645152111577],
                       [32.646845856557796, 131.04133333409698], [26.70848140547715, 128.2628873373702],
                       ]
        visited_id = list()
        headers = {
            'authority': 's.chanel.com',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,id;q=0.8',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': "REDACTED URL"
            'referer': "REDACTED URL"
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': UserAgent().random,
            'x-requested-with': 'XMLHttpRequest',
        }

        for coord in coord_lists:
            time.sleep(1)
            data = [
                ('division[]', '1'),        # divisi 1: fashion: pret-a-porter, leather goods, shoes, accessory, 
                ('productline[]', '1'),
                ('productline[]', '2'),
                ('productline[]', '3'),
                ('productline[]', '4'),
                ('productline[]', '26'),
                ('division[]', '2'),        # divisi 2: eyewear: sunglass, optical
                ('productline[]', '5'),
                ('productline[]', '6'),
                ('division[]', '5'),      # divisi 3: watch & fine jewelry: watch, fine jewelry, bridal collection
                ('productline[]', '18'),
                ('productline[]', '19'),
                ('productline[]', '25'),
                ('division[]', '3'),        # divisi 4: fragrance & beauty: fragrance, makeup, skincare, le exclusive
                ('productline[]', '10'),
                ('productline[]', '14'),
                ('productline[]', '13'),
                ('productline[]', '12'),
                ('chanel-only', '1'),     # as it says
                ('geocodeResults',
                 f'[{{"geometry":{{"bounds":{{"south":20.0,"west":120.0,"north":55.0,"east":155.0}},"location":{{"lat":{coord[0]},"lng":{coord[1]}}},"location_type":"APPROXIMATE","viewport":{{"south":20.0,"west":120.0,"north":55.0,"east":155.0}}}}}}]'),
                ('radius', '150'),
            ]

            store_lists = self.session.post(
                "REDACTED URL"
                headers=headers,
                data=data,
            ).json()

            for store in store_lists['stores']:
                if store['id'] in visited_id: continue
                visited_id.append(store['id'])
                _store = dict()
                details = store['translations'][0]
                _store['store_name'] = details['name']
_store['address'] = '〒' + store['zipcode'] + details['address1'] + details['address2'] + details['mallhotel'] + details['floor']
                _store['url_store'] = "REDACTED URL"
                _store['url_tenant'] = ''
                _store['営業時間'] = '' # Open hours / Business Hours
                _store['lat'] = store['latitude']
                _store['lon'] = store['longitude']
                _store['tel_no'] = store['phone']
                _store['gla'] = ''
                _store['定休日'] = '' # Regular holiday
                _store['駐車場'] = '' # Parking lot Yes ( 有 ) , No ( 無 )
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
    HighbrandChanel(True)


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