from requests_html import HTMLSession, AsyncHTMLSession
from bs4 import BeautifulSoup
import time
from datetime import date
from fake_useragent import UserAgent
import time
from bs4 import BeautifulSoup
# IMPORT PENTING
import pandas as pd
import re, json
from scraping.scraping import CleaningData
from google.cloud import storage
import pyppeteer, asyncio

# Kelas baru untuk override AsyncHTMLSession, 
# terutama di method browser() agar tidak bentrok dengan
# async milik Flask
class AsyncHTMLSessionFixed(AsyncHTMLSession):
    """
    pip3 install websockets==6.0 --force-reinstall
    """
    def __init__(self, **kwargs):
        super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
        self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])

    @property
    async def browser(self):
        if not hasattr(self, "_browser"):
            self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not(self.verify), headless=True, handleSIGINT=False, handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

        return self._browser

class CafeHardrock(CleaningData):
    def __init__(self):
        # Taruh semua variabel yang ada di luar class/methods disini
        self.data = {
            'rs_brand_value[]': '276',
            'action': 'rs_ajax_get_posts',
        }
        self.session = HTMLSession()
        self.rand_agent = {'user-agent':UserAgent().random.strip()}
        self.store_dict = list()
        
        start_time = time.time()
        # Tambahan untuk async render
        loop = asyncio.new_event_loop()
        
        loop.run_until_complete(self.getdata())
        # =====

        end_time = time.time()
        print(f'time: {(end_time-start_time)/60}')

        # ===== MULAI KONEKSI KE BIGQUERY
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        x = pd.DataFrame(self.store_dict)

        # CLEANING 1: PERIKSA KOORDINAT
        x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
        x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
        x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
        x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

        x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
        x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
        x['url_tenant'] = None

        # CLEANING 2: TEXT
        try:
            self.df_clean = self.clean_data(x)
                
            # ======== UPLOAD KE BUCKET >>>>>>>>>
            bucket.blob('/ichsan/_cafe_hardrock.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
        except:  
            # ======== UPLOAD KE BUCKET >>>>>>>>>
            bucket.blob('/ichsan/_cafe_hardrock.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
            raise

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))
    
    # async def karena dalem fungsi harus await proses async
    async def getdata(self):
        url = "REDACTED URL"
        print(url)
        session = AsyncHTMLSessionFixed()
        request = await session.post(url, headers=self.rand_agent, data=self.data)
        await request.html.arender(timeout=0)

        #deklarasi dictionary dan kolom 
        idx = 0

        #deklarasi variabel tetap
        - = 'ハードロックカフェ'
        - = 'Ss'
        - = '/cafe/hardrock'
        - = "Hard Rock Cafe"
        - = 'cafe'
        bhs_jepang1 = 'フード'
        bhs_jepang2 = 'カフェ'
        URL_Tenant = ''
        gla = 'null'
        scrape_date = date.today().strftime('%m/%d/%Y')
        
        cards = request.json()
        for i in cards:
            contents = cards[i]
            try:
                store_name = contents['title']
                URL_store = contents['permalink']
                address = contents['address']
                telf = contents['phone']
                open_time = " ".join(contents['business_hours'].replace('<br />','').split())
                request_store = await session.get(URL_store)
                await request_store.html.arender(timeout=0)

                soup_store = BeautifulSoup(request_store.html.html, 'html.parser')

                maps_link = "REDACTED URL"+ address
                maps = self.session.get(maps_link, allow_redirects=True)
                coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
                # print(coords)
                if coords:
                    lat = coords[0].split(',')[0].replace('@', '')
                    lon = coords[0].split(',')[1]
                else:
                    lat = ''
                    lon = ''
                print(f'{store_name} == {address} == {telf} == {open_time}')            
            except TypeError:
                continue
            
            self.store_dict.append({'store_name':store_name,
            '-':-,
            '-':-,
            '-':-,
            '-':-,
            '-':-,
            '-':bhs_jepang1,
            '-':bhs_jepang2,
            'address':address,
            'url_store':URL_store,
            'url_tenant':URL_Tenant,
            'open_hours':open_time,
            'lat':lat,
            'lon':lon,
            'tel_no':telf,
            'gla':gla,
            'scrape_date':scrape_date})
            print(idx)
            
            idx+=1
            time.sleep(2)

# PRINT HTML TO FILE
# with open('_super_/res.html', 'w', encoding='utf8') as f:
#     f.write(page.html.html)