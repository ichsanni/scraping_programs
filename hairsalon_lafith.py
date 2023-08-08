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

from requests_html import AsyncHTMLSession
import pyppeteer, asyncio
class AsyncHTMLSessionFixed(AsyncHTMLSession):
  def __init__(self, **kwargs):
    super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
    self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])
  @property
  async def browser(self):
    if not hasattr(self, "_browser"):
      self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
                                             handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)
    return self._browser


class HairsalonLafith(CleaningData):
    def __init__(self, from_main=False):
        self.file_name = '_hairsalon_lafith.py'.replace('/', '_').replace('.py', '')
        self.from_main = from_main
        self.session = HTMLSession()
        self.content = list()

        start = time.time()
        url = "REDACTED URL"
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
            'user-agent': UserAgent().random,
            'authority': 'lafith.com',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,id;q=0.8',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': "REDACTED URL"
            'referer': "REDACTED URL"
            'x-requested-with': 'XMLHttpRequest',
        }
        page_num = 1
        while True:
            data = f'action=uael_get_post&page_id=551&widget_id=8f36b86&category=*&skin=classic&page_number={page_num}'\
                   f'&nonce=09a3365113'
            page = self.session.post("REDACTED URL" headers=headers, data=data).json()
            soup = BeautifulSoup(page['data']['html'], 'html.parser')

            store_lists = soup.find_all('div', 'store')
            for store in store_lists:
                store_title = store.find('h2').text
                if 'La fith' not in store_title and 'over hair' not in store_title: continue

                link = store.find('a')['href']
                self.content.append(self.get_data(link))
                print(len(self.content))

            page_num += 1
            if len(store_lists) == 0: break


    def get_data(self, url):
        print(url)
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        time.sleep(1)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['store_name'] = soup.find('h1').text.replace('La fith hair', '').split('【')[0] # sesuai rokesuma, nama tempat saja dan tidak ada kurung kotaktry:
            address = soup.find('h3', text='アクセス').parent.parent.find_next_sibling('div').find('p').text
        except AttributeError:
            address = soup.find('h3', text='アクセス').parent.parent.find_next_sibling('div').text
        
_store['address'] = address.split('\n')[0]
        _store['url_store'] = url
        _store['url_tenant'] = ''
        _store['営業時間'] = soup.find('div','uael-days').text # Open hours / Business Hours
        maps_link = [x['src'] for x in soup.find_all('iframe') if 'maps' in x['src']][0]
        coords = self.geo_loc_embed(maps_link.replace('1sen', '1sjp'))
        _store['lat'] = coords[0]
        _store['lon'] = coords[1]
        _store['tel_no'] = soup.find('h3', text='電話番号').parent.parent.find_next_sibling('div').find('a').text
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
            raw = re.search(r'initEmbed\((.*\])', reqmap.html.html)
            r = json.loads(raw[1])
            try:
                latlon = r[21][3][0][2]
                lat, lon = latlon
            except (IndexError, TypeError):
                lat, lon = '', ''
            return lat, lon

if __name__ == '__main__':
    HairsalonLafith(True)


# PINDAHKAN INI KE ATAS JIKA MENGGUNAKAN RENDER
#
# TAMBAHKAN LINE INI UNTUK DEF YANG MENGGUNAKAN RENDER
