import datetime, csv, time
from turtle import pd
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import pandas as pd
import re, json
from scraping.scraping import CleaningData

from google.cloud import storage


class CafeSegafredo(CleaningData):
    def __init__(self):
        self.session = HTMLSession()
        self.content = list()

        url = "REDACTED URL"
        start = time.time()
        self.get_page(url)

        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        x = pd.DataFrame(self.content)

        # PERIKSA KOORDINAT
        x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
        x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
        x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
        x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

        x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
        x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
        x['url_tenant'] = None

        end = time.time()
        print("============ ", (end - start) / 60, " minute(s) ============")

        try:
            self.df_clean = self.clean_data(x)
                
            # ======== UPLOAD KE BUCKET >>>>>>>>>
            bucket.blob('/ichsan/_cafe_segafredo.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
        except:  
            # ======== UPLOAD KE BUCKET >>>>>>>>>
            bucket.blob('/ichsan/_cafe_segafredo.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
            raise

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))
        
    def get_page(self, url):
        '''
        Visit the link given from the gsheet,
        see if there's data there.
        If yes, scrape immediately.
        If no, call get_data(url) and visit individual pages.
        '''
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')

        store_lists = soup.find_all('li', {'class':'shop_name'})
        for store in store_lists:
            link = store.find('a')['href']
            self.content.append(self.get_data(link))

            # save_data(self.content)
            print(len(self.content))


    def get_data(self, url):
        '''
        Visit individual page,
        see if you can scrape map latitude and longitude.
        If no, visit map individuually by calling get_map_data(url)
        '''
        print(url)
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details_head = soup.find_all('p', {'class':'shop_title'})
        details_info = soup.find_all('p', {'class':'shop_info'})
        _store = dict()

        _store['url_store'] = url

        _store['store_name'] = details_head[0].text.replace('\u3000' ,'').replace('\xa0' ,'')
        # _store['store_name'] = ''
_store['address'] = details_info[0].text.replace('\n', '') + details_info[1].text.replace('\n', '')

        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = details_head[1].text.replace('\u3000' ,'')

        _store['open_hours'] = details_info[4].text.replace('\n', '').replace('\u3000' ,'').replace('\r' ,'').replace('\t' ,'')

        _store['gla'] = ''

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        # print(_store)
        return _store

# PRINT HTML TO FILE
# with open('_super_/res.html', 'w', encoding='utf8') as f:
#     f.write(page.html.html)