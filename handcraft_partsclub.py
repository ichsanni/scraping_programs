from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class HandcraftPartsclub(CleaningData):
  def __init__(self, from_main=False):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.file_name = '_handcraft_partsclub'
    self.content = list()
    self.visited = list()

    start = time.time()
    url = "REDACTED URL"
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    x = pd.DataFrame(self.content)
    if len(x) == 0:
      raise ValueError('Empty df')

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
    if True:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')


  def __str__(self) -> str:
      return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))
        
  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')


    areas = soup.find_all('area')
    for area in areas:
      if area['href'] in self.visited: continue
      self.visited.append(area['href'])
      area_link = "REDACTED URL"+ area['href']
      print(area_link)
      area_page = self.session.get(area_link, headers=headers)
      area_soup = BeautifulSoup(area_page.html.html, 'html.parser')

      store_lists = [x.parent.find_next_sibling('div', 'temp_wrap_') for x in area_soup.find_all('div', 'temp_h3')]
      for store in store_lists:
        print(area_link + "#" + store.find_previous_sibling('div').find('h3')["id"])
        _store = dict()

        _store['url_store'] = area_link + "#" + store.find_previous_sibling('div').find('h3')["id"]

        _store['store_name'] = store.find_previous_sibling('div').find('h3').text.strip().replace('\r', '').replace('\n', '')try: postcode = store.find('p', text=re.compile('〒')).text.replace('\r', '').replace('\n', '').replace('住所：', '').strip()
        except AttributeError: postcode = ''
        try: address = store.find('p', text=re.compile('県|市|町|都|部')).text.replace('\u3000', '').replace('\r', '').replace('\n', '').strip()
        except AttributeError: address = ''
        if postcode + address == '': continue
        
_store['address'] = postcode + address

        # maps = self.get_map_data("REDACTED URL"+ 
_store['address'])
        # # print(lat)
        # # print(lon)
        # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
        # ## print(coords)
        # if coords:
        #   _store['lat'] = coords[0].split(',')[0].replace('@', '')
        #   _store['lon'] = coords[0].split(',')[1]
        # else:
        #   try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   except:
        #     time.sleep(5)
        #     maps_link = "REDACTED URL"+ 
_store['address']
        #     maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        #     location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   try:
        #       lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
        #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
        #   except:
        #       lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
        _store['lat'], _store['lon'] = '', ''

        try: _store['tel_no'] = store.find('p', text=re.compile('tel')).text.replace('tel：', '').replace('\r', '').replace('\n', '').strip()
        except AttributeError: _store['tel_no'] = ''

        try: _store['open_hours'] = store.find('p', text=re.compile('営業時間')).text.replace('営業時間：', '').replace('\r', '').replace('\n', '').strip()
        except AttributeError: _store['open_hours'] = ''

        _store['gla'] = ''

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        # print(_store)
        self.content.append(_store)

        print(len(self.content))

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page

if __name__ == '__main__':
    HandcraftPartsclub(True)