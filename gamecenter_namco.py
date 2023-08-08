import datetime, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from fake_useragent import UserAgent
import re, json
import pandas as pd
from scraping.scraping import CleaningData

from google.cloud import storage


class GamecenterNamco(CleaningData):
  def __init__(self, from_main):
    self.session = HTMLSession()
    self.content = list()
    from_main = from_main
    
    start = time.time()
    page_num = 1
    while True:
      url = f"REDACTED URL"
      print(url)
      try: self.get_page(url)
      except StopIteration: break
      page_num += 1
    end = time.time()

    x = pd.DataFrame(self.content)

    # PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
    x['url_tenant'] = None
    
    print("============ ", (end - start) / 60, " minute(s) ============")

    self.df_clean = self.clean_data(x)
    if True:
      if from_main:
        self.df_clean.to_csv('D:/dags/csv/_gamecenter_namco.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_gamecenter_namco.csv').upload_from_string(
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

    store_lists = soup.find('ul', 'matchList').find_all('a')
    for store in store_lists:
      link = "REDACTED URL"+ store['href']
      if "REDACTED URL"in store['href']:
        link = store['href']
      title = store.find('dt').text
      address = store.parent.find_all('dd')[0].text
      open_hours = store.parent.find_all('dd')[-1].text
      data = self.get_data(link, title, address, open_hours)
      if data:
        self.content.append(data)

        print(len(self.content))

    if len(store_lists) == 0: raise StopIteration


  def get_data(self, url, title, address, open_hours):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = titletry: 
_store['address'] = soup.find('li', 'loc_add').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', ' ').strip()
    except AttributeError: 
_store['address'] = address


    # maps_link = "REDACTED URL"+ 
_store['address']
    # maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
    # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
    # # # print(coords)
    # if coords:
    #   _store['lat'] = coords[0].split(',')[0]
    #   _store['lon'] = coords[0].split(',')[1].strip()
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
    _store['lat'], _store['lon'] = '',''

    try: _store['tel_no'] = soup.find('li', 'loc_tel').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', ' ').strip()
    except AttributeError: _store['tel_no'] = ''

    try: info_text = soup.find('li', 'loc_hours').find('p').text
    except: info_text = ''
    try: _store['open_hours'] = soup.find('li', 'loc_hours').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '').replace(' ', '').strip().replace(info_text, '')
    except AttributeError: _store['open_hours'] = open_hours

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

if __name__ == '__main__':
  GamecenterNamco(True)


# PRINT HTML TO FILE
# with open('_super_/res.html', 'w', encoding='utf8') as f:
#     f.write(page.html.html)

# remove newline
# .replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', ' ').replace(' ', '')
