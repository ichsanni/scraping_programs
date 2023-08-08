from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
    from google.cloud import storage
except ModuleNotFoundError:
    pass

class DrugZagzag(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/drug/zagzag'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content_drug = list()
        self.content_pharmacy = list()
        self.url = "REDACTED URL"
        self.start = time.time()
        self.get_page(self.url)
        self.end = time.time()
        
        drug = pd.DataFrame(self.content_drug)
        pharmacy = pd.DataFrame(self.content_pharmacy)
        
        drug['lat'] = pd.to_numeric(drug['lat'], errors='coerce')
        drug['lon'] = pd.to_numeric(drug['lon'], errors='coerce')
        drug.loc[drug[(drug['lat'] < 20) | (drug['lat'] > 50)].index, 'lat'] = pd.NA
        drug.loc[drug[(drug['lat'] < 20) | (drug['lat'] > 50)].index, 'lon'] = pd.NA
        drug.loc[drug[(drug['lon'] < 121) | (drug['lon'] > 154)].index, 'lat'] = pd.NA
        drug.loc[drug[(drug['lon'] < 121) | (drug['lon'] > 154)].index, 'lon'] = pd.NA
        drug['url_tenant'] = None
        try:
            self.df_clean_drug = self.clean_data(drug)
        except:
            raise
        if True:
            if from_main:
                self.df_clean_drug.to_csv(f'D:/dags/csv/_drug_zagzag.csv', index=False)
            else:
                client = storage.Client()
                bucket = client.get_bucket('scrapingteam')
                bucket.blob(f'/ichsan/_drug_zagzag.csv').upload_from_string(self.df_clean_drug.to_csv(index=False), 'text/csv')

        pharmacy['lat'] = pd.to_numeric(pharmacy['lat'], errors='coerce')
        pharmacy['lon'] = pd.to_numeric(pharmacy['lon'], errors='coerce')
        pharmacy.loc[pharmacy[(pharmacy['lat'] < 20) | (pharmacy['lat'] > 50)].index, 'lat'] = pd.NA
        pharmacy.loc[pharmacy[(pharmacy['lat'] < 20) | (pharmacy['lat'] > 50)].index, 'lon'] = pd.NA
        pharmacy.loc[pharmacy[(pharmacy['lon'] < 121) | (pharmacy['lon'] > 154)].index, 'lat'] = pd.NA
        pharmacy.loc[pharmacy[(pharmacy['lon'] < 121) | (pharmacy['lon'] > 154)].index, 'lon'] = pd.NA
        pharmacy['url_tenant'] = None
        try:
            self.df_clean = self.clean_data(pharmacy)
        except:
            raise
        if True:
            if from_main:
                self.df_clean.to_csv(f'D:/dags/csv/_pharmacy_zagzag.csv', index=False)
            else:
                client = storage.Client()
                bucket = client.get_bucket('scrapingteam')
                bucket.blob(f'/ichsan/_pharmacy_zagzag.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

    def get_page(self, url):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call self.get_data(self.url) and visit individual pages.
  """
        page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        store_lists = soup.find_all('dl')
        for store in store_lists:
            link = store.find('a')['href']
            self.get_data(link)
            # self.save_data_drug(self.content_drug)
            # self.save_data_pharmacy(self.content_pharmacy)
            print('drug', len(self.content_drug))
            print('pharmacy', len(self.content_pharmacy))
            time.sleep(1)

    def get_data(self, url):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(self.url)
  """
        print(url)
        try:
            page = self.session.get(url)
        except ConnectionError:
            time.sleep(2)
            page = self.session.get(url)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        details = soup.find_all('dl', {'class': 'sh_info'})
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = soup.find('h2').text
_store['address'] = details[2].text.replace('\n', '')

        _store['lat'], _store['lon'] = '', ''
        _store['tel_no'] = details[1].text.replace('-', '').replace('\n', '')
        _store['open_hours'] = details[0].text.replace('\n', '')
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        self.content_drug.append(_store)
        if 'a.png' in soup.find('ul', {'id': 'items'}).find('img')['src']:
            _store2 = dict()
            _store2['url_store'] = url
            _store2['store_name'] = soup.find('h2').text
            _store2['-'] = 'ザグザグ'
            _store2['-'] = 'Drg'
            _store2['-'] = '/pharmacy/zagzag'
            _store2['-'] = 'ZAG ZAG'
            _store2['-'] = 'pharmacy'
            _store2['-'] = 'サービス'
            _store2['-'] = '調剤薬局'
            _store2['address'] = details[2].text.replace('\n', '')

            _store2['lat'] = ''
            _store2['lon'] = ''
            _store2['tel_no'] = details[1].text.replace('-', '').replace('\n', '')
            _store2['open_hours'] = details[0].text.replace('\n', '')
            _store2['gla'] = ''
            _store2['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content_pharmacy.append(_store2)


    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    DrugZagzag(True)

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
      self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
                                             handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

    return self._browser
