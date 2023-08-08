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

class ZakkaAtliving(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/zakka/atliving'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.start_time = time.time()
        self.getData()

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
                bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

    def getData(self):
        rand_agent = {'user-agent': UserAgent().random.strip()}
        word = '〒'
        url = "REDACTED URL"
        req =self.session.get(url, headers=rand_agent)
        soup = BeautifulSoup(req.html.html, 'html.parser')
        cards = soup.findAll('li', 'box-shop l')
        for card in cards:
            data_dict = {}
            storelink = "REDACTED URL"+ card.find('a')['href']
            r = self.session.get(storelink, headers=rand_agent)
            soupi = BeautifulSoup(r.html.html, 'html.parser')
            maps = soupi.findAll('iframe')[1]['src']
            location = re.findall('\\d{2,3}\\.\\d{3,}', maps)
            nutelps = soupi.find('dl', 'list-detail').findAll('dd')[-1].text.strip().replace('\u3000', '').replace('\t', '').replace('\n', '').replace('\r', '')
            if word in nutelps:
                nutelp = ''
            else:
                nutelp = soupi.find('dl', 'list-detail').findAll('dd')[-1].text.strip().replace('\u3000', '').replace('\t', '').replace('\n', '').replace('\r', '')
            data_dict['store_name'] = card.find('p', 'name').text.replace('\u3000', '').replace('\t', '').replace('\n', '').replace('\r', '')
            data_dict['-'] = 'A.T.リビング'
            data_dict['-'] = 'Ss'
            data_dict['-'] = '/zakka/atliving'
            data_dict['-'] = 'A.T. Living'
            data_dict['-'] = 'zakka'
            data_dict['-'] = 'ショッピング'
            data_dict['-'] = '雑貨/コスメ'
            data_dict['address'] = soupi.find('dl', 'list-detail').findAll('dd')[1].text.strip().split('階')[0].replace('\n', '').replace('                    ', '').replace('\u3000', '').replace('\t', '').replace('\r', '') + ' 階'
            data_dict['URL_store'] = storelink
            data_dict['URL_Tenant'] = ''
            data_dict['Open_Hours'] = soupi.find('dl', 'list-detail').findAll('dd')[0].text.strip().replace('\u3000', '').replace('\t', '').replace('\n', '').replace('\r', '')
            maps_link = "REDACTED URL"+ data_dict['address']

            data_dict['lat'], data_dict['lon'] = '', ''
            data_dict['Tel_No'] = nutelp
            data_dict['GLA'] = 'null'
            data_dict['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(data_dict)
            print(data_dict)
            # review_df = pd.DataFrame(self.master_list)
            # review_df.to_csv('D:/dags/hasil/_zakka_atliving.csv', index=False)
            print(len(self.content))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            # df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    ZakkaAtliving(True)

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
