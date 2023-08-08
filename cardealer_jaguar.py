import asyncio

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


class CardealerJaguar(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cardealer/jaguar'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.entity_id = list()
        self.start = time.time()
        self.url = "REDACTED URL"
        loop = asyncio.new_event_loop()
        
        loop.run_until_complete(self.get_page(self.url))
        self.end = time.time()
        
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

    async def get_page(self, url):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call get_data(self.url) and visit individual pages.
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        self.session = AsyncHTMLSessionFixed()
        page = await self.session.get(url, headers=headers)
        await page.html.arender(timeout=0, sleep=15)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        domain = "REDACTED URL"
        deptids = list()
        divided_deptids = list()
        for x in soup.find('ol', 'ResultList').find_all('li'):
            try:
                deptids += [x for x in x.find('div').find('article').get('data-departmentids').split(',')]
            except AttributeError:
                continue
        divider = int(len(deptids) / 40) + 1
        for x in range(divider):
            temp = list()
            count = 0
            while count <= 40:
                try:
                    temp.append(deptids.pop())
                except IndexError:
                    break
                count += 1
            divided_deptids.append(temp)
        main_ = "REDACTED URL"
        for d in divided_deptids:
            headers = {'user-agent': UserAgent().random.strip(), 'accept': 'application/json', 'accept-encoding': 'gzip, deflate, br'}
            url = main_ + '&entityid='.join(d)
            self.session = HTMLSession()
            page = self.session.get(url, headers=headers)
            store_lists = page.json()['response']['entities']
            for x in store_lists:
                _store = dict()
                _store['url_store'] = domain + x['url']
                _store['store_name'] = x['profile']['name']pc_code = x['profile']['address']['postalCode'] if x['profile']['address']['postalCode'] else ''
                region = x['profile']['address']['region'] if x['profile']['address']['region'] else ''
                city = x['profile']['address']['city'] if x['profile']['address']['city'] else ''
                sublocality = x['profile']['address']['sublocality'] if x['profile']['address']['sublocality'] else ''
                line1 = x['profile']['address']['line1'] if x['profile']['address']['line1'] else ''
                line2 = x['profile']['address']['line2'] if x['profile']['address']['line2'] else ''
                line3 = x['profile']['address']['line3'] if x['profile']['address']['line3'] else ''
                
_store['address'] = pc_code + ' ' + region + city + sublocality + line1 + line2 + line3
                try:
                    maps_link = "REDACTED URL"+ 
_store['address']

                    _store['lat'], _store['lon'] = '', ''

                except KeyError:
                    maps_link = "REDACTED URL"+ 
_store['address']

                    _store['lat'], _store['lon'] = '', ''

                try:
                    _store['tel_no'] = x['profile']['mainPhone']['display']
                except KeyError:
                    _store['tel_no'] = ''
                _store['open_hours'] = ''
                _store['gla'] = ''
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                # print(_store)
                self.content.append(_store)
                self.save_data()
                print(len(self.content))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CardealerJaguar(True)

