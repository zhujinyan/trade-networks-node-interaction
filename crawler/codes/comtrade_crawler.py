import pandas as pd
import time
import datetime
import os
import random
import requests, urllib
import json

class comtrade_crawler():
    def __init__(self):
        self.working_directory = "/Users/lizhi/Dropbox/CUHK_RA/shipping/data/comtrade" #if os.name != 'nt' else "C:/Users/lizhi/Dropbox/CUHK_RA/shipping/data/comtrade" #else "C:/Users/Guest_dse/Dropbox/CUHK_RA/shipping"
        self.country_list = pd.read_csv(self.working_directory+"/comtrade_country_list.csv").set_index(['partnercode'])
        self.TradeFlow = {'All': 0, 'Import': 1, 'Export': 2, 're-Export': 3, 're-Import': 4}
        self.API_SERVER_ADDRESS = 'http://uu-proxy.com/api/'
        self.TOKEN_ID = '3JU83RRQYV'
        self.proxy_size = 4
        self.TradeFlow = {'All': 0, 'Import': 1, 'Export': 2, 're-Export': 3, 're-Import': 4}
        self.flow = self.TradeFlow['Import']
        self.focal_country = 'USA'
        self.focal_countrycode = '842'
        self.stopyear = 2022
        self.stopmonth = 10
        self.user_list = [
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
                    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
                    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
                    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
                    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
                    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
                    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
                    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
                    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
                    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
                    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
                    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
                    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
                    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
                    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52"
                    ]
        self.comtrade_cols = ['Classification', 'Year', 'Period', 'Period Desc.', 'Aggregate Level',
                    'Is Leaf Code', 'Trade Flow Code', 'Trade Flow', 'Reporter Code',
                    'Reporter', 'Reporter ISO', 'Partner Code', 'Partner', 'Partner ISO',
                    '2nd Partner Code', '2nd Partner', '2nd Partner ISO',
                    'Customs Proc. Code', 'Customs', 'Mode of Transport Code',
                    'Mode of Transport', 'Commodity Code', 'Commodity', 'Qty Unit Code',
                    'Qty Unit', 'Qty', 'Alt Qty Unit Code', 'Alt Qty Unit', 'Alt Qty',
                    'Netweight (kg)', 'Gross weight (kg)', 'Trade Value (US$)',
                    'CIF Trade Value (US$)', 'FOB Trade Value (US$)', 'Flag'
                    ]
    ## get url from country and period
    def urlfcountrytime(self, country, period):
        return 
    ## get filename from country and period
    def filenamefcountrytime(self, country, period):
        return 
    
    ## get period from year, month
    def periodfYearMonth(self, year, month):
        period = str(year) + '0' + str(month) if month < 10 else str(year) + str(month)
        return period
    
    ## ym --> year, month
    def YearMonthfYM(self, ym):
        year = int((ym-1)/12)+2000
        month = ym%12 if ym%12 !=0 else 12
        return year, month

    ## year, month --> ym, 2000.01 as base
    def YMfYearMonth(self, year, month):
        ym = 12*(year-2000)+month
        return ym

    ## download one query
    def retrieve_data(self, partnercountrycode, year, month, proxy):
        try:
            period = self.periodfYearMonth(year, month)
            url = f"https://comtrade.un.org/api/get?max=100000&type=C&freq=M&px=HS&ps={period}&r={self.focal_countrycode}&p={partnercountrycode}&rg={self.flow}&cc=AG6&fmt=csv"
            header = {'User-Agent': random.choice(self.user_list)}
            filename = self.country_list.partner.loc[partnercountrycode] + "_" +  str(partnercountrycode) + "_" + period + ".csv"
            print(f"Retriving {filename} -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            if not os.path.exists(self.working_directory + "/comtradeusimport/" + filename):
                content = requests.get(url, timeout=16, headers=header, proxies=proxy)
                ''' note that sometimes we only get error informations in the responses, and here are some really dumb quick fixes'''
                if self.check_html_text(content.text):
                    self.retrieve_data(partnercountrycode, year, month, self.get_proxy(self.API_SERVER_ADDRESS, self.TOKEN_ID)[0])
                else:
                    # write csv file;
                    with open(self.working_directory + "/comtradeusimport/" + filename, 'wb') as outfile:
                        outfile.write(content.content)
                    print(f"{filename} download finished -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        except requests.RequestException as e:
            print(type(e).__name__ + " has occurred")
            with open(self.working_directory + "/comtradeusimport/" + "exp.csv", 'a', encoding="utf-8") as log:
                log.write(str(datetime.datetime.now()) + ","+ str(url) + "," + str(filename) + "\n")
            print("\n" + content.content.decode())
            self.retrieve_data(partnercountrycode, year, month, self.get_proxy(self.API_SERVER_ADDRESS, self.TOKEN_ID)[0])
            return
    
    def retrieve_data_country(self, partnercountrycode, startperiod, endperiod):
        startyear = (startperiod[:4])
        endyear = (endperiod[:4])
        monthstart = (startperiod[4:])
        monthend = (endperiod[4:])
        for ym in range(self.YMfYearMonth(startyear, startmonth), min(self.YMfYearMonth(endyear, endmonth), self.YMfYearMonth(self.stopyear, self.stopmonth))):
            proxy = self.get_proxy(self.API_SERVER_ADDRESS, self.TOKEN_ID)[0]
            retrieve_data(self, partnercountrycode, year, month, proxy)


    def retrieve_all_data(self, startperiod, endperiod):
        startyear = int(startperiod[:4])
        endyear = int(endperiod[:4])
        monthstart = int(startperiod[4:])
        monthend = int(endperiod[4:])
        for i in range(self.country_list.shape[0]):
            exportcountrycode = self.country_list.index.to_list()[i]
            for year in range(startyear, endyear+1):
                for month in range(1,12+1):
                    if year < 2022 or (year == 2022 and month < 10):
                        proxy = self.get_proxy(self.API_SERVER_ADDRESS, self.TOKEN_ID)[0]
                        self.retrieve_data(exportcountrycode, year, month, proxy)

    ## return True if problematic    
    def check_html_text(self, text):
        if 'USAGE LIMIT' in text:
            print(filename + " USAGE LIMIT -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            return True
        if (text == "<html><body><h1>502 Bad Gateway</h1>\nThe server returned an invalid or incomplete response.\n</body></html>\n" or text == "Too Many Requests.\n" or text == "{\"Message\":\"An error has occurred.\"}"):
            with open(self.working_directory + "/comtradeusimport/" + "serverError.csv", 'a', encoding="utf-8") as log:
                log.write(str(datetime.datetime.now()) + ","+ str(url) + "," + str(filename) + "\n")
            print("\n" + content.content.decode())
            return True
        return False
    
    ## return True if problematic
    def check_data(self, partnercountrycode, year, month, flow):
        period = str(year) + '0' + str(month) if month < 10 else str(year) + str(month)
        filename = country_list.partner.iloc[i] + "_" + str(exportcountrycode) + "_" + period + ".csv"
        df = pd.read_csv(self.working_directory + "/comtradeusimport/" + filename)
        if 'R' in df.columns:
            return True
            self.limit.append("RATE LIMIT: " + working_directory + "/comtradeusimport/" + filename)
        if df.Classification.iloc[0] == 'No data matches your query or your query is too complex. Request JSON or XML format for more information.':
            self.limit.append("NO DATA: " + working_directory + "/comtradeusimport/" + filename)
        return False
    
    def check_all_data(self, startperiod, endperiod):
        self.limit = []
        startyear = startperiod[:4]
        endyear = endperiod[:4]
        for i in range(self.country_list.shape[0]):
            exportcountrycode = self.country_list.index.to_list()[i]
            for year in range(startyear, endyear+1):
                for month in range(mstart, mend):
                    if year < self.stopyear or (year == self.stopyear and month < self.stopmonth):
                        self.check_data(exportcountrycode, year, month)
        if len(limit) == 0:
            print("NO RATE LIMIT!!!!")
        else:
            print("STILL RATE LIMIT!!!! RUN AGAIN!!!!")
        pd.DataFrame({'failed_file': limit}).to_csv(self.working_directory + "/failed_file_list.csv", index=False)
        return 
        
    ## retrieve proxy
    def get_proxy(self, API_SERVER_ADDRESS, TOKEN_ID, nproxy=1, SIZE=4, SCHEMES='http', SUPPORT_HTTPS='true', RESTIME_WITHIN_MS=5000, FORMAT='json'):
        # max(SIZE) = 50
        query = {
            'id': TOKEN_ID,
            'size': SIZE,
            'schemes': SCHEMES,
            'support_https': SUPPORT_HTTPS,
            'restime_within_ms': RESTIME_WITHIN_MS,
            'format': FORMAT
        }
        url = API_SERVER_ADDRESS + 'get_proxies?' + urllib.parse.urlencode(query)
        res_data = urllib.request.urlopen(url)
        proxy_list = json.loads(res_data.read().decode('utf-8'))['proxies']
        proxies = random.choices(proxy_list, k=min([nproxy,len(proxy_list)]))
        proxy = [{"http": f"http://{myproxy['ip']}:{myproxy['port']}"} for myproxy in proxies]
        return proxy

    ## append data by year
    def append_by_year_oneyear(self, year, replace = True):
        for year in range(startyear, endyear+1):
            if not os.path.exists(self.working_directory + "/"  + f"comtrade_{year}.csv") or replace:
                df = pd.DataFrame(columns=comtrade_cols)
                df.to_csv(self.working_directory + "/"  + f"comtrade_{year}.csv", index=False)
                for month in range(1, 12+1):  # 1,13
                    if year < self.stopyear or (year == self.stopyear and month < self.stopmonth):
                        period = str(year) + '0' + str(month) if month < 10 else str(year) + str(month)
                        filename = country_list.partner.iloc[i] + "_" + str(exportcountrycode) + "_" + period + ".csv"
                        df = pd.read_csv(self.working_directory + filename)
                        df.to_csv(self.working_directory + "/"  + f"comtrade_{year}.csv",header = False, mode = 'a', index=False)
                print(f"Comtrade data aggreageted in year {year} -- "+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        return 
    
    ## execute append_by_year_oneyear for all years
    def append_by_year_all(self,startyear, endyear):
        for year in range(startyear, endyear+1):
            self.append_by_year_oneyear(year)
            print(f"Comtrade data in {year} appended -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        return 
    ## append all data, endperiod inclusives
    def append_all(self, startperiod, endperiod):
        df = pd.DataFrame(columns=comtrade_cols)
        df.to_csv(self.working_directory + "/"  + f"comtrade_{startperiod}_{endperiod}.csv", index=False)
        startyear = int(startperiod[:4])
        endyear = int(endperiod[:4])
        self.append_by_year_all(startyear, endyear)
        for year in range(startyear, endyear+1):
            df = pd.read_csv(self.working_directory + "/comtradeusimport/" + f"comtrade_{year}.csv")
            df.to_csv(self.working_directory + "/"  + f"comtrade_{startperiod}_{endperiod}.csv",header = False, mode = 'a', index=False)
            print(f"Comtrade data in {year} appended -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        return 

    def check_proxy(proxy):
        # test = requests.get("http://icanhazip.com/", proxies=proxy).text
        while True:
            try:
                test = json.loads(requests.get(
                    "http://httpbin.org/ip", proxies=proxy).text)
                break
            except:
                pass
        print(test, proxy)
        if os.name == 'nt':
            if test['origin'][:8] == proxy['http'][7:15] or test['origin'][:8] == proxy['https'][8:16]:
                return True
            else:
                return False
        else:
            if test['origin'][:8] == proxy['http'][:8] or test['origin'][:8] == proxy['https'][:8]:
                return True
            else:
                return False
