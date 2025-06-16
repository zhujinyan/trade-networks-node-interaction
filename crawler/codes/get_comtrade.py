import pandas as pd
import time
import datetime
import os
import random
import requests, urllib
import json

proxy_url = "http://proxy.httpdaili.com/apinew.asp?ddbh=2624438449610326757"
# proxy_url = "http://proxy.httpdaili.com/apinew.asp?ddbh=2561981977314326757"
# proxy_url = "http://proxy.httpdaili.com/apinew.asp?ddbh=2516723283590326757"
# proxy_url = "http://www.zdopen.com/ShortProxy/GetIP/?api=202203012331479058&akey=733b4b9470c57f07&timespan=0&type=1"

proxy_url = ''
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


# def get_proxy(proxy_url):
#     '''
#     if proxy_url == '':
#         return ''
#     while True:
#         try:
#             response = requests.get(proxy_url, timeout=16)
#             break
#         except:
#             print("Failed to get proxy... Retrying...")
#             pass
#     proxy_list = response.text.split("\r\n")
#     '''
#     proxy_list = json.load(open("/Users/lizhi/Dropbox/CUHK_RA/shipping/data/proxies.json", 'r'))
#     user = 'helenovang'
#     pwd = "F9JGauUE"
#     proxy = {"http": f"http://{user}:{pwd}@"+random.choice(proxy_list)['proxy'], "https": f"https://{user}:{pwd}@"+random.choice(proxy_list)['proxy']}
#     print(proxy)
#     return proxy
    # if check_proxy(proxy):
    #     return proxy
    # else:
    #     fcount = 0
    #     while fcount < 5:
    #         proxy = {"http": random.choice(
    #             proxy_list), "https": random.choice(proxy_list)}
    #         if check_proxy(proxy):
    #             return proxy
    #         time.sleep(10)
    #     return {'http': ''}
API_SERVER_ADDRESS = 'http://uu-proxy.com/api/'
TOKEN_ID = 'YCCKQGTSKG'
SIZE = 4
SCHEMES = 'http'
SUPPORT_HTTPS = 'true'
RESTIME_WITHIN_MS = 5000
FORMAT = 'json'
def get_proxy(API_SERVER_ADDRESS, TOKEN_ID, SIZE, SCHEMES, SUPPORT_HTTPS, RESTIME_WITHIN_MS, FORMAT):
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
    myproxy = random.choice(proxy_list)
    proxy = {"http": f"http://{myproxy['ip']}:{myproxy['port']}"} #,"https": f"https://{myproxy['ip']}:{myproxy['port']}"}
    return proxy

def download_url(url, filename):
    try:
        # proxy = get_proxy(proxy_url)
        proxy = get_proxy(API_SERVER_ADDRESS, TOKEN_ID, SIZE, SCHEMES, SUPPORT_HTTPS, RESTIME_WITHIN_MS, FORMAT)
        # print("haha")
        content = requests.get(url, timeout=16, headers=header, proxies=proxy)
        # content = requests.get(url, timeout=16, headers=header)
        # print("here")
        ''' note that sometimes we only get error informations in the responses, and here are some really dumb quick fixes'''
        if 'USAGE LIMIT' in content.text:
            print(filename + " USAGE LIMIT "
                  + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            time.sleep(300)
        if (content.text == "<html><body><h1>502 Bad Gateway</h1>\nThe server returned an invalid or incomplete response.\n</body></html>\n" or content.text == "Too Many Requests.\n" or content.text == "{\"Message\":\"An error has occurred.\"}"):
            with open(working_directory + "/data/comtrade/comtradeusimport/" + "serverError.csv", 'a', encoding="utf-8") as log:
                log.write(str(datetime.datetime.now()) + ","
                          + str(url) + "," + str(filename) + "\n")
                print("\n" + content.content.decode())
                download_url(url, filename)
        else:
            # write csv file;
            with open(working_directory + "/data/comtrade/comtradeusimport/" + filename, 'wb') as outfile:
                outfile.write(content.content)
            print(f"{filename} download finished "
                  + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    except requests.RequestException as e:
        print(type(e).__name__ + " has occurred")
        # if not os.path.exists(working_directory + "/data/comtrade/comtradeusimport/" + "exp.log"):
        #     open(working_directory + "/data/comtrade/comtradeusimport/" + "exp.log", 'w', encoding="utf-8").write("hello")
        #     with open(working_directory + "/data/comtrade/comtradeusimport/" + "exp.log", 'w', encoding="utf-8") as log:
        #         log.write(
        #             str(datetime.datetime.now()) + "," + str(type(e).__name__) + "," + str(url) + "," + str(filename) + "\n")
        #         download_url(url, filename)
        # else:
        #     with open(working_directory + "/data/comtrade/comtradeusimport/" + "exp.log", 'a', encoding="utf-8") as log:
        #         log.write(
        #             str(datetime.datetime.now()) + "," + str(type(e).__name__) + "," + str(url) + "," + str(filename) + "\n")
        #         download_url(url, filename)
        download_url(url, filename)

# /Users/lizhi/Dropbox/CUHK_RA/shipping/data/comtrade/comtrade_country_list.csv
working_directory = "/Users/lizhi/Dropbox/CUHK_RA/shipping" if os.name != 'nt' else "C:/Users/Guest_dse/Dropbox/CUHK_RA/shipping"
# uitoken = "ff12b22d8e10b6a38bfbf880bcc7065a" if os.name != 'nt' else "dd89ba2dad0cda7695b10b749916adf1"
# UA
user_list = [
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
header = {'User-Agent': random.choice(user_list)}
# win: dd89ba2dad0cda7695b10b749916adf1
# country list
country_list = pd.read_csv(
    working_directory+"/data/comtrade/comtrade_country_list.csv")
Ncountry = country_list.shape[0]

TradeFlow = {'All': 0, 'Import': 1,
             'Export': 2, 're-Export': 3, 're-Import': 4}
flow = TradeFlow['Import']

start_year = 2007
end_year = 2009
mstart = 1
mend = 12  # mstart + 3
# limit = []
for i in range(Ncountry):
    print(i, country_list.partner.iloc[i])
    exportcountrycode = country_list.partnercode.iloc[i]
    for year in range(start_year, end_year+1):
        for month in range(mstart, mend+1):
            if year < 2022 or (year == 2022 and month < 10):
                period = str(year) + '0' + \
                             str(month) if month < 10 else str(year) + str(month)
                url = f"https://comtrade.un.org/api/get?max=100000&type=C&freq=M&px=HS&ps={period}&r=842&p={exportcountrycode}&rg={flow}&cc=AG6&fmt=csv"
                urlplus = f"https://comtrade.un.org/api/get/plus?max=100000&type=C&freq=M&px=HS&ps={period}&r=842&p={exportcountrycode}&rg={flow}&cc=AG6&fmt=csv"
                #http://comtrade.un.org/api/refs/da/view?max=100000&type=C&freq=M&px=HS&ps=202001&r=842&p=156&rg=1&cc=AG6&fmt=csv
                filename = country_list.partner.iloc[i] + "_" + \
                    str(exportcountrycode) + "_" + period + ".csv"
                print(f"Retriving {filename} "
                      + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                if not os.path.exists(working_directory + "/data/comtrade/comtradeusimport/" + filename):
                    download_url(url, filename)
                else:
                    try:
                        df = pd.read_csv(
                            working_directory + "/data/comtrade/comtradeusimport/" + filename)
                        if 'R' in df.columns:
                            print(
                                f"RATE LIMIT {filename} " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                            download_url(url, filename)
                            # limit.append("RATE LIMIT: " + working_directory + "/data/comtrade/comtradeusimport/" + filename)
                        elif df.Classification.iloc[0] == 'No data matches your query or your query is too complex. Request JSON or XML format for more information.':
                            print(
                                f"NO DATA {filename} " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                            # download_url(url, filename)
                    except:
                        download_url(url, filename)
                
# check for failures due to rate limit
limit = []
for i in range(Ncountry):
    exportcountrycode = country_list.partnercode.iloc[i]
    for year in range(start_year, end_year+1):
        for month in range(mstart, mend):
            if year < 2022 or (year == 2022 and month < 10):
                period = str(year) + '0' + \
                             str(month) if month < 10 else str(year) + str(month)
                filename = country_list.partner.iloc[i] + "_" + \
                    str(exportcountrycode) + "_" + period + ".csv"
                df = pd.read_csv(working_directory
                                 + "/data/comtrade/comtradeusimport/" + filename)
                if 'R' in df.columns:
                    download_url(url, filename)
                    limit.append("RATE LIMIT: " + working_directory
                                 + "/data/comtrade/comtradeusimport/" + filename)
                elif df.Classification.iloc[0] == 'No data matches your query or your query is too complex. Request JSON or XML format for more information.':
                    limit.append("NO DATA: " + working_directory
                                 + "/data/comtrade/comtradeusimport/" + filename)

if len(limit) == 0:
    print("NO RATE LIMIT!!!!")
else:
    print("STILL RATE LIMIT!!!! RUN AGAIN!!!!")
pd.DataFrame({'failed_file': limit}).to_csv(working_directory + "/data/comtrade/failed_file_list.csv", index=False)
time.sleep(10)

comtrade_cols = ['Classification', 'Year', 'Period', 'Period Desc.', 'Aggregate Level',
                 'Is Leaf Code', 'Trade Flow Code', 'Trade Flow', 'Reporter Code',
                 'Reporter', 'Reporter ISO', 'Partner Code', 'Partner', 'Partner ISO',
                 '2nd Partner Code', '2nd Partner', '2nd Partner ISO',
                 'Customs Proc. Code', 'Customs', 'Mode of Transport Code',
                 'Mode of Transport', 'Commodity Code', 'Commodity', 'Qty Unit Code',
                 'Qty Unit', 'Qty', 'Alt Qty Unit Code', 'Alt Qty Unit', 'Alt Qty',
                 'Netweight (kg)', 'Gross weight (kg)', 'Trade Value (US$)',
                 'CIF Trade Value (US$)', 'FOB Trade Value (US$)', 'Flag']

# delete 2022-03
# for i in range(Ncountry):
#     df = pd.DataFrame(columns=comtrade_cols)
#     exportcountrycode = country_list.partnercode.iloc[i]

#     for year in range(start_year, end_year+1):
#         for month in range(1, 13):  # 1,13
#             if year == 2022 and month == 9:
#                 period = str(year) + '0' + \
#                              str(month) if month < 10 else str(
#                                  year) + str(month)
#                 url = f"https://comtrade.un.org/api/get?max=100000&type=C&freq=M&px=HS&ps={period}&r=842&p={exportcountrycode}&rg={flow}&cc=AG6&fmt=csv"
#                 filename = country_list.partner.iloc[i] + "_" + \
#                     str(exportcountrycode) + "_" + period + ".csv"
#                 if os.path.exists(working_directory + "/data/comtrade/comtradeusimport/" + filename):
#                     os.remove(working_directory + "/data/comtrade/comtradeusimport/" + filename)
# check data
for i in range(Ncountry):
    df = pd.DataFrame(columns=comtrade_cols)
    exportcountrycode = country_list.partnercode.iloc[i]

    for year in range(start_year, end_year+1):
        for month in range(1, 13):  # 1,13
            if year < 2022 or (year == 2022 and month < 10):
                period = str(year) + '0' + \
                             str(month) if month < 10 else str(
                                 year) + str(month)
                url = f"https://comtrade.un.org/api/get?max=100000&type=C&freq=M&px=HS&ps={period}&r=842&p={exportcountrycode}&rg={flow}&cc=AG6&fmt=csv"
                filename = country_list.partner.iloc[i] + "_" + \
                    str(exportcountrycode) + "_" + period + ".csv"
                if os.path.exists(working_directory + "/data/comtrade/comtradeusimport/" + filename):
                    try:
                        df_temp = pd.read_csv(
                            working_directory + "/data/comtrade/comtradeusimport/" + filename)
                    except:
                        os.remove(working_directory
                                  + "/data/comtrade/comtradeusimport/" + filename)
                        download_url(url, filename)
                        df_temp = pd.read_csv(
                            working_directory + "/data/comtrade/comtradeusimport/" + filename)
                    df = df.append(df_temp)
                print(
                    f"Appending {filename} " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df.to_csv(working_directory + "/data/comtrade/comtradeusimport/"
              + f"comtrade_{exportcountrycode}_{start_year}_{end_year}.csv", index=False)

# append final dataset
df = pd.DataFrame(columns=comtrade_cols)
for i in range(Ncountry):
    exportcountrycode = country_list.partnercode.iloc[i]
    exportcountryname = country_list.partner.iloc[i]
    df = df.append(pd.read_csv(working_directory + "/data/comtrade/comtradeusimport/"
                   + f"comtrade_{exportcountrycode}_{start_year}_{end_year}.csv"))
    print(f"Appending {exportcountryname} "
          + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
df.to_csv(working_directory + "/data/comtrade/"  + f"comtrade_{start_year}_{end_year}.csv", index=False)
