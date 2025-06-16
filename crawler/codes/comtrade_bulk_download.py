import requests, time, os, datetime, zipfile, io
import glob, re
import pandas as pd
import sys
import json
import logging
from threading import Thread
from multiprocessing import Process
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s", datefmt="%Y-%m-%d %H:%M:%S", handlers = [logging.FileHandler('../comtrade_crawler.log','a','utf-8'), logging.StreamHandler()])

class manager():
    def distributor(self, tasks, nthread):
        N = len(tasks)
        Ni = int(N/nthread)
        residual = N-Ni*nthread
        index = [0]
        for i in range(residual):
            index.append(Ni*(i+1)+i+1)
        for i in range(nthread - residual):
            index.append(index[-1]+Ni)
        start_index = index[:nthread]
        end_index = index[1:nthread+1]
        if N>nthread:
            task_list = [tasks[start_index[i]: end_index[i]] for i in range(nthread)]
        else:
            task_list = [[task] for task in tasks]
        return task_list
    
    def get_task_multithread(self, func, tasks, process_id = 0, nthread = 4):
        logging.info(f"Starting Thread in Process {process_id}")
        task_list = self.distributor(tasks, nthread)
        p_list=[]
        for i in range(nthread):
            p = Thread(target=func, args=(task_list[i],))
            p.daemon=True
            p_list.append(p)

        for p in p_list:
            p.start()
            time.sleep(1)
        
        for p in p_list:
            p.join()

    def get_task_multiprocess(self, func, tasks, ult_func, nprocess = 4, nthread=4):
        task_list = self.distributor(tasks, nprocess)
        # mpool = Pool(nprocess)
        logging.info("Starting multiprocess")
        # mpool = ProcessPoolExecutor(nprocess)
        # res = mpool.map(func, [(ult_func, task_list[i], i, nthread,) for i in range(nprocess)])
        p_list = []
        for i in range(nprocess):
            p = Process(target = func, args = (ult_func, task_list[i], i, nthread,))
            p_list.append(p)
        for p in p_list:
            p.start()
        for p in p_list:
            p.join()

class comtrade_crawler():
    def __init__(self):
        self.working_directory = "../"
    

    def renew_basic_info(self, info):
        if not os.path.exists(self.working_directory + "basic_info/"):
            os.mkdir(self.working_directory + "basic_info/")
        # for info in ["tradeRegimes", "partnerAreas", "reporterAreas", "classificationHS"]:
        response = requests.get(f"https://comtrade.un.org/Data/cache/{info}.json")
        with open(self.working_directory + f"basic_info/{info}.json", "w") as fff:
            json.dump(response.text, fff)
        return
    
    def get_country_list(self):
        self.renew_basic_info("reporterAreas")
        with open(self.working_directory + f"basic_info/reporterAreas.json", "r") as fff:
            myjson = json.load(fff)
        myjson = json.loads(myjson.replace("\r\n", "").replace("\ufeff",""))
        myjson = myjson['results']
        res = pd.DataFrame(myjson)
        
        res.rename(columns={'id':'reportercode','text':'reporter'}, inplace = True)
        res = res[res['reporter']!='All']
        return res
    
    def get_year_list(self):
        self.renew_basic_info("years")
        with open(self.working_directory + f"basic_info/years.json", "r") as fff:
            myjson = json.load(fff)
        myjson = json.loads(myjson.replace("\r\n", "").replace("\ufeff",""))
        myjson = myjson['results']
        res = pd.DataFrame(myjson)
        res.rename(columns={'text':'year'}, inplace = True)
        res = res[['year']]
        res = res[res['year']!='All']
        res = res.astype({'year':int})
        res = res.sort_values(by=['year'], ascending = False)
        return res['year'].to_list()

    def periodfYearMonth(self, year, month):
        period = str(year) + '0' + str(month) if month < 10 else str(year) + str(month)
        return period

    def check_html_text(self, text, filename):
        if 'USAGE LIMIT' in text:
            logging.info(" USAGE LIMIT ")
            return True
        if (text == "<html><body><h1>502 Bad Gateway</h1>\nThe server returned an invalid or incomplete response.\n</body></html>\n"  or text == "{\"Message\":\"An error has occurred.\"}"):
            with open(working_directory + "serverError.csv", 'a', encoding="utf-8") as log:
                log.write(str(datetime.datetime.now()) + "," + "," + str(filename) + "\n")
            logging.info("Bad Server")
            return True
        if text == "Too Many Requests.\n":
            logging.info("Too Many Requests.")
            return True
        return False

    def retrieve_data(self, country, countrycode, year, month = -1, nodata=[]):
        try:
            period = self.periodfYearMonth(year, month) if month > 0 else year
            
            level = "M" if month > 0 else "A"
            freq = "annual" if level == "A" else "month"
            # url = f"https://comtrade.un.org/api/get/bulk/C/M/{period}/842/HS?token=W0YHqyAzFmNGp02eQf4+BIQVkydVa8NYI8iWZKJj1BmDAX7wKz4yau8v1V4HYODsO2tiao8U4BLgNm9Dd41k6/yrXjv5U11MzfYRQs6aIXHvQWXFIwhucgVSp88vEkIk"
            # url = f"https://comtrade.un.org/api/get/bulk/C/A/{year}/842/HS?token=W0YHqyAzFmNGp02eQf4+BIQVkydVa8NYI8iWZKJj1BmDAX7wKz4yau8v1V4HYODsO2tiao8U4BLgNm9Dd41k6/yrXjv5U11MzfYRQs6aIXHvQWXFIwhucgVSp88vEkIk"
            url = f"https://comtrade.un.org/api/get/bulk/C/{level}/{period}/{countrycode}/HS?token=W0YHqyAzFmNGp02eQf4+BIQVkydVa8NYI8iWZKJj1BmDAX7wKz4yau8v1V4HYODsO2tiao8U4BLgNm9Dd41k6/yrXjv5U11MzfYRQs6aIXHvQWXFIwhucgVSp88vEkIk"
            if not os.path.exists(working_directory + f"data/{country}"):
                os.mkdir(working_directory + f"data/{country}") 
            if not os.path.exists(working_directory + f"data/{country}/{freq}"):
                os.mkdir(working_directory + f"data/{country}/{freq}") 
            filename = f"{country}{period}.zip"
            # print(f"Retriving {filename} -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            if not os.path.exists(working_directory + f"data/{country}/{freq}/" + filename) and not (filename in nodata and ((year == 2022 and month <=9) or year < 2022)) and (year<2023 or month<=4) and not "Fmr" in filename:
                logging.info(f"Retrieving {filename}")
                content = requests.get(url)
                ''' note that sometimes we only get error informations in the responses, and here are some really dumb quick fixes'''
                if self.check_html_text(content.text, filename):
                    time.sleep(60)
                    self.retrieve_data(country, countrycode, year, month)
                    
                elif "Not found" in content.text:
                    pd.DataFrame(columns=['id'], data=[filename]).to_csv(self.working_directory + "nodata.csv", index = False, mode = 'a', header = False)
                    nodata = pd.read_csv(self.working_directory + "nodata.csv")['id'].to_list()
                    logging.info(f"{filename} NO DATA!")
                    time.sleep(10)
                    return
                else:
                    # write csv file;
                    with open(working_directory + f"data/{country}/{freq}/" + filename, 'wb') as outfile:
                        outfile.write(content.content)
                    # z = zipfile.ZipFile(io.BytesIO(content.content))
                    # z.extractall(working_directory)
                    # print(f"{filename} download finished -- " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    logging.info(f"{filename} download finished")
                    time.sleep(10)
                    return 
        except requests.RequestException as e:
            print(type(e).__name__ + " has occurred")
            with open(working_directory +  f"data/{country}/"  + "exp.csv", 'a', encoding="utf-8") as log:
                log.write(str(datetime.datetime.now()) + ","+ str(url) + "," + str(filename) + "\n")
            # print("\n" + content.content.decode())
            self.retrieve_data(country, countrycode, year, month)
            return

    def download(self, tasks, nodata):
        for countrycode, country in tasks:
            for year in years:
                for month in months:
                    # print(country, year, month)
                    self.retrieve_data(country.replace(".",""), countrycode, year, month, nodata=nodata)
                    # time.sleep(5)

if __name__ == "__main__":
    working_directory = "../"
    crawler = comtrade_crawler()
    manager = manager()
    countries = crawler.get_country_list()
    freq = sys.argv[1] # annual or month
    mode = ""
    try:
        mode = sys.argv[2] # download, check, unzip, append
    except:
        pass 
    try:
        country = sys.argv[3] # country 
        countries = countries[countries['reporter'] == country]
    except:
        pass 
    print(f"Starting {freq} {mode}")
    years = crawler.get_year_list() if freq == "annual" else range(2010,2023+1)
    years = range(1990,2022+1) if freq == "annual" else years
    months = [-1] if freq == "annual" else range(1,12+1)
    if not os.path.exists(working_directory + "nodata.csv"):
        pd.DataFrame(columns=['id']).to_csv(working_directory + "nodata.csv")
    nodata = pd.read_csv(working_directory + "nodata.csv")['id'].to_list()

    if mode == "download" or not mode:
        tasks = list(countries.itertuples(index=False, name=None))
        # manager.get_task_multithread(crawler.download, tasks, nthread = 4)
        crawler.download(tasks, nodata)

    if mode == "check" or not mode:
        ## unzip data
        for country in countries['reporter'].to_list():
            zipfilelist = glob.glob(working_directory + f"data/{country}/{freq}/{country}*.zip")
            # print(zipfilelist)
            for file in zipfilelist:
                match = re.search(fr".*/data/{country}/{freq}/(.*).zip",file.replace("\\","/"))
                if match:
                    filename = match.group(1)
                    try:
                        with zipfile.ZipFile(file, 'r') as zip_ref:
                            zip_ref.extractall(working_directory + f"data/{country}/{freq}/")
                    except:
                        logging.warning(f"{filename}.zip bad!!")
                        os.remove(file)
                        pass
            csvfilelist = glob.glob(working_directory + f"data/{country}/{freq}/*.csv")
            for file in csvfilelist:
                os.remove(file)

    if mode == "unzip" or not mode:
        ## unzip data
        for country in countries['reporter'].to_list():
            zipfilelist = glob.glob(working_directory + f"data/{country}/{freq}/{country}*.zip")
            # print(zipfilelist)
            for file in zipfilelist:
                match = re.search(fr".*/data/{country}/{freq}/(.*).zip",file.replace("\\","/"))
                filename = match.group(1)
                try:
                    with zipfile.ZipFile(file, 'r') as zip_ref:
                        zip_ref.extractall(working_directory + f"data/{country}/{freq}/")
                except:
                    logging.warning(f"{filename}.zip bad!!")
                    os.remove(file)
                    pass
    
    if mode == "append" or not mode:
        # append data
        # if not os.path.exists(working_directory + f"{freq}/"):
        #     os.mkdir(working_directory + f"{freq}/")
        dropbox_path = working_directory + f"data/{country}/"  #"/Users/lizhi/Dropbox/Data/comtrade/comtrade_annual/eu_countries/"
        for country in countries['reporter'].to_list():
            # print(country)
            # if os.path.exists(working_directory + f"data/{country}/{country}.csv"):
            #     os.remove(working_directory + f"data/{country}/{country}.csv")
            csvfilelist = glob.glob(working_directory + f"data/{country}/{freq}/*.csv")
            
            for file in csvfilelist:
                print(file)
                match = re.search(r"type-C_r-[0-9]*_ps-([0-9]*)_freq.*", file.replace("\\","/"))
                if match:
                    newname = f"{country}" + match.group(1)
                    print(newname)
                    if not os.path.exists(working_directory + f"data/{country}/{freq}/"+ newname + ".csv"):
                        os.rename(file, working_directory + f"data/{country}/{freq}/"+ newname + ".csv")
                    else:
                        os.remove(file)
            i=0
            if freq == "annual":
                periods = years 
            else:
                periods = []
                for year in years:
                    for month in months:
                        tmp = crawler.periodfYearMonth(year, month)
                        periods.append(tmp)

            for period in periods:
                # print(f"{country}{year}")
                if os.path.exists(working_directory + f"data/{country}/{freq}/{country}{period}.csv"):
                    df = pd.read_csv(working_directory + f"data/{country}/{freq}/{country}{period}.csv", low_memory=False)
                    if not os.path.exists(dropbox_path + f"{country}_{freq}.csv") or i == 0:
                        df.to_csv(dropbox_path + f"{country}_{freq}.csv", index = False)
                    else:
                        df.to_csv(dropbox_path + f"{country}_{freq}.csv", index = False, header = False, mode = 'a')
                    os.remove(working_directory + f"data/{country}/{freq}/{country}{period}.csv")
                    i+=1
                logging.info(f"{country}{period} appended")
            logging.info(f"{country}_{freq}.csv ready to use")
    # comtrade_countries = pd.read_csv(working_directory + "comtrade_country_list_eu.csv")
    # comtrade_countries = comtrade_countries[comtrade_countries['EU'] == 1]
    # countries = comtrade_countries['reporter'].to_list()
    # ## look up in comtrade data reporter code
    # country_code = {}
    # # country_code = {'us':842,'eu':97,'mx':484}
    # for country in countries:
    #     country_code[country] = comtrade_countries[comtrade_countries['reporter'] == country]['reportercode'].iloc[0]
    # try:
    #     countries = [sys.argv[1]]
    # except:
    #     pass
    # ## retrieve data
    # start_year = 2003
    # end_year = 2022
    # for country in countries:
    #     for year in range(start_year,end_year+1):
    #         retrieve_data(country, country_code[country], year)
    # ## monthly
    # # for year in range(2003,2022+1):
    # #     for month in range(1,12+1):
    # #         if year<2022 or (year==2022 and month<9):
    # #             retrieve_data(year, month)

    # ## unzip data
    # for country in countries:
    #     zipfilelist = glob.glob(working_directory + f"data/{country}/{country}*.zip")
    #     print(zipfilelist)
    #     for file in zipfilelist:
    #         match = re.search(r".*/data/(.*).zip",file.replace("\\","/"))
    #         filename = match.group(1)
    #         try:
    #             with zipfile.ZipFile(file, 'r') as zip_ref:
    #                 zip_ref.extractall(working_directory + f"data/{country}/")
    #         except:
    #             print(filename + " bad")
    #             pass
    
    # # append data
    # dropbox_path = "/Users/lizhi/Dropbox/Data/comtrade/comtrade_annual/eu_countries/"
    # for country in countries:
    #     if os.path.exists(working_directory + f"data/{country}/{country}.csv"):
    #         os.remove(working_directory + f"data/{country}/{country}.csv")
    #     csvfilelist = glob.glob(working_directory + f"data/{country}/*.csv")
        
    #     for file in csvfilelist:
    #         print(file)
    #         match = re.search(r"type-C_r-[0-9]*_ps-([0-9]*)_freq.*", file.replace("\\","/"))
    #         if match:
    #             newname = f"{country}" + match.group(1)
    #             print(newname)
    #             if not os.path.exists(working_directory + f"data/{country}/"+ newname + ".csv"):
    #                 os.rename(file, working_directory + f"data/{country}/"+ newname + ".csv")
    #             else:
    #                 os.remove(file)
    #     i=0
    #     for year in range(start_year, end_year+1):
    #         print(f"{country}{year}")
    #         if os.path.exists(working_directory + f"data/{country}/{country}{year}.csv"):
    #             df = pd.read_csv(working_directory + f"data/{country}/{country}{year}.csv", low_memory=False)
    #             if not os.path.exists(dropbox_path + f"{country}.csv") or i == 0:
    #                 df.to_csv(dropbox_path + f"{country}.csv", index = False)
    #             else:
    #                 df.to_csv(dropbox_path + f"{country}.csv", index = False, header = False, mode = 'a')
    #             os.remove(working_directory + f"data/{country}/{country}{year}.csv")
    #             i+=1