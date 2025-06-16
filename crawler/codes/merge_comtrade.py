import pandas as pd
import time
import os
import csv
# USE get_comtrade!!!!#
working_directory = "/Users/lizhi/Dropbox/CUHK_RA/shipping/" if os.name != 'nt' else "C:/Users/Guest_dse/Dropbox/CUHK_RA/shipping/"

country_list = pd.read_csv(
    working_directory+"data/comtrade/comtrade_country_list.csv")
Ncountry = country_list.shape[0]

TradeFlow = {'All': 0, 'Import': 1,
             'Export': 2, 're-Export': 3, 're-Import': 4}
flow = TradeFlow['Import']

start_year = 2012
end_year = 2021

comtrade_cols = ['Classification', 'Year', 'Period', 'Period Desc.', 'Aggregate Level',
                 'Is Leaf Code', 'Trade Flow Code', 'Trade Flow', 'Reporter Code',
                 'Reporter', 'Reporter ISO', 'Partner Code', 'Partner', 'Partner ISO',
                 '2nd Partner Code', '2nd Partner', '2nd Partner ISO',
                 'Customs Proc. Code', 'Customs', 'Mode of Transport Code',
                 'Mode of Transport', 'Commodity Code', 'Commodity', 'Qty Unit Code',
                 'Qty Unit', 'Qty', 'Alt Qty Unit Code', 'Alt Qty Unit', 'Alt Qty',
                 'Netweight (kg)', 'Gross weight (kg)', 'Trade Value (US$)',
                 'CIF Trade Value (US$)', 'FOB Trade Value (US$)', 'Flag']


df = pd.DataFrame(columns=comtrade_cols)
for i in range(Ncountry):
    exportcountrycode = country_list.partnercode.iloc[i]
    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            period = str(year) + '0' + \
                         str(month) if month < 10 else str(year) + str(month)
            filename = country_list.partner.iloc[i] + "_" + \
                str(exportcountrycode) + "_" + period + ".csv"
            if os.path.exists(working_directory + "/data/comtrade/comtradeusimport/" + filename):
                df = df.append(pd.read_csv(working_directory
                               + "/data/comtrade/comtradeusimport/" + filename))
            print(f"Appending {filename} "
                  + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
df.to_csv(working_directory + "/data/comtrade/comtradeusimport/"
          + f"comtrade_{start_year}_{end_year}.csv", index=False)
