import pandas as pd
import time
import datetime
import os
import random
import requests
import json

working_directory = "/Users/lizhi/Dropbox/CUHK_RA/shipping/" if os.name != 'nt' else "C:/Users/Guest_dse/Dropbox/CUHK_RA/shipping/"


comtrade_cols = ['Classification', 'Year', 'Period', 'Period Desc.', 'Aggregate Level',
       'Is Leaf Code', 'Trade Flow Code', 'Trade Flow', 'Reporter Code',
       'Reporter', 'Reporter ISO', 'Partner Code', 'Partner', 'Partner ISO',
       '2nd Partner Code', '2nd Partner', '2nd Partner ISO',
       'Customs Proc. Code', 'Customs', 'Mode of Transport Code',
       'Mode of Transport', 'Commodity Code', 'Commodity', 'Qty Unit Code',
       'Qty Unit', 'Qty', 'Alt Qty Unit Code', 'Alt Qty Unit', 'Alt Qty',
       'Netweight (kg)', 'Gross weight (kg)', 'Trade Value (US$)',
       'CIF Trade Value (US$)', 'FOB Trade Value (US$)', 'Flag']
keep_cols = ['Partner Code', 'Partner','Partner ISO']
df = pd.DataFrame(columns = comtrade_cols)
for i in range(2010,2022):
    df = df.append(pd.read_csv(working_directory + f"data/comtrade/comtrade{i}.csv"))
df = df[keep_cols]
df.drop_duplicates(inplace = True)
df = df[df['Partner']!="World"]
df.rename(columns = {'Partner Code':'partnercode', 'Partner':'partner', 'Partner ISO':'partneriso'}, inplace = True)
df.to_csv(working_directory + "/data/comtrade/comtrade_country_list.csv", index = False)
