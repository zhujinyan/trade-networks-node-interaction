# Run
1. In the terminal, set current working directory to ./comtrade/codes
2. Run `python comtrade_bulk_download.py freq [mode] [country]`
   1. mode includes 
      1. download: download files
      2. check: check files integrity
      3. unzip: unzip files into csv
      4. append: append the unzipped files in one csv
   2. country includes the country names in ./comtrade/basic_info/reportAreas.json
3. If `mode` is not specified, the program will run through all modes. 
4. If `country` is not specified, the program will run through all countries. 