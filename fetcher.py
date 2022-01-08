import pandas as pd
import requests
import re

def drug_list (farm = 'A010101'):
    URI = 'https://pesticide.baphiq.gov.tw/information/Query/BugUserange/?flag=&bug=&farm='+farm
    try:
        df = pd.read_html(URI)
        return df[0]
    except ValueError as e:
        return pd.DataFrame()
    
HOME_URI = "https://pesticide.baphiq.gov.tw/information/Query/Bug"
r = requests.get(HOME_URI)
assert r.status_code == requests.codes.ok, "無法擷取農藥資訊網 " + HOME_URI

#retrieve farm id list from homepage
farm_list = re.findall(r'id="farmhidden_([\w]*)"', r.text)
#loop over all farm id
pesticide_df = pd.DataFrame()
for farm in farm_list[0:10]:
    print ('Start downloading', farm)
    df = drug_list(farm)
    if (df.empty != True):
        pesticide_df = pesticide_df.append(df)
        print ('Success', farm)
    else:
        print ('Failed', farm)
#remove duplicated records
pesticide_df.drop_duplicates(inplace = True)
pesticide_df.reset_index(inplace = True, drop = True)
pesticide_df.to_csv('./data/pesticides.csv', encoding = 'utf-8-sig')

