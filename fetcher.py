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
    retry_counter = 0
    while retry_counter<5:
        try:
            df = drug_list(farm)
            break
        except requests.HTTPError as e:
            print (e, farm)
            retry_counter += 1

    if (df.empty != True):
        pesticide_df = pesticide_df.append(df)
        print ('Success', farm)
    else:
        print ('Failed', farm)
#remove duplicated records
pesticide_df.drop_duplicates(inplace = True)
pesticide_df.reset_index(inplace = True, drop = True)
#overwrite the most recent file
pesticide_df.to_csv('./data/pesticides.csv', encoding = 'utf-8-sig')
#back up
pesticide_df.to_csv('./data/pesticides_'+pd.to_datetime("today").strftime("%Y-%m-%d")+'.csv', encoding = 'utf-8-sig')


#FETCHING products info
def label_page_parse (match):
    PAGE_PATH = match.group(1)
    r = requests.get('https://pesticide.baphiq.gov.tw/'+PAGE_PATH)
    IMAGE_LIST = re.findall(r'type=mark&url=([\w-]*.jpg)', r.text)
    print ('Found images:', IMAGE_LIST)
    for img_name in IMAGE_LIST:
        try:
            with open('./label_img/'+img_name, 'wb') as f:
                img = requests.get('https://pesticide.baphiq.gov.tw/information/Query/ViewmarkDownload/?type=mark&url='+img_name)
                f.write(img.content)
            print (img_name, 'downloaded')
        except Exception as e:
            print (img_name, e)
    return (','.join(IMAGE_LIST))

REGISTERED_URI = 'https://pesticide.baphiq.gov.tw/information/Query/RegisterList/?regtid=&regtnostart=&regtnoend=&pestcd=&compna=&prodga=&psbkna=&psbkga=&pestna=&pestga=&cidecd=&pescnt=&type=1&pagesize=55660&newquery=true'
r = requests.get(REGISTERED_URI)
assert r.status_code == requests.codes.ok, "無法擷取標籤頁面 " + HOME_URI
#parse label values and download label images
parsed_page = re.sub(r'<a href="(/information/Query/RegisterViewMark/\?regtid=\d*&regtno=\d*)" class="btn-s" target="_blank">標示</a>',label_page_parse, r.text)
df = pd.read_html(parsed_page)
registered_df = df[1]
registered_df.columns = df[0].columns
registered_df.drop('使用範圍', axis=1, inplace=True)
#overwrite the most recent file
registered_df.to_csv('./data/registered.csv', encoding = 'utf-8-sig')
#back up
registered_df.to_csv('./data/registered_'+pd.to_datetime("today").strftime("%Y-%m-%d")+'.csv', encoding = 'utf-8-sig')
