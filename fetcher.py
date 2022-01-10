import pandas as pd
import requests
import re
import argparse
import sys
import threading
import os

parser = argparse.ArgumentParser()
parser.add_argument("-a",
                    "--action",
                    type=str,
                    default="label",
                    help="'chemicals' for downloading list of chemicals; 'label' for downloading product labels list; 'label_img' for downloading label images")
parser.add_argument("-s",
                    "--regtnostart",
                    type=int,
                    default="00000",
                    help="start of the index of registered.csv, only work when -a label_img")
parser.add_argument("-e",
                    "--regtnoend",
                    type=int,
                    default="99999",
                    help="end of the index of registered.csv, only work when -a label_img")     
#jupyter會傳一個-f進來，不這樣接會有錯誤
args, unknown = parser.parse_known_args()

def chemical_list_dl (farm = 'A010101', path = ''):
    URI = 'https://pesticide.baphiq.gov.tw/information/Query/BugUserange/?flag=&bug=&farm='+farm
    retry_counter = 0
    while retry_counter<5:
        try:
            df = pd.read_html(URI)[0]
            df.to_csv(path, index = False)
            print ('Success:', farm)
            break
        except ValueError as e:
            #no table found
            break
        except Exception as e:
            print (e)
            retry_counter += 1

def save_img(URI, path):
    retry_counter = 0
    while retry_counter<5:
        try:
            with open(path, 'wb') as f:
                img = requests.get(URI)
                f.write(img.content)
            print (URI, 'downloaded')
            break
        except Exception as e:
            retry_counter += 1
            print(URI, e)
        
def label_page_parse (match):
    PAGE_PATH = match.group(1)
    retry_counter = 0
    while retry_counter<5:
        try:    
            r = requests.get('https://pesticide.baphiq.gov.tw/'+PAGE_PATH)
            img_list = re.findall(r'type=mark&url=([\w-]*.jpg)', r.text)
            if len(img_list) == 0:
                print ('No image found')
                return ''
            else:
                print ('Found images:', img_list)
                return (','.join(img_list))
            break
        except Exception as e:
            retry_counter += 1
        return ''
    
if args.action == 'chemicals':
    HOME_URI = "https://pesticide.baphiq.gov.tw/information/Query/Bug"
    r = requests.get(HOME_URI)
    assert r.status_code == requests.codes.ok, "無法擷取農藥資訊網 " + HOME_URI
    #retrieve farm id list from homepage
    farm_list = re.findall(r'id="farmhidden_([\w]*)"', r.text)
    #loop over and download from all farm id
    pesticide_df = pd.DataFrame()
    threads = []
    for index, farm in enumerate(farm_list):
        print ('Start downloading', farm)
        thread = threading.Thread(target = chemical_list_dl, args = (farm, './tmp/'+farm+'.csv'))
        thread.start()
        threads.append(thread)
        if (index+1) % 20 == 0:
            print('Waiting', index)
            for thread in threads :
                thread.join()            
    #wait all thread done
    for thread in threads :
        thread.join()
    
    #import and merge tmp file from all farm id
    pesticide_df = pd.DataFrame()
    for farm in farm_list:
        tmp_path = './tmp/'+farm+'.csv'
        try:
            pesticide_df = pesticide_df.append(pd.read_csv(tmp_path))
            #remove temp file
            os.remove(tmp_path)
        except Exception as e:
            print('Loading failed:', farm)
    #remove duplicated records
    pesticide_df.drop_duplicates(inplace = True)
    pesticide_df.reset_index(inplace = True, drop = True)
    #overwrite the most recent file
    pesticide_df.to_csv('./data/pesticides.csv', encoding = 'utf-8-sig')
    #back up
    pesticide_df.to_csv('./data/pesticides_'+pd.to_datetime("today").strftime("%Y-%m-%d")+'.csv', encoding = 'utf-8-sig')


if args.action == 'label':
    REGISTERED_URI = 'https://pesticide.baphiq.gov.tw/information/Query/RegisterList/?regtid=&regtnostart=&regtnoend=&pestcd=&compna=&prodga=&psbkna=&psbkga=&pestna=&pestga=&cidecd=&pescnt=&type=1&pagesize=55660&newquery=true'
    #REGISTERED_URI = 'https://pesticide.baphiq.gov.tw/information/Query/RegisterList/?regtid=&regtnostart=00192&regtnoend=00394&pestcd=&compna=&prodga=&psbkna=&psbkga=&pestna=&pestga=&cidecd=&pescnt=&type=1&pagesize=55660&newquery=true'
    r = requests.get(REGISTERED_URI)
    assert r.status_code == requests.codes.ok, "無法擷取標籤頁面 " + HOME_URI
    #parsing label values and download label images
    parsed_page = re.sub(r'<a href="(/information/Query/RegisterViewMark/\?regtid=\d*&regtno=\d*)" class="btn-s" target="_blank">標示</a>',label_page_parse, r.text)
    #saving table as csv
    df = pd.read_html(parsed_page)
    registered_df = df[1]
    registered_df.columns = df[0].columns
    registered_df.drop('使用範圍', axis=1, inplace=True)
    #overwrite the most recent file
    registered_df.to_csv('./data/registered.csv', encoding = 'utf-8-sig')
    #back up
    registered_df.to_csv('./data/registered_'+pd.to_datetime("today").strftime("%Y-%m-%d")+'.csv', encoding = 'utf-8-sig')


if args.action == 'label_img':
    #Downloading label images
    registered_df = pd.read_csv('./data/registered.csv', encoding = 'utf-8-sig')
    img_list = ','.join(registered_df[args.regtnostart:args.regtnoend]['標示'].astype(str).tolist()).split(',')
    threads = []
    for index, img_name in enumerate(img_list):
        if '.jpg' not in img_name:
            continue
        IMG_URI = 'https://pesticide.baphiq.gov.tw/information/Query/ViewmarkDownload/?type=mark&url='+img_name
        IMG_SAVE_PATH = './label_img/'+img_name
        thread = threading.Thread(target = save_img, args = (IMG_URI, IMG_SAVE_PATH))
        thread.start()
        threads.append(thread)
        #wait every 100 thread
        if ((index+1) % 20 == 0):
            print ('Waiting', index)
            for thread in threads:
                thread.join()


