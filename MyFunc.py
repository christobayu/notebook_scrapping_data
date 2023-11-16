import io
import pandas as pd

import numpy as np
import json

import gspread as gs

import geopy.distance
from scipy.spatial import cKDTree

from pandarallel import pandarallel
pandarallel.initialize(progress_bar=False,nb_workers=2)

def get_post_code(longitude,latitude,data_location):
    latitude=float(latitude)
    longitude=float(longitude)
    # if latitude<=6 and latitude>=-11 and longitude<=95 and longitude>=141 : pass
    # else : latitude,longitude=longitude,latitude

    target = [latitude,longitude]
    list_coordinate = np.c_[data_location.latitude, data_location.longitude]
    
    try:
        indx = cKDTree(list_coordinate).query(target,k=1)[1]
        temp=data_location.iloc[indx]
    except:
        pass
    else:  
        postal_code=temp.Kodepos
        province=temp.Province
        kabupaten=temp.Kabupaten
        kecamatan=temp.Kecamatan
        kelurahan=temp.Kelurahan

    try:
        return pd.Series([postal_code,province.upper(),kabupaten.upper(),kecamatan.upper(),kelurahan.upper()])
    except:
        return pd.Series([None,None,None,None,None])

def is_on_radius(coordinate1,coordinate2,radius):
    if geopy.distance.geodesic(coordinate1, coordinate2).m <= radius:
        return True
    else:
        return False

def get_hot_area(latitude,longitude,hot_area):
    latitude=float(latitude)
    longitude=float(longitude)

    target = [latitude,longitude]
    list_coordinate = np.c_[hot_area.Latitude, hot_area.Longitude]
    indx = cKDTree(list_coordinate).query(target,k=1000)[1]
    indx = indx[:len(hot_area)]
    temp=hot_area.iloc[indx]
    temp.Radius=temp.Radius.astype(int)
    temp=temp[temp.apply(lambda x:is_on_radius([x.Latitude,x.Longitude],[latitude,longitude],x.Radius),axis=1)]
    if len(temp)>0:
        return temp.iloc[0]['Hot Area']
    else:
        return None
    
def cleaning_data_merchants(data):
    data.rename(columns={'query':'keyword'},inplace=True)
    data.name=data.name.str.title()
    data.name.fillna('',inplace=True)
    data.subtypes.fillna('',inplace=True)
    data.name=data.name.str.replace('"','')

    data.type=data.type.str.lower()
    data.keyword=data.keyword.str.lower()
    data.subtypes=data.subtypes.str.lower()
    data.category=data.category.str.lower()
    
    new_columns=[]
    for column in data.columns:
        new_columns.append(column.replace('.','__').replace('(','').replace(')',''))
    data.columns=new_columns

    for column in data.columns :
        try:
            data[column]=data[column].str.replace(';','').str.replace('"','').str.replace('\n',' ').str.replace('\r','').str.replace('\t','')
        except:
            pass

    about_columns=[]
    others_columns=[]
    for column in data.columns:
        if 'about__' in column:
            about_columns.append(column)
        else:
            others_columns.append(column)
    # about_columns.sort()
    about_columns.sort(reverse=False)
    data=data[others_columns+about_columns]
    data=data[data.subtypes.isnull()==False]

    data.reviews.fillna(0,inplace=True)
    data.photos_count.fillna(0,inplace=True)

    data.working_hours=data.working_hours.apply(json.dumps)
    data.about=data.about.apply(json.dumps)

    return data

def is_exclude(subtypes,excludes):
    subtypes=subtypes.split(',')
    total=[]
    for subtype in subtypes:
        for exclude in excludes:
            if exclude in subtype:
                total.append(subtype)
    total=len(set(total))
    
    if len(subtypes)==total:
        return False
    else:
        return True

def get_subtype_exclude(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1H9YtIjyPaW6hVSWbGh65KU0f9Q97QgoALp-nBsBvo3g/edit#gid=658246273')

    worksheet = sheet.worksheet('Excluded Subtypes')
    subtype_exclude = pd.DataFrame(worksheet.get_all_values())
    subtype_exclude.columns=['KEYWORD']
    subtype_exclude=subtype_exclude[subtype_exclude.KEYWORD!='']
    subtype_exclude.KEYWORD=subtype_exclude.KEYWORD.str.lower()

    return subtype_exclude
    # worksheet = sheet.worksheet('Excluded Types')
    # subtype_exclude2 = pd.DataFrame(worksheet.get_all_values())
    # subtype_exclude2.columns=['KEYWORD']
    # subtype_exclude2=subtype_exclude2[subtype_exclude2.KEYWORD!='']
    # subtype_exclude2.KEYWORD=subtype_exclude2.KEYWORD.str.lower()
    
    # return pd.concat([subtype_exclude,subtype_exclude2])

def get_keyword_allowed(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1DRlB-W2L31xRcKxGxaCxoScTq-xUs-hAe4s8XbhA9eo/edit#gid=1604329748')

    worksheet = sheet.worksheet('Keywords')
    keyword_allowed = pd.DataFrame(worksheet.get_all_values())
    keyword_allowed.columns=keyword_allowed.iloc[0]
    keyword_allowed=keyword_allowed[1:]
    keyword_allowed=keyword_allowed[['Keywords Allowed']]
    keyword_allowed.columns=['KEYWORD']
    keyword_allowed=keyword_allowed[keyword_allowed.KEYWORD!='']
    keyword_allowed.KEYWORD=keyword_allowed.KEYWORD.str.lower()
    return keyword_allowed

def get_name_exclude(private_key):
    gc = gs.service_account(filename=f'{private_key}')

    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1H9YtIjyPaW6hVSWbGh65KU0f9Q97QgoALp-nBsBvo3g/edit#gid=79371918')
    worksheet = sheet.worksheet('Excluded Keywords')
    name_exclude = pd.DataFrame(worksheet.get_all_values())
    name_exclude.columns=name_exclude.iloc[0]
    name_exclude=name_exclude[1:]
    name_exclude.columns=['KEYWORD']
    name_exclude=name_exclude[name_exclude.KEYWORD!='']
    name_exclude.KEYWORD=name_exclude.KEYWORD.str.lower()
    return name_exclude

def get_popular_brands(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1DRlB-W2L31xRcKxGxaCxoScTq-xUs-hAe4s8XbhA9eo/edit#gid=1604329748')

    worksheet = sheet.worksheet('Popular Brands')
    popular_brands = pd.DataFrame(worksheet.get_all_records())
    popular_brands.columns=['brands']
    popular_brands.brands=popular_brands.brands.str.lower()
    popular_brands.brands=popular_brands.brands.str.replace("'",'')
    return popular_brands

def get_hot_area_list(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1SyTyTTLCL46cWFAlV-QNNRCZjyVuqAg-m0C8zpmCMUQ/edit#gid=0')
    worksheet = sheet.worksheet('Area')
    hot_area = pd.DataFrame(worksheet.get_all_values())
    hot_area.columns=hot_area.iloc[0]
    hot_area=hot_area[1:]
    hot_area=hot_area[hot_area.Latitude!='']
    return hot_area

def get_exclude_restorant(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1H9YtIjyPaW6hVSWbGh65KU0f9Q97QgoALp-nBsBvo3g/edit#gid=0')
    worksheet = sheet.worksheet('Exclude Restaurant')
    exclude_restorant = pd.DataFrame(worksheet.get_all_values())
    exclude_restorant.columns=exclude_restorant.iloc[0]
    exclude_restorant=exclude_restorant[1:]

    exclude_restorant=exclude_restorant[['Place Id']]
    exclude_restorant.columns=['place_id']
    exclude_restorant=exclude_restorant[exclude_restorant.place_id!='']
    return exclude_restorant

def get_include_restorant(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1mADtRV9O1WxT-S0D3fG3cnY7k9XbQqDAEbzb_4SGaw0/edit#gid=314789269')
    worksheet = sheet.worksheet('Type 1 Mall')
    mall_type1 = pd.DataFrame(worksheet.get_all_values())
    mall_type1.columns=mall_type1.iloc[0]
    mall_type1=mall_type1[1:]

    mall_type1=mall_type1[['google id']]
    mall_type1.columns=['google_id']
    mall_type1=mall_type1[mall_type1.google_id!='']

    return mall_type1


def get_include_restaurant_type1(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1H9YtIjyPaW6hVSWbGh65KU0f9Q97QgoALp-nBsBvo3g/edit?usp=sharing')
    worksheet = sheet.worksheet('Included Type 1')
    outlet_type1 = pd.DataFrame(worksheet.get_all_values())
    outlet_type1.columns=outlet_type1.iloc[0]
    outlet_type1=outlet_type1[1:]

    outlet_type1=outlet_type1[['Place Id','Google Id']]
    outlet_type1.columns=['place_id','google_id']
    # outlet_type1=outlet_type1[(outlet_type1.place_id!='')&(outlet_type1.google_id!='')]

    return outlet_type1

def get_include_restaurant_mall_type1(private_key):
    gc = gs.service_account(filename=f'{private_key}')
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xj4IXNc0qHtpPhMauK7XiYKxCLnaIqDjxJjV4ttqPkk/edit?usp=sharing')

    worksheet = sheet.worksheet('Type 1 Mall')

    outlet_in_mall_type1 = pd.DataFrame(worksheet.get_all_values())
    outlet_in_mall_type1.columns=outlet_in_mall_type1.iloc[0]
    outlet_in_mall_type1=outlet_in_mall_type1[1:]
    outlet_in_mall_type1=outlet_in_mall_type1[outlet_in_mall_type1.building_name!='']
    outlet_in_mall_type1.columns=['input_date', 'address', 'google_id','place_id', 'building_name']

    return outlet_in_mall_type1



def filtering_data_merchants(data,private_key):

    subtype_exclude=get_subtype_exclude(private_key)
    keyword_allowed=get_keyword_allowed(private_key)
    name_exclude=get_name_exclude(private_key)
    popular_brands=get_popular_brands(private_key)
    # hot_area=get_hot_area_list(private_key)
    exclude_restorant=get_exclude_restorant(private_key)
    mall_type1=get_include_restorant(private_key)

    data.name=data.name.str.lower()
    data.name=data.name.astype(str)
    data.subtypes.fillna('',inplace=True) 
    data.name.fillna('',inplace=True) 
    data.reviews.fillna(0,inplace=True)
    data.photos_count.fillna(0,inplace=True)
    data=data[data.business_status=='OPERATIONAL']

    data_popular=data[data['name'].str.contains('|'.join(popular_brands.brands))]
    data_mall_type1=data[data['google_id'].isin(mall_type1.google_id)]

    data=data[~data.google_id.isin(data_popular.google_id)]
    data=data[~data.google_id.isin(data_mall_type1.google_id)]

    data.reviews=data.reviews.astype(float).astype(int)
    data.photos_count=data.photos_count.astype(float).astype(int)

    data=data[data.reviews>1]
    data=data[data.photos_count>5]
    # data=data[data['keyword'].str.contains('|'.join(keyword_allowed.KEYWORD))]

    data=pd.concat([data_mall_type1,data_popular,data])

    data['type']=data['type'].astype(str)
    data['subtypes']=data['subtypes'].astype(str)
    data['category']=data['category'].astype(str)

    data=data[~data['subtypes'].str.contains('|'.join(subtype_exclude.KEYWORD))]
    data=data[~data['name'].str.contains('|'.join(name_exclude.KEYWORD))]
    data=data[~data['place_id'].isin(exclude_restorant.place_id)]
    del data_popular

    # data['hot_area']=data.apply(lambda x:get_hot_area(x.latitude,x.longitude,hot_area),axis=1)

    about_columns=[]
    others_columns=[]
    for column in data.columns:
        if 'about__' in column:
            about_columns.append(column)
        else:
            others_columns.append(column)
    about_columns.sort(reverse=False)
    data=data[others_columns+about_columns]

    data.name=data.name.str.title()

    # data1=data[(data['keyword'].str.contains('restaurant|diner'))&(data['about__Service options__Dine-in']==True)]
    # data2=data[~(data['keyword'].str.contains('restaurant|diner'))]
    # data=pd.concat([data1,data2])

    # del data1,data2

    return data


from google.cloud import storage
import glob

def upload_file_to_GCS(private_key,bucket_name,src_path_file,target_path_file):
    path_to_private_key = f'./{private_key}'
    client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    bucket = storage.Bucket(client, bucket_name)

    print(f'Uploading {src_path_file}')
    # The name of file on GCS once uploaded
    blob = bucket.blob(target_path_file)
    # The content that will be uploaded
    blob.upload_from_filename(src_path_file,timeout=None)
    print(f'Path in GCS: {bucket_name}/{target_path_file}')


from google.cloud import bigquery
def create_table_from_gcs(private_key,gcs_path_uri,schema,table_name,table_schema,insert_mode='replace'):
    path_to_private_key = f'./{private_key}'
    bigqueryClient = bigquery.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    jobConfig = bigquery.LoadJobConfig()
    jobConfig.skip_leading_rows = 1
    jobConfig.source_format = bigquery.SourceFormat.CSV
    # jobConfig.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    if insert_mode=='replace':
        jobConfig.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif insert_mode=='append':
        jobConfig.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    jobConfig.field_delimiter = ";"
    jobConfig.autodetect=True
    jobConfig.schema=table_schema
    jobConfig.allow_quoted_newlines=True
    tableRef = bigqueryClient.dataset(schema).table(table_name)
    
    bigqueryJob = bigqueryClient.load_table_from_uri(gcs_path_uri, tableRef, job_config=jobConfig)
    bigqueryJob.result()
    print(f'table on : {schema}.{table_name}')


