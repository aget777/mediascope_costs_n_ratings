#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('reload_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

import os
import re
import sys
import json
import gc
import warnings
from datetime import datetime, date, timedelta

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import turbodbc
from turbodbc import connect

from IPython.display import JSON
from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

import config
from normalize_funcs import *
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table
 

db_name = config.db_name
# ссылка на гугл csv Словарь чистки объявлений
full_cleaning_link = config.full_cleaning_link
# ссылка на гугл csv Словарь дисконтов по Типам медиа
discounts_link = config.discounts_link


# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

warnings.simplefilter(action='ignore', category=FutureWarning)
# убираем лишние предупреждения
pd.set_option('mode.chained_assignment', None)


# In[3]:


# функция забирает гугл докс с чисткой Объявлений
# можем передать в нее список с названиями типов медиа, по которым отфильтровать датаФрейм
# по умолчанию забираем все
def get_cleaning_dict(media_type_lst=None):
    # т.к. забираем csv по ссылке, чтобы исключить ошибки при добавлении новых столбцов
    # формируем список из номеров колонок от 0 до 23
    cols_count = [i for i in range(23)]
    # опускаем заголовки на 1-ую строку датаФрейма и передаем номера столбцов, которые нам нужны
    df = pd.read_csv(full_cleaning_link, header=None, usecols=cols_count)
    # поднимаем из 1-ой строки названия в заголовки
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df['media_type'] = df['media_type'].str.upper()
    # если в функцию передан список типов медиа, то оставляем только нужные строки
    if media_type_lst:
        media_type_lst = [i.upper() for i in media_type_lst]
        df = df.query('media_type in @media_type_lst')
# удаляем дубликаты
    df = df.drop_duplicates('media_key_id')
# приводим формат даты к нормальному виду
    df['first_issue_date'] = pd.to_datetime(df['first_issue_date'])
# создаем флаг для очищенных / удаленных объявлений
    df['cleaning_flag'] = df['include_exclude'].apply(lambda x: 1 if x=='include' else 0)
    df = df.drop(['ad_transcribtion'], axis=1)
# приводим строки в верхний регистр, нормализуем цифры и тд.
    custom_ad_dict_int_lst = config.custom_ad_dict_int_lst
    df = normalize_columns_types(df, custom_ad_dict_int_lst)

    return df


# In[4]:


# создаем функцию для получения Дисконтов по типам медиа
# можем передать в нее список с названиями типов медиа, по которым отфильтровать датаФрейм
# по умолчанию забираем все

def get_media_discounts(media_type_lst=None):
    cols_count = [i for i in range(3)]
    # опускаем заголовки на 1-ую строку датаФрейма и передаем номера столбцов, которые нам нужны
    df = pd.read_csv(discounts_link, header=None, usecols=cols_count)
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df['media_type'] = df['media_type'].str.upper()
    
    if media_type_lst:
        media_type_lst = [i.upper() for i in media_type_lst]
        df = df.query('media_type in @media_type_lst')
        
    df = normalize_columns_types(df, ['year'], ['disc'])
    return df


# In[ ]:


# # dicts_lst = config.nat_tv_slices
# # Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
# # Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
# tv_index_dicts = {
#     'advertiserListId': [config.tv_index_advertiser_list_dict, config.tv_index_advertiser_list_dict_vars_list, config.tv_index_advertiser_list_dict_int_lst],
#     'brandListId': [config.tv_index_brand_list_dict, config.tv_index_brand_list_dict_vars_list, config.tv_index_brand_list_dict_int_lst],
#     'subbrandListId': [config.tv_index_subbrand_list_dict, config.tv_index_subbrand_list_dict_vars_list, config.tv_index_subbrand_list_dict_int_lst],
#     'modelListId': [config.tv_index_model_list_dict, config.tv_index_model_list_dict_vars_list, config.tv_index_model_list_dict_int_lst],
#     'articleList2Id': [config.tv_index_article_list2_dict, config.tv_index_article_list2_dict_vars_list, config.tv_index_article_list2_dict_int_lst],
#     'articleList3Id': [config.tv_index_article_list3_dict, config.tv_index_article_list3_dict_vars_list, config.tv_index_article_list3_dict_int_lst],
#     'articleList4Id': [config.tv_index_article_list4_dict, config.tv_index_article_list4_dict_vars_list, config.tv_index_article_list4_dict_int_lst],
#     'adSloganAudioId': [config.tv_index_audio_slogan_dict, config.tv_index_audio_slogan_dict_vars_list, config.tv_index_audio_slogan_dict_int_lst],
#     'adSloganVideoId': [config.tv_index_video_slogan_dict, config.tv_index_video_slogan_dict_vars_list, config.tv_index_video_slogan_dict_int_lst],
#     'regionId': [config.tv_index_region_dict, config.tv_index_region_dict_vars_list, config.tv_index_region_dict_int_lst],
#     'tvNetId': [config.tv_index_tv_net_dict, config.tv_index_tv_net_dict_vars_list, config.tv_index_tv_net_dict_int_lst],
#     'tvCompanyId': [config.tv_index_tv_company_dict, config.tv_index_tv_company_dict_vars_list, config.tv_index_tv_company_dict_int_lst],
#     'adTypeId': [config.tv_index_ad_type_dict, config.tv_index_ad_type_dict_vars_list, config.tv_index_ad_type_dict_int_lst],
# }


# In[ ]:





# In[ ]:





# In[5]:


def get_tv_index_dicts(dict_name, search_lst=None):
    if search_lst:
        search_lst = [str(id) for id in search_lst]
        
    if 'advertiserList' in dict_name:
        df = cats.get_tv_advertiser_list(search_lst)
        df = df.rename(columns={'id': 'advertiserListId', 'name': 'advertiserListName', 'ename': 'advertiserListEName'})

    if 'brandList' in dict_name:
        df = cats.get_tv_brand_list(search_lst)
        df = df.rename(columns={'id': 'brandListId', 'name': 'brandListName', 'ename': 'brandListEName'})
        
    if 'subbrandList' in dict_name:
        df = cats.get_tv_subbrand_list(search_lst)
        df = df.rename(columns={'id': 'subbrandListId', 'name': 'subbrandListName', 'ename': 'subbrandListEName'})

    if 'modelList'in dict_name:
        df = cats.get_tv_model_list(search_lst)
        df = df.rename(columns={'id': 'modelListId', 'name': 'modelListName', 'ename': 'modelListEName'})
        
    if 'articleList2' in dict_name:
        df = cats.get_tv_article_list2(search_lst)
        df = df.rename(columns={'id': 'articleList2Id', 'name': 'articleList2Name', 'ename': 'articleList2EName'})

    if 'articleList3' in dict_name:
        df = cats.get_tv_article_list3(search_lst)
        df = df.rename(columns={'id': 'articleList3Id', 'name': 'articleList3Name', 'ename': 'articleList3EName'})

    if 'articleList4' in dict_name:
        df = cats.get_tv_article_list4(search_lst)
        df = df.rename(columns={'id': 'articleList4Id', 'name': 'articleList4Name', 'ename': 'articleList4EName'})
                                
    # if 'adId' in dict_name:
    #     df = cats.get_tv_ad(search_lst)
    #     df = df.rename(columns={'id': 'adId', 'name': 'adName', 'ename': 'adEName'})

    if 'adSloganAudioId' in dict_name:
        df = cats.get_tv_ad_slogan_audio(search_lst)
        df = df.rename(columns={'id': 'adSloganAudioId', 'name': 'adSloganAudioName', 'notes': 'adSloganAudioNotes'})

    if 'adSloganVideo' in dict_name:
        df = cats.get_tv_ad_slogan_video(search_lst)
        df = df.rename(columns={'id': 'adSloganVideoId', 'name': 'adSloganVideoName', 'notes': 'adSloganVideoNotes'})

    # if 'region' in dict_name:
    #     df = cats.get_tv_region(search_lst)
    #     df = df.rename(columns={'id': 'regionId', 'name': 'regionName', 'ename': 'regionEName'})

    # if 'tvNet' in dict_name:
    #     df = cats.get_tv_net()
    #     df = df.rename(columns={'id': 'tvNetId', 'name': 'tvNetName', 'ename': 'tvNetEName'})

    # if 'tvCompany' in dict_name:
    #     df = cats.get_tv_company(search_lst)
    #     df = df.rename(columns={'id': 'tvCompanyId', 'name': 'tvCompanyName', 'ename': 'tvCompanyEName'})

    # if 'adType' in dict_name:
    #     df = cats.get_tv_ad_type(search_lst)
    #     df = df.rename(columns={'id': 'adTypeId', 'name': 'adTypeName', 'ename': 'adTypeEName'})
        
    return df


# In[6]:


# функция для обновления всех словарей ТВ индекс КРОМЕ nat_tv_ad_dict
# После записи данных в отчеты Simple и Buying
# в справочнике nat_tv_ad_dict - содержится cleanig_flag, который еше НЕ обновился
# таким образом у нас зафиксировано состояние с прошлой загрузки и мы можем понять, какие новые объявления появилсь в БД
# для этого по каждому отдельному столбцу, который является ключом для верхнеуровневых справочников
# мы забираем список уникальных ИД, у которых cleanig_flag=2 (т.е. только новые объявления)
# список этих ИД мы передаем в запрос к ТВ Индексу, чтобы только то, что нам нужно
# в конце жобавляем новые строки к справочникам
def update_tv_index_dicts():
    
    # сначала проверяем справочник объявлений - есть там новые или нет
    query = f"select top(1) adId from nat_tv_ad_dict where cleaning_flag=2"
    df = get_mssql_table(config.db_name, query=query)
    
    # если ответ НЕ пустой, то запускаем логику обновления всех верхнеуровневых справочников
    if not df.empty:
        # у нас сформирован справочник словарей
        # Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
        # Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
        for key, value in config.tv_index_dicts.items():
            # передаем в SQL запрос название поля, которое нас интересует
            query = f"select distinct {key} from nat_tv_ad_dict where cleaning_flag=2"
            df = get_mssql_table(config.db_name, query=query)
            
            # преобразуем датаФрейм из 1 столбца в список
            search_lst = df[key].tolist()
            # отправляем запрос в ТВ индекс
            df = get_tv_index_dicts(key, search_lst)
            # нормализуем данные
            df = normalize_columns_types(df, value[2])
            # записываем в БД
            downloadTableToDB(db_name, value[0], df)
    else:
        print(f'Новых данных для загрузки нет')


# In[8]:


# update_tv_index_dicts()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




