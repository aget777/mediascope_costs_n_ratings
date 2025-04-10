#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import turbodbc
from turbodbc import connect


import config
import config_media_costs
from normalize_funcs import normalize_columns_types
from create_dicts import get_cleaning_dict
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table


db_name = config.db_name


# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

warnings.simplefilter(action='ignore', category=FutureWarning)
# убираем лишние предупреждения
pd.set_option('mode.chained_assignment', None)


# In[ ]:





# In[3]:


def get_adex_dicts(dict_name, search_lst=None):
    search_lst_str = ''
    # если передан список уникальных ИД объявлений, то преобразуем список в строку
    if search_lst:
        # достаем из настроек словарей навзание колонк,по которой нужно выполнить фильтрацию в исходной БД Медиаскоп
        search_col_name = config_media_costs.adex_dicts[dict_name][3]
        
        search_lst_str = config.get_lst_to_str(search_lst)
        # формируем условие для фильтрации запроса к БД
        search_lst_str = f'where {search_col_name} in ({search_lst_str})'
    # для каждого справочника формируем строку с указанием полей для загрузки
    if 'tv_Ad'==dict_name:
        cols = 'vid, name as adName, notes, atid, stdur, alid, blid, sblid, mlid, slid2, slid3, slid4, fiss, slaid, slvid'
        

    # отправляем запрос в БД Медиа инвестиции
    query = f'select {cols} from {dict_name} {search_lst_str}' 
    df = get_mssql_table(config.investments_db_name, query=query, conn_lst=config.conn_lst)
    
    # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
    # перебираем справочник config_tv_investments.rename_cols_dict
    # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
    # чтобы передать для присвоения нового названия
    new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
    df = df.rename(columns=new_cols_name)
    
    return df


# In[13]:


# query = f"select distinct adId  from media_tv_costs"
# ad_id_dict = get_mssql_table(config.db_name, query=query)
# search_lst = list(ad_id_dict['adId'])


# In[14]:


# len(search_lst)


# In[5]:


# createDBTable(config.db_name, 'media_tv_costs', config_media_costs.media_tv_costs_vars_list, flag='create')


# In[9]:


# for key, value in config_media_costs.adex_dicts.items():
#     print(key)
#     df = get_adex_dicts(key, search_lst)


# In[10]:


# df.shape


# In[11]:


# df.head(2)


# In[ ]:


# for key, value in config_media_costs.adex_dicts.items():
#     print(key)


# In[ ]:


# функция для обонвления основного справочнка объявлений nat_tv_ad_dict
# ее запускаем в самую после заливки данных из ТВ индекс и обновления всех справочников
# НО ПЕРЕД заливкой новых объявлений в гугл докс

def update_adex_ad_dict(df, media_type):
    # забираем гугл докс с чисткой
    df_cleaning_dict = get_cleaning_dict(media_type=media_type)
    # нормализуем типы данных
    df_cleaning_dict = normalize_columns_types(df_cleaning_dict, config_tv_index.custom_ad_dict_int_lst) 
    
    # создаем список из названий полей, которые нам нужны дальше для метчинга
    custom_cols_list = [col[:col.find(' ')] for col in config_tv_index.custom_ad_dict_vars_list]
    custom_cols_list = list(set(custom_cols_list) - set(['ad_id', 'media_type']))
    # оставляем только нужные поля
    df_cleaning_dict = df_cleaning_dict[custom_cols_list]


# In[ ]:


# df_cleaning_dict = get_cleaning_dict(media_type='tv')
# # нормализуем типы данных
# df_cleaning_dict = normalize_columns_types(df_cleaning_dict, config_tv_index.custom_ad_dict_int_lst) 

# # создаем список из названий полей, которые нам нужны дальше для метчинга
# custom_cols_list = [col[:col.find(' ')] for col in config_tv_index.custom_ad_dict_vars_list]
# custom_cols_list = list(set(custom_cols_list) - set(['ad_id', 'media_type']))
# # оставляем только нужные поля
# df_cleaning_dict = df_cleaning_dict[custom_cols_list]

