#!/usr/bin/env python
# coding: utf-8

# In[6]:


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



import config
import config_tv_index
import normalize_funcs 
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table
 

db_name = config.db_name
# ссылка на гугл csv Словарь чистки объявлений
full_cleaning_link = config.full_cleaning_link


# In[7]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

warnings.simplefilter(action='ignore', category=FutureWarning)
# убираем лишние предупреждения
pd.set_option('mode.chained_assignment', None)


# In[ ]:


# функция забирает гугл докс с чисткой Объявлений
# можем передать в нее список с названиями типов медиа, по которым отфильтровать датаФрейм
# по умолчанию забираем все
def get_cleaning_dict(media_type='tv'):
    # т.к. забираем csv по ссылке, чтобы исключить ошибки при добавлении новых столбцов
    # формируем список из номеров колонок от 0 до 23
    cols_count = [i for i in range(23)]
    # опускаем заголовки на 1-ую строку датаФрейма и передаем номера столбцов, которые нам нужны
    df = pd.read_csv(full_cleaning_link, header=None, usecols=cols_count)
    # поднимаем из 1-ой строки названия в заголовки
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df['media_type'] = df['media_type'].str.upper()

    media_type = media_type.upper()
    df = df[df['media_type']==media_type]
# удаляем дубликаты
    df = df.drop_duplicates('media_key_id')
# приводим формат даты к нормальному виду
    df['first_issue_date'] = pd.to_datetime(df['first_issue_date'])
# создаем флаг для очищенных / удаленных объявлений
    df['cleaning_flag'] = df['include_exclude'].apply(lambda x: 1 if x=='include' else 0)
    df = df.drop(['ad_transcribtion'], axis=1)
# приводим строки в верхний регистр, нормализуем цифры и тд.
    custom_ad_dict_int_lst = config.custom_ad_dict_int_lst
    df = normalize_funcs.normalize_columns_types(df, custom_ad_dict_int_lst)

    return df


# In[ ]:


# создаем функцию для получения Дисконтов по типам медиа
# можем передать в нее список с названиями типов медиа, по которым отфильтровать датаФрейм
# по умолчанию забираем все

def get_media_discounts(media_type='TV'):
    cols_count = [i for i in range(3)]
    # опускаем заголовки на 1-ую строку датаФрейма и передаем номера столбцов, которые нам нужны
    df = pd.read_csv(config.discounts_link, header=None, usecols=cols_count)
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df = df[df['media_type']==media_type]
    df['media_type'] = df['media_type'].str.upper()
    df = normalize_funcs.normalize_columns_types(df, ['year'], ['disc'])
    
    return df


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




