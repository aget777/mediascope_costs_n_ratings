#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
import os
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
import pyodbc

import config
from normalize_funcs import *
from db_funcs import createDBTable, downloadTableToDB

import turbodbc
from turbodbc import connect
import gc
import sys

db_name = config.db_name
full_cleaning_link = config.full_cleaning_link


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


def update_cleanig_dict(flag='regular'):
    custom_ad_dict = config.custom_ad_dict
    
    if flag=='first':
        # создаем в БД таблицы для словарей
        create_mssql_cleanig_dicts(db_name)
        # забираем из гугл докса таблицу чистки и приводим ее в порядок
        df = get_cleaning_dict()
        # записываем в БД словарь для чистки
        downloadTableToDB(db_name, custom_ad_dict, df)
    else:
        custom_ad_dict_vars_list = config.custom_ad_dict_vars_list
        # удаляем существующий словарь
        createDBTable(db_name, custom_ad_dict, custom_ad_dict_vars_list, flag='drop')
        # забираем из гугл докса таблицу чистки и приводим ее в порядок
        df = get_cleaning_dict()
        # перезаписываем новые данные
        downloadTableToDB(db_name, custom_ad_dict, df)


# In[5]:


# update_cleanig_dict(flag='regular')


# In[ ]:




