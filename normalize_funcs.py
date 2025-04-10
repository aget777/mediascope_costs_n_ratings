#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

import config
from create_dicts import get_media_discounts

np.object = np.object_


# In[2]:


# функция для нормализации данных - приводим в нижний регистр, заполняем пропуски и округляем до 2-х знаков после запятой
# принимает на вход:
# - датаФрейм
# - список из названий полей с типом данных Int (по умолчанию пустой список)
# - список из названий полей с типом данных Float (по умолчанию пустой список)

def normalize_columns_types(df, int_lst=list(), float_lst=list()):
    varchar_lst = list(df.columns) #df.loc[:,df.dtypes==np.object].columns # Через всторенный метод находим поля с текстовыми данными
    varchar_lst = list(set(varchar_lst) - set(int_lst) - set(float_lst)) # исключаем из списка с текстовыми данными поля Int и Float
    df[varchar_lst] = df[varchar_lst].apply(lambda x: x.astype('str').str.upper().str.strip())

    
    
    # Обрабатываем поля с типом данных Int
    df[int_lst] = df[int_lst].fillna('0')
    df[int_lst] = df[int_lst].apply(lambda x: x.astype('str').str.replace('\xa0', '').str.replace(',', '.').str.replace(' ', ''))
    df[int_lst] = df[int_lst].apply(lambda x: x.astype('float64').astype('int64'))
    # Обрабатываем поля с типом данных Float
    df[float_lst] = df[float_lst].fillna('0.0')
    df[float_lst] = df[float_lst].apply(lambda x: x.astype('str').str.replace('\xa0', '').str.replace(',', '.').str.replace(' ', '').str.replace('р.', ''))
    df[float_lst] = df[float_lst].apply(lambda x: x.astype('float64').round(2))
    
# возвращаем нормализованный датаФрейм
    return df


# In[ ]:





# In[3]:


# функция для добавления флага чистки
# на вход принимает основной датаФрейм
# и сокращенный датаФрейм из гугл диска, в котором оставили только ИД объявления и флаг чистки
# если объявления нет в гуглдоксе, то ставим флаг 2
# report simple/buying - в зависимости от типа отчета используем разную логику
# для отчета simple - добавляем флаг чистки из БД
# для отчета buying добавляем дисконт к расходам по году и типу медиа
def append_custom_columns(df, report, nat_tv_ad_dict=None, media_type='TV'):
    media_type = media_type.upper()
    # добавляем поле с типом мелиа - TV
    # создаем спец ключ для объединения со справочником чистка объявлений
    df['media_type'] = media_type.upper()
    df['media_key_id'] = df['media_type'] + '_' + df['adId'].astype('str')
    
    if report.lower()!='simple':
        # забираем таблицу Медиа дисконтов
        media_discounts = get_media_discounts(media_type)
        
    # забираем Год из даты отчета, чтобы добавить дисконт
        df['year'] = df['researchDate'].str.slice(0, 4)
        df['year'] = df['year'].astype('int64')
        
        df = df.merge(media_discounts, how='left', left_on=['media_type', 'year'], right_on=['media_type', 'year'])
        df['ConsolidatedCostRUB_disc'] = df['ConsolidatedCostRUB'] * df['disc']
        
        
    if report.lower()=='simple':
        # чтобы соеденить со словарем приводим adId к типу int64
        # df['adId'] = df['adId'].astype('int64')
        # добавляем флаг чистки в датаФрейм
        df = df.merge(nat_tv_ad_dict, how='left', left_on=['media_key_id'], right_on=['media_key_id'])
        # удаляем лишнее поле
        # df = df.drop(columns=['ad_id'])
        # ставим флаг чистки = 2 для ИД новый неочищенных объявлений
        df['cleaning_flag'] = df['cleaning_flag'].fillna(2)
        df = df.fillna('')
        # Добавляем размер дисконта по Типу медиа и Году
    
    
    return df

