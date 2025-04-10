#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime

import config

start_of_the_time = datetime.strptime('1990-01-01', '%Y-%m-%d') # указываем дату начала сбора данных, для преобразования номера месяца

media_type_dict = {'tv': 'tv'} # {'tv': 'tv', 'radio': 'ra'}


# In[2]:


# создаем фильтр, который будем применять во воложенном запросе для фильтрации ВСЕХ ТАБЛИЦ по ВСЕМ ИСТОЧНИКАМ
# этот фильтр используем в ТВ, Радио, ООН, Пресса
# Префикс для фильтруемой таблицы задан с запасом = t10
main_filter_str = config.main_filter_str


# In[3]:


first_row_query_dict = {
    'tv': 't1.vid, t1.cid, t1.distr, t1.mon, t1.from_mon, t1.estat, t1.cnd_cost_rub,  t1.vol, t1.cnt'
}


# In[4]:


# создаем словарь, с помощью которого будем переименовывать поля в таблицах Фактов и Справочниках
# Названия приводим к стандарту из ТВ индекс
rename_cols_dict = {
    'vid':          'adId',
    'cid':          'tvCompanyId',
    'distr':        'breaksDistributionType',
    'mon':          'mon_num',
    'cnd_cost_rub': 'ConsolidatedCostRUB',
    'cnt':          'Quantity',
    'notes':        'adNotes',
    'atid':         'ad_type_id',
    'stdur':        'adStandardDuration',
    'alid':         'advertiserListId',
    'blid':         'brandListId',
    'sblid':        'subbrandListId',
    'mlid':         'modelListId',
    'slid2':        'articleList2Id',
    'slid3':        'articleList3Id',
    'slid4':        'articleList4Id', 
    'slaid':        'adSloganAudioId',
    'slvid':        'adSloganVideoId',
}


# In[5]:


# таблица фактов по отчету TV расходы

media_tv_costs = 'media_tv_costs'

media_tv_costs_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(20)',
            'adId int',
            'researchDate nvarchar(10)',
            'breaksDistributionType char',
            'tvCompanyId smallint',
            'mon_num smallint',
            'from_mon smallint',
            'estat char',
            'ConsolidatedCostRUB bigint',
            'year smallint',
            'vol int',
            'Quantity smallint',
             'disc float', 
            'ConsolidatedCostRUB_disc float'
]

media_tv_costs_int_lst = ['adId', 'tvCompanyId', 'mon_num', 'from_mon',  'ConsolidatedCostRUB', 'year', 'vol', 'Quantity']
media_tv_costs_float_lst = ['disc', 'ConsolidatedCostRUB_disc']


# In[ ]:





# In[ ]:





# In[ ]:





# In[6]:


# dicts_lst = config.nat_tv_slices
# Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
# Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
media_dicts_costs = {
    'tv': [media_tv_costs, media_tv_costs_vars_list, media_tv_costs_int_lst, media_tv_costs_float_lst],
}


# In[7]:


# словарь с характеристиками TV Объявлений

adex_ad_dict_list_tv = 'adex_ad_dict_list_tv'

adex_ad_dict_list_tv_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(20)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'ad_type_id smallint',
            'adStandardDuration smallint',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int', 
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',           
            'adSloganAudioId int',
            'adSloganVideoId int',         
            'adFirstIssueDate nvarchar(10)',
    
    'cleaning_flag tinyint',
            'clip_type_region nvarchar(100)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'competitor nvarchar(20)',
            'category_general nvarchar(50)',
            'delivery nvarchar(30)',
            'product_category nvarchar(50)',
            ]

adex_ad_dict_list_tv_int_lst = ['adId', 'ad_type_id', 'adStandardDuration', 'advertiserListId', 'brandListId', 'subbrandListId', 
    'modelListId', 'articleList2Id', 'articleList3Id', 'articleList4Id', 'adSloganAudioId', 'adSloganVideoId', 'cleaning_flag']


# In[8]:


adex_ad_lst_dicts = {
    'tv_Ad': [adex_ad_dict_list_tv, adex_ad_dict_list_tv_vars_list, adex_ad_dict_list_tv_int_lst, 'vid'],
}


# In[ ]:




