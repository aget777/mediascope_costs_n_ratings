#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gspread
from oauth2client.service_account import ServiceAccountCredentials

import config
from db_funcs import get_mssql_table



# In[2]:


# функция для того, чтобы создать подключение к Гугл докс
def create_connection(service_file):
    client = None
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_file, scope
        )
        client = gspread.authorize(credentials)
        print("Connection established successfully...")
    except Exception as e:
        print(e)
    return client


# In[3]:


# функция для загрузки данных в гугл таблицу
def export_dataframe_to_google_sheet(worksheet, df):
    try:
        worksheet.update(
            [df.columns.values.tolist()] + df.values.tolist(),
            
        )
        print("DataFrame exported successfully...")
    except Exception as e:
        print(e)


# In[9]:


# с помощью этой функции делаем выгрузку новых объявлений на отдельный лист 
# эту функцию запускаем в самом конце после обновлений всех таблиц
# она затирает все данные, которые были на листе и записывает заново только новые объявления

def append_ads_to_google():
    # делаем запрос к БД, чтобы получить новые объявления
    query = f"select *  from {config.nat_tv_ad_dict} where cleaning_flag=2"
    nat_tv_new_ad_dict_df = get_mssql_table(config.db_name, query=query)
    # создаем подключение к гуглу
    client = create_connection(config.service)
    # прописываем доступы к документу, в который будем вносить запись
    sh = client.open_by_url(config.google_docs_link)
    sh.share(config.gmail, perm_type='user', role='writer')
    google_sheet = sh.worksheet(config.new_ads_sheet)
    # очищаем лист
    google_sheet.clear()
    # записываем новые данные
    export_dataframe_to_google_sheet(google_sheet, nat_tv_new_ad_dict_df)


# In[ ]:





# In[ ]:





# In[12]:





# In[ ]:




