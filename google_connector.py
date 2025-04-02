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


# In[1]:


# с помощью этой функции делаем выгрузку новых объявлений на отдельный лист 
# эту функцию запускаем в самом конце после обновлений всех таблиц
# она затирает все данные, которые были на листе и записывает заново только новые объявления

def append_ads_to_google():
    # делаем запрос к БД, чтобы получить новые объявления
    query = """
    select 
        t1.media_type, t1.media_key_id, t1.adId, t1.adName, t1.adNotes, t1.adFirstIssueDate,
        t1.advertiserListId, t2.advertiserListName, t1.brandListId, t3.brandListName,
        t1.subbrandListId, t4.subbrandListName, t1.modelListId, t5.modelListName,
        t1.articleList2Id, t6.articleList2Name, t1.articleList3Id, t7.articleList3Name,
        t1.articleList4Id, t8.articleList4Name, t1.adSloganAudioId, t9.adSloganAudioName,
        t1.adSloganVideoId, t10.adSloganVideoName
    from 
        (select * from nat_tv_ad_dict
        where cleaning_flag=2) t1 
            left join tv_index_advertiser_list_dict t2
            on t1.advertiserListId=t2.advertiserListId
            left join tv_index_brand_list_dict t3
            on t1.brandListId=t3.brandListId
            left join tv_index_subbrand_list_dict t4
            on t1.subbrandListId=t4.subbrandListId
            left join tv_index_model_list_dict t5
            on t1.modelListId=t5.modelListId
            left join tv_index_article_list2_dict t6
            on t1.articleList2Id=t6.articleList2Id
            left join tv_index_article_list3_dict t7
            on t1.articleList3Id=t7.articleList3Id
            left join tv_index_article_list4_dict t8
            on t1.articleList4Id=t8.articleList4Id
            left join tv_index_audio_slogan_dict t9
            on t1.adSloganAudioId=t9.adSloganAudioId
            left join tv_index_video_slogan_dict t10
            on t1.adSloganVideoId=t10.adSloganVideoId
    """
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


# In[5]:


# append_ads_to_google()


# In[ ]:





# In[ ]:





# In[ ]:




