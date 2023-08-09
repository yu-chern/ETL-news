import requests
from bs4 import BeautifulSoup
import pandas as pd
from transform import transform_data

#News api key: 5f064216a5a9434e9e1464227097fd52

def extract_data():
    url = ('https://newsapi.org/v2/everything?'
        'q=Ukraine&'
        'sources=bbc-news&'
        'from=2023-07-01&'
        'sortBy=popularity&'
        'apiKey=5f064216a5a9434e9e1464227097fd52'
      )
    sql_table_name = 'news_etl_table'
    news_csv_file = 'news_table.csv'
    news_list = []
    response = requests.get(url)
    for i,article, in enumerate(response.json()['articles']):
        news_list.append([i, article['title'], article['author'], article['publishedAt'], article['url'], article['description']])
    
    news_df = pd.DataFrame(news_list, columns=['id', 'title', 'author', 'publishedAt', 'url', 'description'])
    transform_data(news_df,news_csv_file,sql_table_name)
    return True

#extract_data()