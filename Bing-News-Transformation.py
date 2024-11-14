#!/usr/bin/env python
# coding: utf-8

# ## Bing-News-Transformation
# 
# New notebook

# In[14]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# In[15]:


df = spark.read.option("multiline", "true").json("Files/bing-latest-news.json")
# df now is a Spark DataFrame containing JSON data from "Files/bing-latest-news.json".
display(df)


# In[16]:


from pyspark.sql.functions import explode
df_exploded = df.select(explode(df["value"]).alias("json_object"))


# In[17]:


display(df_exploded)


# In[18]:


json_list = df_exploded.toJSON().collect()


# ## Testing the JSON string list

# In[19]:


print(json_list[9])


# In[20]:


import json
news_json = json.loads(json_list[9])


# In[21]:


print(news_json['json_object']['name'])
print(news_json['json_object']['description'])
print(news_json['json_object']['category'])
print(news_json['json_object']['url'])
print(news_json['json_object']['image']['thumbnail']['contentUrl'])
print(news_json['json_object']['provider'][0]['name'])
print(news_json['json_object']['datePublished'])


# ## Processing the JSON Property List

# In[22]:


title = []
description = []
category = []
image = []
url = []
provider = []
datePublished = []

#Process each JSON object in the list
for json_str in json_list:
    try:
        #Parse the JSON string into a dictionary
        article = json.loads(json_str)

        if article['json_object'].get('category') and article['json_object'].get('image', {}).get('thumbnail', {}).get('contentUrl'):

   
            #Extract information from the disctionary
            title.append(article['json_object']['name'])
            description.append(article['json_object']['description'])
            category.append(article['json_object']['category'])
            url.append(article['json_object']['url'])
            image.append(article['json_object']['image']['thumbnail']['contentUrl'])
            provider.append(article['json_object']['provider'][0]['name'])
            datePublished.append(article['json_object']['datePublished'])

    except Exception as e:
            print(f"Error processing JSON object: {e}")


# ## Converting the List to a Dataframe

# In[23]:


from pyspark.sql.types import StructType, StructField, StringType


#Combine the Lists
data = list(zip(title,description,category,url,image,provider,datePublished))

# Define Schema
schema = StructType([
    StructField('title', StringType(), True),
    StructField('description', StringType(), True),
    StructField('category', StringType(), True),
    StructField('url', StringType(), True),
    StructField('image', StringType(), True),
    StructField('provider', StringType(), True),
    StructField('datePublished', StringType(), True)
])

# Create Dataframe
df_cleaned = spark.createDataFrame(data, schema=schema)


# In[24]:


display(df_cleaned.limit(10))


# 

# ## Processing the Date Column

# In[25]:


from pyspark.sql.functions import to_date, date_format

df_cleaned_final = df_cleaned.withColumn('datePublished',date_format(to_date('datePublished'),'dd-MMM-yyyy'))


# In[26]:


display(df_cleaned_final)


# ## Writing the Final Dataframe to the Lakehouse DB in a Delta Format
# 
# #### Implementing Incremental Load

# In[27]:


from pyspark.sql.utils import AnalysisException

try: 
    
    table_name = 'bing_lake_db.tbl_latest_news'

    df_cleaned_final.write.format('delta').saveAsTable(table_name)

except AnalysisException:

    print('Table Already Exists')

    df_cleaned_final.createOrReplaceTempView('vw_df_cleaned_final')


    spark.sql(f""" MERGE INTO {table_name} target_table
                   USING vw_df_cleaned_final source_view
                  
                   ON source_view.url = target_table.url

                   WHEN MATCHED and
                   source_view.title <> target_table.title OR
                   source_view.description <> target_table.description OR
                   source_view.category <> target_table.category OR
                   source_view.image <> target_table.image OR
                   source_view.provider <> target_table.provider OR
                   source_view.datePublished <> target_table.datePublished

                   THEN UPDATE SET *

                   WHEN NOT MATCHED THEN INSERT *
                   
                """)


# In[28]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT count(*) from bing_lake_db.tbl_latest_news

