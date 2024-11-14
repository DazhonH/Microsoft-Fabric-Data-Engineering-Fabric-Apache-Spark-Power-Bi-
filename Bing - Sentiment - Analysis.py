#!/usr/bin/env python
# coding: utf-8

# ## Bing - Sentiment - Analysis
# 
# New notebook

# In[6]:


df = spark.sql("SELECT * FROM bing_lake_db.tbl_latest_news LIMIT 1000")
display(df)


# In[14]:


import synapse.ml.core
from synapse.ml.services import AnalyzeText


# In[15]:


model = (AnalyzeText()
        .setTextCol('description')
        .setKind('SentimentAnalysis')
        .setOutputCol('response')
        .setErrorCol('error'))


# In[18]:


#Apply the Model to df
result = model.transform(df)


# In[20]:


display(result)


# In[21]:


#Create Sentiment Column
from pyspark.sql.functions import col

sentiment_df = result.withColumn('Sentiment', col('response.documents.sentiment'))


# In[22]:


display(sentiment_df)


# In[23]:


sentiment_df_final = sentiment_df.drop('error','response')


# In[24]:


display(sentiment_df_final)


# In[25]:


from pyspark.sql.utils import AnalysisException

try: 
    
    table_name = 'bing_lake_db.tbl_sentiment_analysis'

    sentiment_df_final.write.format('delta').saveAsTable(table_name)

except AnalysisException:

    print('Table Already Exists')

    sentiment_df_final.createOrReplaceTempView('vw_sentiment_df_final')


    spark.sql(f""" MERGE INTO {table_name} target_table
                   USING vw_sentiment_df_final source_view
                  
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

