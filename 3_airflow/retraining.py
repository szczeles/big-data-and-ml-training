#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import argparse
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.pipeline import Pipeline
import requests
import datetime

spark = SparkSession.builder     .config('spark.driver.memory', '3g')     .enableHiveSupport().getOrCreate()


# In[2]:


# base_path = 'file:///data'


# In[3]:


# posts = spark.read.format('parquet').load(f'{base_path}/Posts')


# In[4]:


# posts.drop('Body').where(col('PostTypeId') ==1).write.parquet('/data/stackoverflow/Posts', mode='overwrite')


# In[6]:


parser = argparse.ArgumentParser()
parser.add_argument('--posts-path', default='/data/stackoverflow/Posts')
parser.add_argument('--date', default='2019-01-01')
parser.add_argument('--models-path', default='/models/questions_to_close')
parser.add_argument('--es', default='https://3syhz9ohpy:heysa9l1c0@bdml-9041401221.eu-central-1.bonsaisearch.net:443')
parser.add_argument('-f') # TODO remove me
args = parser.parse_args()


# In[7]:


def log(message, user_id='mariusz', level='INFO'):
    url = f'{args.es}/bdml/log'
    data = {
        '@timestamp': datetime.datetime.now().isoformat(),
        'message': message,
        'level': level,
        'user_id': user_id
    }
    print(requests.post(url, json=data).status_code)

def send_metric(name, value, user_id='mariusz'):
    url = f'{args.es}/bdml-metrics/metric'
    data = {
        '@timestamp': datetime.datetime.now().isoformat(),
        'name': name,
        'value': value,
        'user_id': user_id
    }
    print(requests.post(url, json=data).status_code)
    
log('Hello World!')


# In[8]:


posts = spark.read.parquet(args.posts_path)


# In[9]:


QUESTION_POST_TYPE = 1
questions = posts.where(col('PostTypeId') == QUESTION_POST_TYPE)


# In[10]:


questions_so_far = questions.where(col('CreationDate').cast('date') < args.date)
send_metric('questions_so_far', questions_so_far.count())


# In[11]:


input_df = questions_so_far.select(
        'Id',
    
        (col('Score')<0).alias('is_score_less_than_zero'),
        'Score',
        (size('Tags') == 1).alias('has_one_tag'),
        (size('Tags') == 2).alias('has_two_tags'),
        (size('Tags') >= 3).alias('has_more_than_two_tags'),
        (substring('Title', -1, 1) == '?').alias('ends_with_qm'),
    
        col('ClosedDate').isNotNull().alias('is_closed'),
    )


# In[12]:


assembler = VectorAssembler(inputCols=[
    'is_score_less_than_zero',
    'Score',
    'has_one_tag',
    'has_two_tags',
    'has_more_than_two_tags',
    'ends_with_qm'
], outputCol="features")

lr = LogisticRegression(labelCol="is_closed")

pipeline = Pipeline(stages=[assembler, lr])


# In[13]:


mlable_df = input_df.withColumn('is_closed', col('is_closed').cast('int'))
model = pipeline.fit(mlable_df)


# In[14]:


log(f'Current coefficients: {model.stages[-1].coefficientMatrix}')


# In[15]:


send_metric('auc', model.stages[-1].summary.areaUnderROC)


# In[16]:


# model.stages[-1].write().overwrite().save(f'{args.models_path}/{args.date}')


# In[1]:


model.write().overwrite().save(f'{args.models_path}/{args.date}')

