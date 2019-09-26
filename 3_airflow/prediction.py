#!/usr/bin/env python
# coding: utf-8

# In[11]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import argparse
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.pipeline import PipelineModel
import requests
import datetime

spark = SparkSession.builder     .config('spark.driver.memory', '3g')     .enableHiveSupport().getOrCreate()


# In[2]:


parser = argparse.ArgumentParser()
parser.add_argument('--posts-path', default='/data/stackoverflow/Posts')
parser.add_argument('--date', default='2019-01-01')
parser.add_argument('--models-path', default='/models/questions_to_close')
parser.add_argument('--es', default='https://3syhz9ohpy:heysa9l1c0@bdml-9041401221.eu-central-1.bonsaisearch.net:443')
parser.add_argument('-f') # TODO remove me
args = parser.parse_args()


# In[3]:


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


# In[4]:


posts = spark.read.parquet(args.posts_path)


# In[5]:


QUESTION_POST_TYPE = 1
questions = posts.where(col('PostTypeId') == QUESTION_POST_TYPE)


# In[6]:


questions_to_classify = questions.where(col('CreationDate').cast('date') == args.date).drop('ClosedDate')
send_metric('questions_to_classify', questions_to_classify.count())


# In[7]:


input_df = questions_to_classify.select(
        'Id',
    
        (col('Score')<0).alias('is_score_less_than_zero'),
        'Score',
        (size('Tags') == 1).alias('has_one_tag'),
        (size('Tags') == 2).alias('has_two_tags'),
        (size('Tags') >= 3).alias('has_more_than_two_tags'),
        (substring('Title', -1, 1) == '?').alias('ends_with_qm')
    )


# In[12]:


model = PipelineModel.load(f'{args.models_path}/{args.date}')


# In[14]:


results = model.transform(input_df)


# In[18]:


results.select('Id', 'prediction').write.parquet(f'/results/{args.date}')


# In[19]:


#2x - send metric: ile oceniono na 0, ile oceniono na 1
data = spark.read.parquet(f'/results/{args.date}')
send_metric('suspicious_questions', data.where(col('prediction') == 1).count())
send_metric('valid_questions', data.where(col('prediction') == 0).count())
send_metric('total_questions', data.count())


# In[ ]:




