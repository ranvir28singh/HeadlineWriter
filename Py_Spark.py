
# coding: utf-8

# In[29]:

import pyspark
import pandas as pd
import numpy as np


# In[30]:

from pyspark import SparkContext, SparkConf
sc=SparkContext.getOrCreate()


# In[31]:

import re, string

desc_file = sc.textFile('E:/Rutgers/Semester3/Deep-learning/Project/Visualizations/test.txt')


# In[32]:

punc = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'


# In[33]:

def uni_to_clean_str(x):
    converted = x.encode('utf-8')
    lowercased_str = converted.lower()
    # for more difficult cases use re.split(' A|B')
    lowercased_str = lowercased_str.replace('--',' ')
    clean_str = lowercased_str.translate(None, punc) #Change 1
    return clean_str


# In[34]:

t_RDD = desc_file.flatMap(lambda x: uni_to_clean_str(x).split())
t_RDD = t_RDD.map(lambda x: (x,1))
t_RDD = t_RDD.reduceByKey(lambda x,y: x + y)


# In[35]:

t_RDD = desc_file.map(lambda x: uni_to_clean_str(x))


# In[36]:

t_RDD = desc_file.flatMap(lambda x: uni_to_clean_str(x).split())
t_RDD = t_RDD.map(lambda x: (x,1))


# In[37]:

t_RDD = desc_file.flatMap(lambda x: uni_to_clean_str(x).split())
t_RDD = t_RDD.map(lambda x: (x,1))
t_RDD = t_RDD.reduceByKey(lambda x,y: x + y)


# In[39]:

t_RDD = desc_file.flatMap(lambda x: uni_to_clean_str(x).split())
t_RDD = t_RDD.map(lambda x: (x,1)) 
t_RDD = t_RDD.reduceByKey(lambda x,y: x + y)
t_RDD = t_RDD.map(lambda x:(x[1],x[0])) 
list1=t_RDD.sortByKey(False).take(50)


# In[ ]:

df1= pd.DataFrame(list1, columns = list("t"))
d1['t'].apply(pd.Series)
df1.columns = ['Word', 'Count']
df1.to_csv("./worcloud.csv")


# In[23]:

with open('E://Rutgers//Semester3//Deep-learning//Project//Visualizations//sample-1M.jsonl', 'rb') as f:
    w1=Counter(item['source']  for item in json_lines.reader(f) if item['media-type']=='News')


# In[24]:

df=pd.DataFrame(dict(w1),index=['Count'])


# In[ ]:

News_top=df.transpose().sort(['Count'],ascending=False)[:20]


# In[ ]:

News_top['News Source']=News_top.index


# In[ ]:

News_top.plot('News Source', 'Count', kind ='bar',color='b')
plt.show()

