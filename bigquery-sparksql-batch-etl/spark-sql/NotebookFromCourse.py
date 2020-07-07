#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark


# In[2]:


from pyspark import SparkContext
from pyspark.sql import SQLContext


# In[3]:


from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


# In[4]:

sc = SparkContext()
spark = SQLContext(sc)


# In[5]:


flights_data = spark.read.json("gs://etl-practice/flights-data/2019-05-06.json")


# In[8]:


flights_data.registerTempTable("flights_table")


# In[10]:


spark.sql("select * from flights_table limit 10").show()


# In[12]:


query = """
        select
            flight_date,
            round(avg(arrival_delay),2) as avg_arrival_delay,
            round(avg(departure_delay),2) as avg_departure_delay,
            flight_num
        from flights_table
        group by
            flight_num,
            flight_date
"""
avg_delays_by_flight_nums = spark.sql(query)


# In[20]:


qry = """
        select 
            *,
            case
                when distance between 0 and 500 then 1
                when distance between 501 and 1000 then 2
                when distance between 1001 and 2000 then 3
                when distance between 2001 and 3000 then 4
                when distance between 3001 and 4000 then 5
                when distance between 4001 and 5000 then 6
            end distance_category
        from flights_table
"""
flights_data = spark.sql(qry)
flights_data.registerTempTable("flights_data")


# In[21]:


query = """
        select
            flight_date,
            round(avg(arrival_delay),2) as avg_arrival_delay,
            round(avg(departure_delay),2) as avg_departure_delay,
            distance_category
        from flights_data
        group by
            distance_category,
            flight_date
"""
avg_delays_by_distance_category = spark.sql(query)


# In[22]:


avg_delays_by_distance_category.show()


# In[26]:


from datetime import date
current_date = str(date.today())

flight_nums_output_path = "gs://etl-practice/flights-data-output/{0}_{1}".format(current_date,"flight_nums")
distance_category_output_path ="gs://etl-practice/flights-data-output/{0}_{1}".format(current_date,"distance_category")

avg_delays_by_flight_nums.coalesce(1).write.format("json").save(flight_nums_output_path)
avg_delays_by_distance_category.coalesce(1).write.format("json").save(distance_category_output_path)


# In[ ]:




