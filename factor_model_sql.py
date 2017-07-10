
# coding: utf-8

# In[1]:


#!/usr/bin/python3

# mysql  Ver 14.14 Distrib 5.7.18, for Linux (x86_64) using  EditLine wrapper
# data files are "./fff.csv" and "./data/*.csv"

'''
notes:
1. get & clean data
    tech:
        small: NASDAQ:NTGR, NYSE:PLT
        medium: NYSE:SMI
        large: NASDAQ:AAPL, NASDAQ:AMZN
    non-tech:
        small: NYSE:HRI, NYSE:DF
        medium: NASDAQ:UHAL
        larege: NYSE: BRK.A, NYSE:XOM
2. OLS with 3 basic factors
3. get google search trend
4. regress on lag; regress on different sectors of revenue
5. automate the process

x. ML training
'''
import os
import numpy as np
import pandas as pd
import matplotlib as plt
from statsmodels.formula.api import ols
# http://www.statsmodels.org/devel/generated/statsmodels.regression.linear_model.RegressionResults.html
import MySQLdb
# DROP TABLE symbols, AAPL,AMZN,BRK_A,DF,HRI,NTGR,PLT,SMI,UHAL,XOM,fff;



# In[2]:


def add_stock_to_db(symbol, csv_file_path):
    # add table of stock prices from csv file
    sql_1 = "CREATE TABLE " + symbol + " (date TEXT NOT NULL, close DECIMAL(18,4) NOT NULL)"
    cur.execute(sql_1)
    sql_2 = "LOAD data local infile " + csv_file_path + " INTO TABLE " + symbol + r""" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'  IGNORE 1 LINES (date, close)"""
    cur.execute(sql_2)
#     sql_3 = "UPDATE " + symbol + " SET date = (SELECT UNIX_TIMESTAMP(DATE(STR_TO_DATE(date, '%m/%d/%Y %H:%i')));"
    sql_3 = "UPDATE " + symbol + " SET date = DATE((SELECT STR_TO_DATE(date, '%m/%d/%Y %H:%i')));"
    cur.execute(sql_3)


def add_factors_to_db(symbol, csv_file_path):
    # add table of factors data
    sql_1 = "CREATE TABLE " + symbol + " (date TEXT NOT NULL, PREM DECIMAL(18,4) NOT NULL, SMB DECIMAL(18,4) NOT NULL, HML DECIMAL(18,4) NOT NULL, RF DECIMAL(18,4) NOT NULL)"
    cur.execute(sql_1)
    sql_2 = "LOAD data local infile " + csv_file_path + " INTO TABLE " + symbol + r""" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'  IGNORE 1 LINES (date, PREM, SMB, HML,RF)"""
    cur.execute(sql_2)
    sql_3 = "UPDATE " + symbol + " SET date = DATE((SELECT STR_TO_DATE(date, '%Y%m%d')));"
    cur.execute(sql_3)


# In[3]:


db = MySQLdb.connect(host="localhost",
                    user="steve",
                    passwd="mima",
                    db="factors")

cur = db.cursor()


# In[4]:


# stock data; 20120706-20170630

# TODO: automate the dictionary generation, import sys, recognize file name
# csv_file_list = os.listdir('data/')
# stock_list = []
# stock_file_dict = {}
# for _ in csv_file_list:
#     stock_list.append(_.split('.')[0])
#     stock_file_dict[_.split('.')[0]] = "data/" + _
    
# print(stock_list)
# print(stock_file_dict)

# create table of symbols
stock_list = [r"'AAPL'", r"'AMZN'", r"'BRK_A'", r"'DF'", r"'HRI'", r"'NTGR'", r"'PLT'", r"'SMI'", r"'UHAL'", r"'XOM'"]
cur.execute("CREATE TABLE symbols (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, symbol CHAR(6) UNIQUE NOT NULL)")

for _ in stock_list:
    sql = "INSERT INTO symbols (symbol) VALUES (" + _ + ")"
    cur.execute(sql)
    

stock_file_dict = {"AAPL" : r"'data/AAPL.csv'",
                  "AMZN" : r"'data/AMZN.csv'",
                  "BRK_A" : r"'data/BRK_A.csv'",
                  "DF" : r"'data/DF.csv'",
                  "HRI" : r"'data/HRI.csv'",
                  "NTGR" : r"'data/NTGR.csv'",
                  "PLT" : r"'data/PLT.csv'",
                  "SMI" : r"'data/SMI.csv'",
                  "UHAL" : r"'data/UHAL.csv'",
                  "XOM" : r"'data/XOM.csv'"}

for _ in stock_file_dict.keys():
    symbol = _
    csv_file_path = stock_file_dict[_]
    add_stock_to_db(symbol, csv_file_path)

# add factors data to DB
add_factors_to_db('fff', r"'fff.csv'")


# In[5]:


# import stock data from DB
df = pd.DataFrame(index=pd.read_sql("SELECT date FROM AAPL", con=db).date)

for _ in stock_file_dict.keys():
    df[_] = pd.read_sql("SELECT close FROM " + _, con=db).values

# data preprocessing
df = df.pct_change()[1:]*100
df = df[:255]


# In[6]:


# import factors data from DB
ndf = pd.DataFrame()
ndf[["PREM","SMB","HML","RF"]]=pd.read_sql("SELECT * FROM fff", con=db)[1:][["PREM","SMB","HML","RF"]]
for _ in ndf.columns:
    df[_] = ndf[_].values


# In[7]:


model_dict = {}
pvalues_dict = {}
for i in range(len(stock_list)):
    key = df.columns[i]
    model_dict[key] = ols(df.columns[i] + " ~ PREM+SMB+HML", df).fit()
    print(key, model_dict[key].pvalues[model_dict[key].pvalues < 0.05])
    print(key, model_dict[key].params)


# In[8]:


db.commit()
db.close()


# In[ ]:




