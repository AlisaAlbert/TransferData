#!/usr/bin/env python
# encoding: utf-8
# author: AlisaAlbert
# 2019/5/19 12:26

import pandas as pd
import numpy as np
import pickle
import time,os
from multiprocessing import Pool
import pymysql
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 100)

# 数据库连接函数
def connect_db():
    DB_Host = '***'
    DB_User = '***'
    DB_Pwd = '***'
    DB_Name = '***'
    DB_Port = 3306
    db_connect = pymysql.connect(host=DB_Host, user=DB_User, password=DB_Pwd, database=DB_Name, port=DB_Port,
                               charset='utf8')
    return db_connect

#单进程读取文件夹中的单份文件
def read_data(path):
    start = time.time()
    with open(path, 'rb') as f:
        filename = pickle.load(f)
    end = time.time()
    print('Task runs %0.2f seconds.' % ((end - start)))
    return filename

#向数据库插入数据
def insert_data(db_connect, result, table):
    cursor = db_connect.cursor()
    #转换数据格式，插入数据库
    static_result_df1 = np.array(result).tolist()
    static_result_df2 = list(map(tuple, static_result_df1))

    sql_truncate = "truncate {};".format(table)
    sql_insert = '''
    insert into {}
        (columns_name
    ) values 
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''.format(table)

    try:
        # 执行sql语句
        cursor.execute(sql_truncate)
        cursor.executemany(sql_insert, static_result_df2)
        # 执行sql语句
        cursor.commit()
        print("Done Task!")
    except:
        # 发生错误时回滚
        cursor.rollback()
    cursor.close()



if __name__=='__main__':
    #开启进程，与逻辑核保持一致
    connect_db = connect_db()
    filepath = r'D:\filename'
    table = 'table_name'

    t1 = time.time()
    pro_num = 10 #进程数
    pool = Pool(processes = pro_num)
    job_result = []
    #遍历文件夹读取所有文件
    for file in os.listdir(filepath):
        filename = filepath + '\\' + file
        res = pool.apply_async(read_data, (filename,))
        job_result.append(res)

    pool.close() #关闭进程池
    pool.join()

    #合并所有读取的文件
    get_result = pd.DataFrame()
    for tmp in job_result:
        get_result = get_result.append(tmp.get())
    t2 = time.time()

    insert_data(connect_db, get_result, table)
    print('It took a total of %0.2f seconds.' % (t2 - t1))