#!/usr/bin/env python
# encoding: utf-8
# author: AlisaAlbert
# 2019/5/19 11:25

import pickle
import pymysql
import pandas as pd
from multiprocessing import Pool
import  time

#mysql连接
def connect_db():
    DB_Host = '***'
    DB_User = '***'
    DB_Pwd = '***'
    DB_Name = '***'
    DB_Port = 3306
    db_connect = pymysql.connect(host=DB_Host, user=DB_User, password=DB_Pwd, database=DB_Name, port=DB_Port,
                               charset='utf8')
    return db_connect

#省份-城市配置表
def get_division_list(db_connect, division_table):
    sql = 'select * from {};'.format(division_table)
    data = pd.read_sql(sql, con = db_connect)
    province_list = data['province_name'].unique().tolist()
    city_list = []
    for code in province_list:
        city = data[data['province_name'].str.contains(code)]['city_code'].unique().tolist()
        city_list.append(city)
    return city_list

#单进程读取目标表
def read_data(db_connect, target_table, code):
    start = time.time()
    sql = 'select * from {0} where city_code in ({1});'.format(target_table,"'" + "','".join(code) + "'")
    data_df = pd.read_sql(sql,con = db_connect)
    print('数据读入成功！')
    end = time.time()
    print('Task runs %0.2f seconds.' % ((end - start)))
    return data_df

if __name__ == "__main__":
    # 开启的进程数, 与逻辑核保持一致即可，普通台式机建议18，高性能工作站建议60
    target_table = 'table_name1'
    division_table = 'table_name2'
    db_connect = connect_db()
    city_list = get_division_list(db_connect, division_table)

    proc_num = 31 #进程数
    pool = Pool(processes = proc_num)
    jop_result = []
    for code in city_list:
        # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
        res = pool.apply_async(read_data, (db_connect,target_table,code,))
        jop_result.append(res)

    pool.close() #关闭进程池，防止进一步操作。如果所有操作持续挂起，它们将在工作进程终止前完成
    pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束

    #保存为pkl文件
    for index,tmp in enumerate(jop_result):
        result_path = r'path\result_' + str(index) + '.pkl'
        tmp_df = tmp.get()
        with open(result_path, 'wb') as f:
            pickle.dump(tmp_df, f)
