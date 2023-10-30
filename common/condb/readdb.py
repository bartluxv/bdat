import os
import cx_Oracle
import pandas
import pymysql
from pyhive import hive
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# from config import Starrocks as sr
import time

pymysql.install_as_MySQLdb()


def pdMySQL(sql="select xxx", db="tickrs_mas_ads"):
    """

    :param sql: 仅支持 select
    :param db:
    :return: datafeame
    """
    if not (sql.startswith("select") or sql.startswith("SELECT")):
        return "仅支持 select 语句"

    sr_fengine = f"mysql+pymysql://{sr.user}:{sr.password}@{sr.host}:{sr.port}/{db}"
    df = pandas.read_sql_query(sql, sr_fengine)
    return pandas.DataFrame(df)


def read_csv(file_path):
    """

    :param file_path:
    :return: dataframe
    """
    df = pandas.read_csv(file_path)
    return pandas.DataFrame(df)


def sparkMySQL(sql=""):
    """
    官网下载 mysql-connector-java-8.0.28.jar
    $PYTHON/site-packages/pyspark/mysql-connector-java-8.0.28.jar
    :param sql:
    :return:
    """
    spark = (SparkSession.builder.appName("test_create_data")
             .enableHiveSupport().getOrCreate()
             )
    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    spark.sql("set hive.exec.dynamic.partition = true")

    df = (spark
          .read.format("jdbc")
          .option("url", "jdbc:mysql://172.33.69.173:9030")
          .option("dbtable", "(SELECT  distinct stock_code  FROM tickrs_dwd.view_stock_holding_tickrs)  t")
          .option("user", "starrocks")
          .option("password", "starrocks")
          .load()
          )
    return df


def read_hive(sql="show tables"):
    """
    读取hive
    pip3 install thrift_sasl
    pip3 install sasl
    pip3 install thrift
    pip3 install pyhive
    :param sql:
    :return:
    """

    hiveConn = hive.connect(host='172.33.69.37', port=10000, username='hadoop', auth='NOSASL')

    cursor = hiveConn.cursor()
    cursor.execute(sql)  # 执行sql语句

    status = cursor.poll().operationState  # 得到执行语句的状态
    print(cursor.fetchall())

    for i in cursor.fetchall():
        print(i)

    cursor.close()
    hiveConn.close()
    return cursor.fetchall()


def sparkOracle(sql=""):
    """
     $PYTHON/site-packages/pyspark/jars/ojdbc.jar
    :param sql:
    :return: ksauat/hsdc8023
    """
    spark = (SparkSession.builder.appName("test_create_data")
             .enableHiveSupport().getOrCreate()
             )
    df = (spark
          .read.format("jdbc")
          .option("url", "jdbc:oracle:thin:@oracle-mas-bss-feature.hszq8.com:1521:hkhszq")
          .option("user", "ksauat")
          .option("password", "hsdc8023")
          .option("dbtable", "(select * from v$version)  t")
          .load()
          )

    return df


def pdOracle(sql=""):
    """
    1、甲骨文官网下载oracle客户端驱动 如 "instantclient_19_16"
    2、将驱动加入到环境变量
    :param sql:
    :return:
    """
    # 如果客户端驱动已加入到环境变量这步骤可以忽略
    cx_Oracle.init_oracle_client(lib_dir=r"/home/data/instantclient_19_10")
    os.environ["NLS_LANG"] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn("oracle-mas-bss-feature.hszq8.com", "1521", "hkhszq")  # (host,port,sid)
    engine = create_engine('oracle+cx_oracle://ksauat:hsdc8023@' + dns)  # (...//user:paaswor@...)
    sql = "SELECT BANNER_FULL FROM V$VERSION where 1=1"
    data = pandas.read_sql(sql, engine)
    return data


if __name__ == '__main__':
    # sql = "SELECT t.* FROM ads_bss_report_stock_holder_list_di t WHERE init_date='2023-02-02'"
    # df_table = sparkMySQL(sql)
    #
    # filepat = "/Users/luxu/Documents/.IdeaProjects/bdautomation/testcase/test_ads_bss_report_stock_holder_list_di/ads_bss_report_stock_holder_list_di.csv"
    #
    # dfcsv = read_csv(filepat)
    #
    # assert df_table.values == dfcsv.values

    # stock_code =search( """(select  t.stock_code,  row_number() over (order by t.stock_code) as rid  from ( SELECT  distinct stock_code  as stock_code  FROM tickrs_dwd.view_stock_holding_tickrs ) t )  as t0""")
    # stock_code.show(70)
    #
    #
    # df = sparkOracle()
    # a=df.toJSON()
    # df.show()

    a=pdOracle()
