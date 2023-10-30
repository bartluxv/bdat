# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : bart.x.lu
# Create Date           : 2023/05/15
# **************************************
import os
import cx_Oracle
import pandas
from pyhive import hive
from sqlalchemy import create_engine
from config import HIVE, ROOT_PATH

os.environ["NLS_LANG"] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

cx_Oracle.init_oracle_client(lib_dir=r"C:\code\work\apiFrame\RM\instantclient_19_16")


class ConConfig:
    """
    数据库驱动链接
    """

    def mysql(self, host, port, database, username, password, **kwargs):
        """

        :return:
        """
        return create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

    def oracle(self, host, port, database, username, password, **kwargs):
        """

        :return:
        """
        dns = cx_Oracle.makedsn(host, port, database)  # (host,port,sid)
        return create_engine(f'oracle+cx_oracle://{username}:{password}@' + dns)  # (...//user:paaswor@...)

    def hive(self, host=HIVE.host, port=HIVE.port, username=HIVE.username):
        """
        访问hive
        :return:
        """
        return hive.connect(host=host, port=port, username=username, auth='NOSASL')

    def starrocks(self, host, port, database, username, password, **kwargs):
        """
        访问 starrocks
        :param host: 主机名
        :param port: 端口号
        :param database: 库名
        :param username: 用户名
        :param password: 密码
        :param kwargs: 其它参数 、非必填
        :return:
        """
        return create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')


if __name__ == "__main__":
    # con = ConConfig().starrocks(host="172.33.69.173", port=9030, username="starrocks", password="starrocks",
    #                             database="ads")

    conora=ConConfig().oracle(host="oracle-global-ksa-bss-uat.hszq8.com",port=1521,database="hkhszq",
                              username="ksa_oms_ro",
                              password="hsdcoms0011")
    print(conora)
    df = pandas.read_sql(
        "select 1 ",
        conora)
    print(df)
    print()
