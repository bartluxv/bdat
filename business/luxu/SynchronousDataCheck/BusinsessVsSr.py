# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 业务数据库与 sr ods 数据量对比
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/9/21
# **************************************
from datetime import datetime, timedelta

import os
from config import ROOT_PATH, Starrocks, StarrocksFeature, HiveUat
import pandas
from common.condb.dbconnconf import ConConfig
from common.funcs import log
from common.paser.paserYaml import yamltomap
from config import ROOT_PATH


class CheckSrOds(ConConfig):
    """

    """
    dblink = yamltomap()

    def __init__(self, db_type, db_conn, db_name, tb_name, sr_db_ods, sr_tb_ods, start_date, env="uat", offset: int = 0,
                 exchange="tickrs", *args,
                 **kwargs):
        """

        :param db_type: 数据库类型 mysql、oracle
        :param db_conn: 数据库链接名称
        :param db_name: 业务库名
        :param tb_name: 业务表名
        :param sr_db_ods: sr_ods 库名
        :param sr_tb_ods: sr_ods 表名
        :param start_date: 查询的开时间
        :param exchange: 市场 broker,tickrs,mas,sham
        :param env: 环境 feature、uat
        :param offset: 结束时间与开始时间的差值

        """
        self.db_type = db_type
        self.db_conn = db_conn
        self.db_name = db_name
        self.tb_name = tb_name
        self.sr_db_ods = sr_db_ods
        self.sr_tb_ods = sr_tb_ods
        self.start_date = start_date
        self.end_date = self.last_date(offset)
        self.exchange = exchange

        # starrocks 连接
        sr = Starrocks if env == "uat" else StarrocksFeature
        self.coon_sr = self.mysql(host=sr.host, port=sr.port, username=sr.user, password=sr.password,
                                  database="ads")
        # oracle 连接
        self.coon_ora = self.oracle(**self.dblink["mas"][env]["hkuat"])

    def last_date(self, offset):
        """

        :return:
        """
        initds = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date = (initds + timedelta(days=int(offset))).strftime("%Y-%m-%d")
        log().debug(f"当前执行的结束时间 {end_date}")
        return end_date

    @staticmethod
    def datadriven(date: list):
        """
        数据驱动
        date:抽取数据日期
        :return:
        """
        dfdate = pandas.DataFrame(date, columns=["date"])

        path = os.path.join(ROOT_PATH, "data/sourcedatacheckods.csv", )
        df = pandas.read_csv(path, index_col='index', dtype=str)
        df.fillna('', inplace=True)
        df = df.loc[df['db_type'] == 'mysql', ["db_type", "db_conn", "db_name", "tb_name", "hive_db_ods", "hive_tb_ods",
                                               "ext_condition"]]
        df["on"] = 1
        dfdate["on"] = 1
        dfl = pandas.merge(left=df, right=dfdate, on='on')
        cols = dfl.columns
        return [dict(zip(cols, i)) for i in dfl.values.tolist()]



    def check_conn(self):
        """"""
        sql = "select 1 as tcol"
        try:
            bus = pandas.read_sql(sql, self.coon_sr)
            ods = pandas.read_sql(sql, self.coon_ora)
        except Exception as e:
            return e
        else:
            return bus.to_dict() == ods.to_dict()

    @property
    def getbusinessdb(self):
        """
        查询业务库

        :return:DataFrame
        """
        filter = f"to_char(CREATE_TIME_UTC,'YYYY-MM-DD') >='{self.start_date}' and to_char(CREATE_TIME_UTC,'YYYY-MM-DD') <='{self.end_date}' "
        sql = f"""select * from {self.db_name}.{self.tb_name} where {filter}"""
        log().debug(f"oracle  SQL： {sql}")
        df = pandas.read_sql(sql, self.coon_ora, )
        return df

    @property
    def getSrOds(self):
        """
        读取hive库
        :return:
        """
        filter = f"to_char(CREATE_TIME_UTC,'YYYY-MM-DD') >='{self.start_date}' and to_char(CREATE_TIME_UTC,'YYYY-MM-DD') <='{self.end_date}' "

        sql = f"""select * from {self.sr_db_ods}.{self.sr_tb_ods} a where {filter}"""
        log().debug(f"starrocks SQL： {sql}")
        df = pandas.read_sql(sql, self.coon_sr)
        cols = df.columns
        df.columns = [str(i).replace("a.", "") for i in cols]
        return df

    def checkCols(self):
        """
        字段统计比较
        :return:
        """
        bus_cnt = self.getbusinessdb.count().to_dict()
        ods_cnt = self.getSrOds.count().to_dict()
        print("bus_cnt", bus_cnt)
        print("ods_cnt", ods_cnt)
        for k, v in bus_cnt.items():
            try:
                ods_value = ods_cnt[k]
                assert v == ods_cnt[k]
            except AssertionError:
                raise AssertionError(f" 字段 {k} 在业务库的数量是 {v} ,ods 在表的数量是 {ods_value} ")
            except KeyError as e:
                raise KeyError(f"字段  {k} 在 SR ods 表中不存在")


if __name__ == "__main__":
    pass
