# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : ads层数据同步到sr上的数据对比
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/8/4
# **************************************
import os
from datetime import datetime, timedelta
import pandas
from common.condb.dbconnconf import ConConfig
from common.funcs import log
from config import ROOT_PATH, Starrocks as sr, HiveUat
from pymysql.err import ProgrammingError


class CheckSR(ConConfig):
    """

    """
    check_coonect_status = {}

    def __init__(self, exchange, name, hive_db, hive_ads, hive_filter, sr_db, sr_ads, sr_filter, start_date,
                 offset: int = 0,
                 *args,
                 **kwargs):
        """

        :param exchange: 站点 sham|vbroker|tickrs|mas
        :param name: 表描述
        :param hive_db: hive ads 表名 :dbname
        :param hive_ads: hive ads 表名 :tablename
        :param hive_filter: hive 数据抽取唯独 如：ds|trade_date｜如果不传着默认查全表
        :param sr_db: sr_ads 表名 dbname
        :param sr_ads: sr_ads 表名 tablename
        :param sr_filter: sr_ads 抽取字段 如：init_date|report_date |如果不传着默认查全表
        :param start_date: 数据对比的抽取日起 YY-MM-DD
        :param offet: 数据抽取的偏移时间段,

        """
        self.exchange = exchange
        self.name = name
        self.hive_db = hive_db
        self.hive_ads = hive_ads
        self.hive_filter = hive_filter
        self.sr_db = sr_db
        self.sr_ads = sr_ads
        self.sr_filter = sr_filter
        self.start_date = start_date
        self.end_date = self.last_date(offset)

        #starrock 连接
        self.coon_sr = self.mysql(host=sr.host, port=sr.port, username=sr.user, password=sr.password, database="ads")
        #hive 连接
        self.coon_hive = self.hive(**getattr(HiveUat, exchange))

    def last_date(self, offset):
        """

        :return:
        """
        initds = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date = (initds + timedelta(days=int(offset))).strftime("%Y-%m-%d")
        log().debug(f"当前执行的结束时间 {end_date}")
        return end_date

    @staticmethod
    def datadriven(start_date: str = None, offset: int = None, file="data/checkAdsAndSr.csv"):
        """
        数据驱动
        date:抽取数据日期
        :return:
        """

        path = os.path.join(ROOT_PATH, file)
        df = pandas.read_csv(path, dtype=str)
        df.fillna('', inplace=True)
        df = df.loc[:,
             ["exchange", "name", "hive_db", "hive_ads", "hive_filter", "sr_db", "sr_ads", "sr_filter", "start_date",
              "offset"]]
        cols = df.columns
        if start_date:
            df["start_date"] = start_date
        if offset:
            df["offset"] = offset
        return [dict(zip(cols, i)) for i in df.values.tolist()]

    def check_conn(self):
        """
        检验链接
        :return:
        """
        if self.check_coonect_status.get(self.exchange):
            """如果已经验证过链接成功 则不需要重复多验证"""
            return True

        sql = "select 1 as tcol"
        try:
            bus = pandas.read_sql(sql, self.coon_sr)
            ods = pandas.read_sql(sql, self.coon_hive)

        except BaseException as e:
            raise Exception("hive 或者 starrocks 连接失败")
            __class__.check_coonect_status[self.exchange] = False
        else:
            __class__.check_coonect_status[self.exchange] = True
            return True

    @property
    def getHiveAds(self):
        """
        读取hive库
        :return:
        """
        filter_expr = f'{self.hive_filter} >= "{self.start_date}" and {self.hive_filter} <= "{self.end_date}"' if self.hive_filter else '1=1'
        sql = f"""select * from {self.hive_db}.{self.hive_ads} a where {filter_expr}"""
        log().info(f"hive ads SQL: {sql}")
        df = pandas.read_sql(sql, self.coon_hive)
        cols = df.columns
        df.columns = [str(i).replace("a.", "") for i in cols]
        log().debug(f"hive ads df: \n {df}")
        return df

    @property
    def getSrData(self):
        """
        查询 starrocks 上的数据

        :return:DataFrame
        """
        filter_expr = f'{self.sr_filter} >= "{self.start_date}" and {self.sr_filter} <= "{self.end_date}"' if self.hive_filter else '1=1'

        sql = f"""select * from {self.sr_db}.{self.sr_ads} where {filter_expr}"""
        log().info(f"sr ads SQL: {sql}")
        df = pandas.read_sql(sql, self.coon_sr)
        log().debug(f"sr ads df: \n {df}")

        return df

    def checkCols(self):
        """
        字段统计比较，忽略掉 ds,init_date,created_date,updated_date
        :return:
        """
        hive_ads_cnt = self.getHiveAds.count().to_dict()
        sr_cnt = self.getSrData.count().to_dict()
        log().info(f"【hive】{self.hive_db}.{self.hive_ads} : , {hive_ads_cnt}")
        log().info(f"【SR】{self.sr_db}.{self.sr_ads} : {sr_cnt}")
        for k, v in hive_ads_cnt.items():
            if k in ("ds", "init_date", "created_date", "updated_date"):
                continue

            try:
                sr_value = sr_cnt[k]
            except KeyError as e:
                log().error(f"字段  {k} 在 sr 表中不存在")
                raise KeyError(f"字段  {k} 在 sr 表中不存在")
            else:
                if sr_value != v:
                    log().error(f"字段 {k} 在数仓ads的数量是 {v} , 在sr表的数量是 {sr_value} ")
                    raise AssertionError(f"字段 {k} 在数仓ads的数量是 {v} , 在sr表的数量是 {sr_value} ")


if __name__ == "__main__":
    pass
