# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : dws 层发散
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/9/21
# **************************************
import os
from datetime import datetime, timedelta
import pandas
from common.condb.dbconnconf import ConConfig
from common.funcs import log
from config import ROOT_PATH, HiveUat


class CheckMasODS:
    """
    针对特殊情况的 dws 表，主表的数据对比
    例如 1，主表有多个表之间 union all
        2、dws 层表之间使用了 内连接的
        3、sham 市场的成交纠正
        4、主表做了聚合操作的

    发放命名规范 以 db__tablbe
    """

    def tickrs_dws__dws_mas_ast_client_stock_inf_di(self, coon, start_date, end_date):
        """
        :param coon: hive 连接信息
        :param start_date:  查询ods 的开始时间，选用
        :param  end_date:   查询ods 的结束时间，选用

        针对  tickrs_dws__mas_ast_client_stock_inf_di 的处理
        主表 ：
                tickrs_ods.ods_mal_settle_us_security_holding_pat_di
                tickrs_ods.ods_mal_settle_security_holding_pat_di

        :return:
        """
        filter = f""
        sql = """"""


class CheckDWS(ConConfig, CheckMasODS):
    """

    """
    check_coonect_status = {}

    def __init__(self, exchange, name, dws_db, dws_table, dws_filter, ods_db, ods_table, ods_filter, start_date,
                 agg_columns,
                 offset=0,
                 *args,
                 **kwargs):
        """

        :param exchange: 站点 sham|vbroker|tickrs|mas
        :param name: 表描述
        :param dws_db: hive dws 表名 :dbname
        :param dws_table: hive dws 表名 :tablename
        :param dws_filter: dws 数据抽取唯独 如：ds|trade_date｜如果不传着默认查全表
        :param ods_db: dws 的主表 的dbname
        :param ods_table: dws ods层的主表，若有多个则以 英文逗号分割
        :param ods_filter: dws ods层的主表的 的抽取数据的字段。如：ds|trade_date｜如果不传着默认查全表
        :param agg_columns: dws 的聚合字段 "ds,client_id"
        :param start_date: 数据对比的抽取日起 YY-MM-DD
        :param offet: 数据抽取的偏移时间段,

        """
        self.exchange = exchange
        self.name = name
        self.dws_db = dws_db
        self.dws_table = dws_table
        self.dws_filter = dws_filter
        self.ods_db = ods_db
        self.ods_table = ods_table
        self.ods_filter = ods_filter
        self.start_date = start_date
        self.end_date = self.last_date(offset)
        self.agg_columns = agg_columns

        # hive 连接
        self.coon_hive = self.hive(**getattr(HiveUat, exchange))

    def last_date(self, offset):
        """

        :return:
        """
        initds = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date = (initds + timedelta(days=int(offset))).strftime("%Y-%m-%d")
        log().debug(f"当前执行的结束时间 {end_date}")
        return end_date

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
        filter_expr = f'{self.dws_filter} >= "{self.start_date}" and {self.dws_filter} <= "{self.end_date}"' if self.dws_filter else '1=1'
        sql = f"""select * from {self.dws_db}.{self.dws_filter} a where {filter_expr}"""

        log().info(f"hive dws SQL: {sql}")

        df = pandas.read_sql(sql, self.coon_hive)
        cols = df.columns
        df.columns = [str(i).replace("a.", "") for i in cols]
        log().debug(f"hive dws df: \n {df}")
        return df

    def chekcDwsDif(self):
        """
        验证 dws 是否发散，根据传入的发散字段来确定
        :return: 唯一列出现发散的数据，按key 行数统计
        """
        agg_columns = str(self.agg_columns).split(",")
        dws = self.getHiveAds.groupby(agg_columns).size().reset_index(name="count")

        # 数据量>1的 数据
        dfdws = dws[dws['cnt'] > 1]
        df = pandas.merge(dfdws, self.getHiveAds, on=agg_columns)
        log().error(f"dws 扩散的数据：{df}")
        return len(dfdws)

    @property
    def dwscnt(self):
        """
        汇总层 dws 的行数
        :return:
        """
        return self.getHiveAds.count().max()

    def odscnt(self):
        """
        主表 ods 的行数
        特殊情况的ods 会单独处理
        :return:
        """
        funcods = getattr(self, self.dws_table)

        if funcods:
            # 单独处理特殊场景的ods
            return funcods(start_date=self.start_date, end_date=self.end_date)
        else:  # 通用模式处理
            filter_expr = f'ds >= "{self.start_date}" and ds <= "{self.end_date}"' if self.ods_filter else '1=1'
            sql = f"""select * from {self.ods_db}.{self.ods_table} a where {filter_expr}"""
            log().info(f"hive ods SQL: {sql}")
            df = pandas.read_sql(sql, self.coon_hive)
            return df.count().max()


if __name__ == "__main__":
    pass
    obj=CheckDWS(
        exchange="mas"
    )
