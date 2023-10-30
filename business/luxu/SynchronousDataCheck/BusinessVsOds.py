# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 上游数据与ods 比较
# Rules of use          : 
# Author                : bart.x.lu
# Create Date           : 2023/05/15
# **************************************
import os
import re
import pandas
from common.condb.dbconnconf import ConConfig
from common.paser.paserini import Paser
from config import ROOT_PATH, HiveUat
from common.funcs import log


class CheckOds(ConConfig):
    """

    """
    dbconf = Paser().paser().transdata()

    def __init__(self, db_type, db_conn, db_name, tb_name, hive_db_ods, hive_tb_ods, ext_condition, date, *args,
                 **kwargs):
        """

        :param db_type: 数据库类型 mysql、oracle
        :param db_conn: 数据库链接名称
        :param db_name: 业务库名
        :param tb_name: 业务表名
        :param hive_db_ods: hive_ods 库名
        :param hive_tb_ods: hive_ods 表名
        :param ext_condition: 抽取条件 BALANCE_LOG_DATE='{{ ds_nodash }}' |NAN |date_format(update_time,'%Y-%m-%d')='{{ ds }}'
        :param date : 数据校验参数 YYYY-MM-DD
        """
        self.db_type = db_type
        self.db_conn = db_conn
        self.db_name = db_name
        self.tb_name = tb_name
        self.hive_db_ods = hive_db_ods
        self.hive_tb_ods = hive_tb_ods
        self.ext_condition = ext_condition
        self.date = date
        self.last_trade_date = os.getenv("last_trade_date")

        self.hive_coon = self.hive(**HiveUat.sham)
        for k, v in kwargs.items():
            setattr(self, k, v)

    @staticmethod
    def datadriven(date: list):
        """
        数据驱动
        date:抽取数据日期
        :return:
        """
        dfdate = pandas.DataFrame(date, columns=["date"])

        path = os.path.join(ROOT_PATH, "data/airflow_sahm_test_sys_sync_meta.csv", )
        df = pandas.read_csv(path, index_col='index', dtype=str)
        df.fillna('', inplace=True)
        df = df.loc[:, ["db_type", "db_conn", "db_name", "tb_name", "hive_db_ods", "hive_tb_ods",
                        "ext_condition", "is_get_last_trade_date"]]
        df["on"] = 1
        dfdate["on"] = 1
        dfl = pandas.merge(left=df, right=dfdate, on='on')
        cols = dfl.columns
        return [dict(zip(cols, i)) for i in dfl.values.tolist()]

    @property
    def conn(self):
        """创建业务库链接信息"""
        func = getattr(self, self.db_type)
        conf = self.dbconf.get(self.db_conn, None)
        if not conf:
            log().error(f" 未配置 db_conn: {self.db_conn} 信息")
        return func(**conf)

    def check_conn(self):
        """"""
        sql = "select 1 as tcol"
        try:
            if self.db_type == "mysql":
                bus = pandas.read_sql(sql, self.conn)
            ods = pandas.read_sql(sql, self.hive_coon)
        except Exception as e:
            log().error(f"业务表连接失败： {self.db_conn}")
            return e
        else:
            if self.db_type == "mysql":
                return bus.to_dict() == ods.to_dict()
            else:
                return True

    @property
    def getbusinessdb(self):
        """
        查询业务库

        :return:DataFrame
        """
        self.ext_condition = self.ext_condition.replace("\n", " ")
        dateInt = str(self.date).replace("-", "")
        last_trade_dateInt = str(self.last_trade_date).replace("-", "")

        # 是否需要根据 creared_time or create_time_utc 额外过滤 默认不需要
        self.table_created_time = False
        def_columns = {"create_time_utc", "created_time", "create_time", "created_date", "create_date"}
        if "=" in self.ext_condition:
            "增量抽取"
            other_filter = re.compile(r"{{ ds }}").sub(self.date, self.ext_condition)
            other_filter = re.compile(r"{{ ds_nodash }}").sub(dateInt, other_filter)


        elif str(getattr(self, "is_get_last_trade_date", None)) == "1":
            """抽取当天，与最后一个营业日"""
            other_filter = re.compile(r"{{ ds_nodash }}").sub(dateInt, self.ext_condition, count=2)
            other_filter = re.compile(r"\${last_trade_date}").sub(last_trade_dateInt, other_filter)

        else:
            """快照表 全量抽"""
            other_filter = f" 1=1 "
            self.table_created_time = True

        other_filter = other_filter.replace("\n", "  ")
        sql = f"""select * from {self.db_name}.{self.tb_name} where {other_filter}"""
        log().info(f"业务数据库查询 SQL :{sql}")
        df = pandas.read_sql(sql, self.conn)

        if self.table_created_time:  # 针对快照再次做过滤

            colsumns = set(df.columns)
            log().info(f"业务库表的字段： {colsumns}")
            filter_col = def_columns.intersection(colsumns).pop()
            log().info(
                f"快照表  SQL:\n select * from {self.db_name}.{self.tb_name}  where {filter_col} <= {self.date}    根据落库时间进一步过滤")
            self.filter_col = filter_col
            df[filter_col] = pandas.to_datetime(df[filter_col])
            df[filter_col] = df[filter_col].dt.strftime('%Y-%m-%d')
            df = df.loc[df[filter_col] <= self.date,]

        log().debug(f"业务库数据 {df}")
        return df

    @property
    def getHiveOds(self):
        """
        读取hive库
        :return:
        """
        sql = f"""select * from {self.hive_db_ods}.{self.hive_tb_ods} a where ds= '{self.date}'"""
        log().info(f"ods 数据库查询 SQL :{sql}")
        df = pandas.read_sql(sql, self.hive_coon)
        cols = df.columns
        df.columns = [str(i).replace("a.", "") for i in cols]
        log().info(f"数仓表字段： {cols}")

        # 针对快照再次做过滤
        if self.table_created_time:
            filter_col = self.filter_col
            df[filter_col] = pandas.to_datetime(df[filter_col])
            df[filter_col] = df[filter_col].dt.strftime('%Y-%m-%d')
            df = df.loc[df[filter_col] <= self.date,]

        log().debug(f"hive ods 数据 {df}")
        return df

    def checkCols(self):
        """
        字段统计比较
        :return:
        """
        bus_data = self.getbusinessdb.values.tolist()
        ods_data = self.getHiveOds.values.tolist()
        log().debug(f"bus_cnt, {bus_data}")
        log().debug(f"ods_cnt, {ods_data}")


        cnt_bus = len(bus_data)
        cnt_ods = len(ods_data)
        if cnt_bus == cnt_ods:
            log().info("业务库、ods 表数据量相等")
        else:
            log().error(f"业务库、ods 表数据量不相等 {self.tb_name} : 数据量是 {cnt_bus};"
                        f"{self.hive_tb_ods} : 数据量是 {cnt_ods}")
            raise AssertionError(f"业务库、ods 表数据量不相等 {self.tb_name} : 数据量是 {cnt_bus};"
                                 f"{self.hive_tb_ods} : 数据量是 {cnt_ods}")


if __name__ == "__main__":
    obj = CheckOds(**{'db_type': 'mysql', 'db_conn': 'hs_us_otc_ro', 'db_name': 'hs_us_otc', 'tb_name': 'hq_day_data',
                      'hive_db_ods': 'sahm_ods', 'hive_tb_ods': 'ods_ksa_us_otc_hq_day_data_di',
                      'ext_condition': "date_format(kline_date,'%Y-%m-%d')='{{ ds }}'", 'on': 1, 'date': '2023-05-22'}
                   )

    # w = obj.getbusinessdb
    # print(w.values.tolist())
    obj.checkCols()

    # print(obj.datadriven(['2023-01-01']))
