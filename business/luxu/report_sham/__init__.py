# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/6/5
# **************************************
import functools
from pandasql import sqldf
import pandas
from common.condb.dbconnconf import ConConfig

obj = ConConfig()


def sqltoDF(func):
    """
    接收sql 返回 pandas.Dataframe
    """

    @functools.wraps(func)
    def wapper(*args, **kwargs):
        sql = func(*args, **kwargs)
        sql = str(sql).strip()
        assert sql.startswith("select") or sql.startswith("SELECT"), "非sql查询语句"
        df = pandas.read_sql(sql, obj.hive())
        return df

    return wapper


if __name__ == "__main__":
    pass
