# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/7/19
# **************************************
import json
import pandas
from common.funcs import requestBase
from config import Starrocks as sr
from config import ShamFeatureLightAPI as conf
from common.condb.dbconnconf import ConConfig


class Sham_API_Demo(ConConfig):
    """
    沙特可疑报表
    """

    def __init__(self):
        """

        """
        self.path = "/sa/financialReport/suspicious/journal"
        self.sql = """select count(*) from sahm_feature_ads.ads_crm_suspicious_transactions_data_di where curr_date ='2023-04-23' """
        self.data = {'api_key': 'hs_bss',
                     'api_secret': '123456',
                     'beginDate': '20230423',
                     'endDate': '20230423',
                     'exchangeType': 'P',
                     # 'clientId': '829999993',
                     # 'clientName': 'ccc_819999993',
                     # 'stockCode': '222222',
                     # 'exceptionType': 'C',
                     # 'bizType': 1,
                     # 'orderBy': '',
                     'pageNo': 1,
                     'pageSize': 2000}

    @property
    def getAPIData(self):
        """
        获取接口返回数据
        :return:
        """
        return requestBase(method="post", apiParh=self.path, data=self.data, conf=conf)

    @property
    def getStarrocksData(self):
        """
        获取starocks 中的数据
        sql:数据库中查询语句
        :return:
        """

        coon = self.starrocks(host=sr.host, port=sr.port, username=sr.user, password=sr.password, database='')
        return pandas.read_sql(self.sql, coon)

    def assertDataRows(self):
        """
        数据库与接口数据量对比
        :return:
        """
        api_data = json.loads(self.getAPIData.text, strict=False)
        api_rows = len(api_data['data'])

        db_data = self.getStarrocksData.values.tolist()
        cnt_rows = db_data[0][0]
        print(api_rows)
        print(cnt_rows)
        assert api_rows == cnt_rows, "接口返回行数，与数据库返回行数不一致"


if __name__ == "__main__":
    obj = Sham_API_Demo()

    obj.assertDataRows()
