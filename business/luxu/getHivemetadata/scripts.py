# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/7/4
# **************************************
import pandas
from pyhive import hive


class getMeta():

    def __init__(self, db="sahm_ods", table="ods_ksa_settle_all_security_success_entrust_di"):
        """

        """
        self.db = db
        self.table = table
        self.host = '172.33.69.37'
        self.databases = ["sahm_ods", "sahm_dwd", "sahm_dws", "sahm_ads"]
        self.coon = hive.connect(host=self.host, port=10000, username="hadoop", auth='NOSASL')
        self.paserTableMetaData()

    def paserTableMetaData(self):
        """

        :param table:
        :return:
        """
        df = pandas.read_sql(f"desc {self.db}.{self.table}", self.coon)
        data = list(zip(*df.values.tolist()))
        self.df = pandas.DataFrame(data)
        self.df["tablename"] = self.table
        col = list(self.df.columns)
        col2 = [col[-1]] + col[:-1]
        self.df = self.df[col2]
        self.to_excel()

    def to_excel(self):
        """
        sheet_name: ods.table
        :return:
        """
        pandas.DataFrame(self.df).to_csv(f'{self.db}.csv', index=False, mode="a", encoding='utf-8')


if __name__ == "__main__":
    obj = getMeta("sahm_ods", "ods_ksa_settle_all_security_success_entrust_di")
    obj2 = getMeta("sahm_ods", "ods_ksa_act_member_invitation_df")
    getMeta("sahm_ods", "ods_ksa_act_member_invitation_df")
    getMeta("sahm_ods", "ods_ksa_act_member_invitation_df")
    getMeta("sahm_ods", "ods_ksa_act_member_invitation_df")
