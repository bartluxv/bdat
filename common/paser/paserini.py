# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 2
# Author                : bart.x.lu
# Create Date           : 2023/05/22
# **************************************
import os
import re
from config import ROOT_PATH
from configparser import ConfigParser


class Paser:
    pattern = re.compile(r"jdbc:(mysql|oracle).+?(?://|@)(.+com):(\d+)(?:/|:)(\w+)")

    def paser(self, file="config/sa_uat_dbconf.ini"):
        """
         解析 ini 转化成 json
        :param file:
        :return:
        """
        file = os.path.join(ROOT_PATH, file)
        file=os.path.normcase(file)
        cf = ConfigParser()
        cf.read(file, encoding='utf-8')
        self.data = {sec: {k: v for k, v in val.items()} for sec, val in cf.items()}
        return self

    def transdata(self):
        """
        url:mysql =>'jdbc:mysql://mysql-hs-hq-daily.hszq8.com:3306/hs_hq'
        url:oracle => 'jdbc:oracle:thin:@oracle-global-ksa-bss-uat.hszq8.com:1521:hkhszq'
        :return:
        """
        data = {}
        tf = lambda x: self.pattern.findall(x)[0]
        for k, value in self.data.items():
            if k not in ("DEFAULT",):
                dbtype, host, port, database = tf(value["db_uri"])
                value["dbtype"] = dbtype
                value["host"] = host
                value["port"] = port
                value["database"] = database
                data[k] = value
        return data


if __name__ == "__main__":
    obj = Paser()
    obj.paser()
    a = obj.transdata()
    print(a)
