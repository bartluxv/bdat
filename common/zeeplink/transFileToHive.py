"""
默认 note:  "AutoTest"

"""

import pandas
import decimal
import json
from datetime import datetime

from requests import request


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)


class ZeepLinAPI:
    """
    zeppelin api
    """

    def __init__(self, **kwargs):
        """
        zeepelin 配置信息，以及基础信息
        :param kwargs:
        header
        host    zeppelin 访问地址
        port    zeppelin 端口
        notename
        noteId
        paragraphId
        code:运行在 zeppelin 上的pyspark 代码
        user：zeepelin 登陆用户
        password：zeeplin 登陆密码
        """
        # cookie = "JSESSIONID=bf08b653-70a8-4906-80fa-96551dfa1463"
        #
        # self.header = {"Cookie": kwargs.get("cookie", cookie)}
        self.host = kwargs.get("host", "172.33.69.37")
        self.port = kwargs.get("port", "8890")
        self.notename = kwargs.get("notename", "AutoTest")
        self.noteId = kwargs.get("noteId", "")
        self.paragraphId = kwargs.get("paragraphId", "")
        self.code = kwargs.get("code", "%pyspark\n.......")
        self.user = kwargs.get("user", "bart.x.lu")
        self.password = kwargs.get("password", "shen.....")
        self.url = f"http://{self.host}:{self.port}/#/"

    def __requests(self, method, path, **kwargs):
        """

        :param method: get | post
        :param path: /api/notebook/job/2HR2BTKXR"
        :param kwargs:
        :return:
        """
        url = f"http://{self.host}:{self.port}{path}"
        try:
            res = request(method, url, headers=self.header, **kwargs)
            text = res.text
            print(text)
            resdata = json.loads(text, parse_float=decimal.Decimal)

        except Exception as e:
            return text
        else:
            return resdata

    @property
    def header(self):
        """
        用户名密码设置 cookie
        :return:
        """
        url = f"http://{self.host}:{self.port}/api/login"
        data = {
            "userName": self.user,
            "password": self.password
        }
        resp = request(method="post", url=url, data=data)
        cookie = "=".join(resp.cookies.items()[0])
        return {"Cookie": cookie}

    def createNote(self):
        """
        新建 note,并缓存其noteId
        :return:
        """
        data = {
            "name": self.notename,
        }
        try:
            res = self.__requests(method="post", path="/api/notebook", json=data)
            self.noteId = res["body"]
        except Exception as e:
            raise Exception(f"{e} ==> 创建note 或 获取返会的noteId 失败， 请登陆 {self.url} 检查是否已经存在 note :{self.notename}")

    def getNoteList(self):
        """
        获取 note 信息
        本次无须使用
        :return:
        """

    def reomveNote(self):
        """
        删除 note
        :return:
        """
        path = f"/api/notebook/{self.noteId}"
        try:
            res = self.__requests(method="delete", path=path, )
            assert res["status"] == "OK", f"删除 noteId = {self.noteId} 失败"
        except Exception as e:
            print(f"{e} \n 删除 noteId 失败")

    def createParagraph(self):
        """
        新建 Paragraph 并缓存其 ParagraphId
        :return:
        """
        path = f"/api/notebook/{self.noteId}/paragraph"
        data = {
            "title": "Paragraph insert revised",
            "text": f"""{self.code}""",
            "index": 0
        }
        try:
            res = self.__requests(method="post", path=path, json=data)
            self.paragraphId = res["body"]
        except Exception as e:
            print(f"{e} \n 创建 paragraph 或 获取返会的paragraphId 失败")

    def removeParagraph(self):
        """
        删除 Paragraph
        :return:
        """
        path = f"/api/notebook/{self.noteId}/paragraph/{self.paragraphId}"
        try:
            res = self.__requests(method="delete", path=path)
            assert res["status"] == "OK", f"删除 paragraph = {self.paragraphId} 失败"
        except Exception as e:
            print(f"{e} \n 创建 paragraph 或 获取返会的noteId 失败")

    def runParagraph(self, ):
        """
        同步运行 paragraph
        :return:
        """
        path = f"/api/notebook/run/{self.noteId}/{self.paragraphId}"

        try:
            res = self.__requests(method="post", path=path)
            assert res["status"] == "OK"
        except Exception as e:
            print("运行 段落失败", res)

    def asyncParagraph(self):
        """
        异步运行 paragraph
        :return:
        """


class TransFile:
    """

    """

    def transmation(self, file: str, startindex: int, startcol: int, sheet: str):
        """
        会使控制替换成 ''
        :param file: xlsx ，abspath
        :param startindex: 读取数据的开始行
        :param startcol:   读取数据的开始列
        :param sheet: sheetname
        :return:
        """
        if not (str(file).endswith("xlsm") or str(file).endswith("xlsx")):
            raise FileNotFoundError("仅支持 xlsm 、 xlsx 文件")

        df = pandas.read_excel(file, sheet_name=sheet, dtype=str)
        df = pandas.DataFrame(df)
        df.fillna('', inplace=True)  # null值替换成 ''

        func = lambda x: x.split(".")[1] if '.' in x else x  # 清洗列
        if startindex == 0:
            cols = df.columns
        elif startindex >= 1:
            data = df.iloc[startindex - 1:, startcol:]  # 截取有效数据
            cols = list(data.iloc[0, :, ])  # 获取列名
            df = pandas.DataFrame(data.iloc[1:, ])  # 数据去除第一行行名
        else:
            raise Exception("startindex 应为整数")

        columns = [func(col) for col in cols]
        df.columns = columns
        self.df = df
        return df

    def dftoJsonStr(self):
        """
        df -> json dumps
        :return:
        """

        value = list(pandas.DataFrame(self.df).values)
        value.insert(0, self.df.columns)
        value = [[i for i in ind] for ind in value]
        self.value = json.dumps(value, cls=DateEncoder)
        return self.value

    def createSparkTemplate(self, table="", mode="append"):
        """
        生成 zeepplin pyspark note 模版
        :param table: 表名
        :param mode: 插入模式 overwrite ｜ append
        :return:
        """
        assert mode != "overwrite" or mode != "append" "错误的参数 只能 overwrite ｜ append"
        create_template = """%pyspark
import sys
import json
from pyspark.sql import SparkSession

target_table = str('{table}')
mode=str(f'{mode}')     # overwrite or append
data = {data}
# spark = (SparkSession.builder.appName(f"test_"+target_table)
#          .enableHiveSupport().getOrCreate())
spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
spark.sql("set hive.exec.dynamic.partition = true")

col=data[0]
data=data[1:]
df = spark.createDataFrame(data).toDF(*col)
df.show()
df.write.format("hive").mode(mode).insertInto(target_table)
"""
        code = create_template.format(table=table, data=self.value, mode=mode)

        return code

    def searchSparkTemlate(self):
        pass

    def rename_defaultName(self, df, mapper={"colnum": "value"}):
        """
        修改字段默认名称
        kv=re.compile("([a-zA-z0-9_]+).+default (.*?) ").findall(desc)
        dict([(i[0],eval(i[1])) for i in kv])
        :param df:
        :param mapper:
        :return:
        """
        for colnum, value in mapper.items():
            try:
                t = df[colnum]
            except:
                df[colnum] = value
            else:
                t.fillna(value=value, inplace=True)
                df[colnum] = t
        return df


def toHive(file=None, sheet=None, startindex=0, startcol=0, target_table=None, host=None, port=8890, user="",
           password="", notename="testc"):
    """

    :param file: 待读取待excel文件
    :param sheet: excel sheet 页
    :param startindex: 读取excel sheet 页的起始行 默认为 0
    :param startcol:读取 excel 页的起始列 默认为 0
    :param target_table: 写入hive 的目标表
    :param host: zeppelin 的地址，必填
    :param port: zeppelin 的端口
    :param user: 登陆 zeppelin 的用户名
    :param password: 登陆 zeppelin 的密码
    :param notename: 在zeppelin 上临时的笔记文件，不重名即可
    :return:
    """
    obj = TransFile()
    obj.transmation(file, startindex, startcol, sheet)
    obj.dftoJsonStr()
    code = obj.createSparkTemplate(table=target_table)  # 生存pyspark job 代码

    job = ZeepLinAPI(code=code, host=host, port=port, user=user, password=password, notename=notename)
    job.createNote()         # 创建note
    job.createParagraph()    # 创建段落
    job.runParagraph()       # 执行段落
    job.removeParagraph()    # 删除
    job.reomveNote()         # 删除note


if __name__ == '__main__':

    host = "172.33.69.37"  # zeppelin 地址
    port = 8890  # zeppelin 端口
    user = "bart.x.lu"  # zeppelin 登陆用户名
    password = "shxxxxx"  # zeppelin 登陆密码
    notename = "notename"  # 请设置一个不存在的notename，防止与他人冲突
    target_table = "tickrs_ods.ods_sgs_settle_trade_view_for_app_di"  # 待写入的hive 表名
    file = '/Users/luxu/Desktop/ods_sgs_settle_trade_view_for_app_di.xlsx'  # 待读取待excel文件
    sheet = "Result 1"  # excel sheet 页

    sheets = [
        {"target_table": "tickrs_ods.ods_sgs_settle_trade_view_for_app_di",
         "sheet": "ods_sgs_settle_trade_view_for_app_di",

         },
        {"target_table": "tickrs_ods.ods_sgs_settle_trade_view_for_app_di",
         "sheet": "ods_sgs_settle_trade_view_for_app_di",

         },
    ]

    for shee in sheets:
        target_table = shee["target_table"]
        sheet = shee["sheet"]

        a = TransFile()
        a.transmation(file, 0, 0, sheet=sheet)
        a.dftoJsonStr()
        code = a.createSparkTemplate(table=target_table)

        obj = ZeepLinAPI(code=code,
                         host=host,
                         port=port,
                         # notename=notename
                         user=user,
                         password=password
                         )
        print(obj.header)
        obj.createNote()  # 创建note
        obj.createParagraph()  # 创建段落
        obj.runParagraph()  # 执行段落
        obj.removeParagraph()  # 删除
        obj.reomveNote()  # 删除note
