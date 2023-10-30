"""

"""
import json
import pandas
from config import LightAPI as conf
from common.funcs import requestBase


class ReporSortAPI:
    """分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: 接口请求自定义参数
        """
        self.path = "/api/ma/financialReport/str00114"
        self.pageSize = kwargs.get("pageSize", 10)
        self.allpageSzie = kwargs.get("allpageSzie", 50000)
        self.kwargs = kwargs

    def rest(self, pageNo=1, pageSize=50000):
        """
        请求模版,默认获取全部数据
        :param pageNo:
        :param pageSize:
        :return:
        """
        path = self.path
        data = {
            # "reportDate": self.reportDate,  # 结单日期   2022-11-11 2022-11
            # "startDate": self.startDate,  # 结单日期   2022-11-11 2022-11
            # "endDate": self.endDate,
            # "market":"",
            # "currency":"",
            # "stockCode":"",
            # "clientOrgFlag":"",
            # "MclientCode": "610334984",
            "pageNo": pageNo,
            "pageSize": pageSize,
        }
        kwargs = {k: v for k, v in self.kwargs.items() if k not in ("pageSize", "allpageSzie")}
        data.update(**kwargs)
        resp = requestBase(method="post", apiParh=path, data=data, conf=conf)
        print(resp.text)
        rdata = json.loads(resp.text)["data"]

        # 无数据时候返回 end
        if len(rdata) == 0:
            return False
        return rdata

    @property
    def getPageData(self):
        """
        分页获取全部数据
        :return:
        """
        alldata = []
        for page in range(1, 10000):
            a = self.rest(page, pageSize=self.pageSize)
            if a == False:
                break
            elif isinstance(a, list):
                alldata += a
        return alldata

    def getPageDataCount(self):
        """
        分页获取数据,统计重复的个数

        :return:
        """
        try:
            data = self.getPageData
        except Exception as e:
            return "接口请求数据错误"
        else:
            df = pandas.DataFrame(data, dtype=str)
        repeat_statistics = df.groupby("reference").count().sort_values(by="mClientCode", ascending=False)
        return repeat_statistics

    @property
    def getAllDataDeduplication(self):
        """获取全部数据 行统计"""
        try:
            data = self.rest(pageSize=self.allpageSzie)
        except Exception as e:
            return "接口请求数据错误"
        else:
            df = pandas.DataFrame(data, dtype=str)
            dfdata = df.values.tolist()
            ldata = ["_".join([str(y) for y in i]) for i in dfdata]
            row_cnt = pandas.DataFrame(ldata, columns=["allcol"]).groupby("allcol").count().to_dict()
            return row_cnt

    @property
    def getPageDataDeduplication(self):
        """获取分页数据 行统计"""
        try:
            data = self.getPageData
        except Exception as e:
            return "接口请求数据错误"
        else:
            df = pandas.DataFrame(data, dtype=str)
            dfdata = df.values.tolist()
            ldata = ["_".join([str(y) for y in i]) for i in dfdata]
            row_cnt = pandas.DataFrame(ldata, columns=["allcol"]).groupby("allcol").count().to_dict()
            return row_cnt


if __name__ == '__main__':
    pass
