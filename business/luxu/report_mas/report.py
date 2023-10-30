"""

"""
import json
import pandas

from business.luxu.report_mas import ReporSortAPI
from config import LightAPI as conf
from common.funcs import requestBase


class Report4API(ReporSortAPI):
    """4 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: reportDate
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0004"
        self.pageSize = 100


class Report115API(ReporSortAPI):
    """115 分页排序"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0115/stockHodlingReport"


class Report114API(ReporSortAPI):
    """114 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: reportDate
        """
        super().__init__(**kwargs)
        self.pageSize = 10
        self.path = "/api/ma/financialReport/str00114"

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
        df = df.groupby("reference").count().sort_values(by="mClientCode", ascending=False)
        repeat_statistics = df[df.apply(lambda x: x["reportDate"] > 1, axis=1)]
        return repeat_statistics.values.tolist()


class Report113API(ReporSortAPI):
    """113 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: reportDate
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str00113"


class Report111APITradeDetail(ReporSortAPI):
    """111 交易明细分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  startDate,endDate
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str00111/detail"


class Report29APITradeDetail(ReporSortAPI):
    """29 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: startDate,endDate
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0029/getMutiAccountCash"


class Report31APITradeDetail(ReporSortAPI):
    """31 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: startDate,endDate,reportStyle='1'
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0031"


class Report23APITradeDetail(ReporSortAPI):
    """23 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: reportDate,reportStyle='1'
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0023"


class Report24APITradeDetail(ReporSortAPI):
    """24 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs: reportDate,reportStyle='1'
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0024"


class Report27APITradeDetail(ReporSortAPI):
    """27 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  startDate, endDate,reportStyle='1'
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str0027"


class Report18RealAPITradeDetail(ReporSortAPI):
    """18实时 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate,reportStyle='1',realtime=1
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str006/journal"


class Report18HistoryAPITradeDetail(ReporSortAPI):
    """18 历史 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str006/history/journal"


class Report97APITradeDetail(ReporSortAPI):
    """97 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  startDate,endDate,
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str043/journal"


class Report77RealAPITradeDetail(ReporSortAPI):
    """77 实时 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate,
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str023/journal"


class Report77HistoryAPITradeDetail(ReporSortAPI):
    """77 历史 分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate,
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str023/history/journal"


class Report14APITradeDetail(ReporSortAPI):
    """14  分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate,
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/str005/journal"


class Report35OrderAPITradeDetail(ReporSortAPI):
    """TDR002-35号报表获取订单  分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate,
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/tdr002/getOrder"


class Report35EntrustAPITradeDetail(ReporSortAPI):
    """TDR002-35号报表获取委托  分页排序"""

    def __init__(self, **kwargs):
        """

        :param kwargs:  balanceLogDate, orderIds[string]
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/tdr002/getEntrust"

    def setOrderIds(self):
        obj = Report35OrderAPITradeDetail(**self.kwargs)
        resdata = obj.rest()
        if len(resdata) > 0:
            orderId = [i["orderId"] for i in resdata]
            self.kwargs.update(orderIds=orderId)
        else:
            raise AssertionError(f"35号报表无当然数据 balanceLogDate =  {self.kwargs.get('balanceLogDate', None)}")


class Report35EntrustSucessAPITradeDetail(ReporSortAPI):
    """TDR002-35号报表获取成交  分页排序"""

    def __init__(self, **kwargs):
        """
        :param kwargs: orderIds[string]
        """
        super().__init__(**kwargs)
        self.path = "/api/ma/financialReport/tdr002/getSuccess"

    def setOrderIds(self):
        obj = Report35OrderAPITradeDetail(**self.kwargs)
        resdata = obj.rest()
        if len(resdata) > 0:
            orderId = [i["orderId"] for i in resdata]
            self.kwargs.update(orderIds=orderId)
        else:
            raise AssertionError(f"35号报表无当然数据 balanceLogDate =  {self.kwargs.get('balanceLogDate', None)}")


if __name__ == '__main__':
    obj = Report35OrderAPITradeDetail(balanceLogDate='20230525')
    print("所有数据条数 =>", len(obj.rest(pageSize=50000)))
    print(obj.getPageData)
