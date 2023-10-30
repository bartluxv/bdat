"""

"""
import json
import pandas

from business.luxu.report_mas import ReporSortAPI
from config import LightAPI as conf
from common.funcs import requestBase




class Report115API(ReporSortAPI):
    """持仓摘要 分页排序"""

    def __init__(self):
        super().__init__()
        self.path = "/statementdata/tickrs/queryStockHolding"


if __name__ == '__main__':
    obj = Report115API()
    print( "所有数据条数 =>",len(obj.rest(pageSize=50000)))

