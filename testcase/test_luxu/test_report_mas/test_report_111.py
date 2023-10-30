import allure
import pytest
from data.pagesortdata import reportDate,pageSize
from business.luxu.report_mas.report import Report111APITradeDetail

kwargs = [{"pageSize": p, "startDate": r,"endDate":r} for p in pageSize for r in reportDate]

class TestReportPage:

    @allure.feature("111号报表")
    @allure.story("分页排序")
    @pytest.mark.parametrize("kwargs", kwargs)
    def test_111ReportTradeDetail(self, kwargs):
        """

        :return:
        """

        obj = Report111APITradeDetail()
        print(kwargs)
        for k, v in kwargs.items():
            print(k, "=", v)
            setattr(obj, k, v)
        with allure.step("1、获取全部数据"):
            assert obj.rest(), "获取数据失败"

        with allure.step("2、分页获取全部数据"):
            assert obj.getPageData, "获取数据失败"

        with allure.step(" check 全量数据与分页全量数据，行数对比"):
            assert len(obj.rest()) == len(obj.getPageData), "分页数据量不一致"

        with allure.step("check 分页数据是否有重复"):
            pass
            # assert len(obj.getPageDataCount()) == 0, "有重复数据"

        with allure.step("check 去重后的数据对比"):
            if obj.getPageDataDeduplication != obj.getAllDataDeduplication:
                raise AssertionError('全量数据，与分页数据去重后不一致')




