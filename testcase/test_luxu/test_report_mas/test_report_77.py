import allure
import pytest
from data.pagesortdata import pageSize, reportDate
from business.luxu.report_mas.report import Report77RealAPITradeDetail,Report77HistoryAPITradeDetail

kwargs = [{"pageSize": p, "balanceLogDate": r, "reportStyle": "1"} for p in pageSize for r in reportDate]


class TestReportPage:

    @allure.feature("77号报表")
    @allure.story("实时分页排序")
    @pytest.mark.parametrize("kwargs", kwargs)
    def test_Real_Report(self, kwargs):
        """

        :return:
        """

        obj = Report77RealAPITradeDetail(**kwargs)
        print(kwargs)
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

    @allure.feature("77号报表")
    @allure.story("历史分页排序")
    @pytest.mark.parametrize("kwargs", kwargs)
    def test_History_Report(self, kwargs):
        """

        :return:
        """

        obj = Report77HistoryAPITradeDetail(**kwargs)
        print(kwargs)
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
