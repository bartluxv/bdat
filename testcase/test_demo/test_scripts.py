import allure
import pytest



class TestDemo:
    tohive_data = [
        "data/ads_bss_report_stock_holder_list_di.csv",
    ]

    task = [{"table": "tickrs.ads_bss_report_stock_holder_list_di", "date": "2023-03-01", "active": "spark"}]

    @allure.feature("6号表")
    @allure.story("金额")
    @pytest.mark.skip()
    def test_01_xinyao(self):
        """
        主流程 平台
        :return:
        """
        allure.story("业务库与ods数据对比11111111111")

        print('妖媚0')
        assert False


    @pytest.mark.parametrize("name", [{'data0':"32"}, 'data1', 'data2'])
    @allure.feature("6号表")
    @allure.story("持仓")
    def test_02_xinyao(self, name):
        """场景一"""
        print(f'妖媚2   {name}')
        assert True

    @allure.feature("6号表")
    @allure.story("收市价")
    def test_03_xinyao(self):
        """场景二"""
        print('妖媚3')
        assert True


    @allure.epic("sham")
    @allure.feature("6号表")
    @allure.story("状态")
    @allure.title("000001")
    def test_04_xinyao(self):
        """
        场景三
        :return:
        """
        with allure.step("创建数据到 ods111"):
            x = 1
            assert True
        with allure.step("运行调度 dwd"):
            assert x
        with allure.step("运行调度 dws"):
            x -= 1
            assert x
        with allure.step("运行调度 ads"):
            assert True
        with allure.step("同步数据 starrocks"):
            raise AssertionError('同步数据失败')
        with allure.step("数据校验"):
            assert False
