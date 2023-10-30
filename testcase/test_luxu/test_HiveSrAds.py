# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : ads 层数据推送到 sr上的数据对比测试用例
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/8/31
# **************************************
import allure

from business.luxu.SynchronousDataCheck.adsVsSr import CheckSR
from common.funcs import testPlatform


class TestCheckSR:

    @testPlatform
    def functioncase(self, kwargs):
        obj = CheckSR(**kwargs)
        print(kwargs)
        with allure.step("1、获取数据库链接信息"):
            a = obj.check_conn()
            assert a == True, "数据库未连接上"

        with allure.step("2、获取数仓 ADS 数据"):
            data = obj.getHiveAds.values.tolist()
            assert data, "数仓 ADS 无数据"

        with allure.step("3、获取 Starrocks ADS 数据"):
            data = obj.getSrData.values.tolist()
            assert data, "Starrocks ADS 无数据"

        with allure.step("4、数仓 与 Sr  ads 层 表字段数量统计对比"):
            obj.checkCols()

    for kwargs in CheckSR.datadriven():
        name=kwargs.get("name")
        exec(f"""
@testPlatform
def test_{name}(self, kwargs={kwargs}):

    obj = CheckSR(**kwargs)
    print(kwargs)
    with allure.step("1、获取数据库链接信息"):
        a = obj.check_conn()
        assert a == True, "数据库未连接上"

    with allure.step("2、获取数仓 ADS 数据"):
        data = obj.getHiveAds.values.tolist()
        assert data, "数仓 ADS 无数据"

    with allure.step("3、获取 Starrocks ADS 数据"):
        data = obj.getSrData.values.tolist()
        assert data, "Starrocks ADS 无数据"

    with allure.step("4、数仓 与 Sr  ads 层 表字段数量统计对比"):
        obj.checkCols()
    """

             )


if __name__ == "__main__":
    pass