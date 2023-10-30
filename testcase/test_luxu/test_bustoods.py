# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : bart.x.lu
# Create Date           : 2023/05/15
# **************************************
import os

import allure
import pytest

from business.luxu.SynchronousDataCheck.BusinessVsOds import CheckOds

# class TestReportPage:
#
#     @allure.feature("数据对比")
#     @allure.story("业务库与ods数据对比")
#     @pytest.mark.parametrize("kwargs", CheckOds.datadriven(['2023-10-12',]))
#     def test(self, kwargs):
#         """
#
#         :param kwargs:
#         :return:
#         """
#         obj = CheckOds(**kwargs)
#         print(kwargs)
#         with allure.step("1、获取数据库链接信息"):
#             a = obj.check_conn()
#             assert a == True, a
#
#         with allure.step("2、获取业务表数据"):
#             data = obj.getbusinessdb.values.tolist()
#             assert data, "业务库无数据"
#
#         with allure.step("3、获取 ods 层表数据"):
#             data = obj.getHiveOds.values.tolist()
#             assert data, "ods库无数据"
#
#         with allure.step("4、业务 与 ods 层 表字段数量统计对比"):
#             obj.checkCols()
from common.funcs import testPlatform, SkipError, EnvError

date = os.getenv("date")


class TestReportPage:

    @testPlatform
    def functioncase(self, kwargs):
        """

        :param kwargs:
        :return:
        """
        obj = CheckOds(**kwargs)
        print(kwargs)
        with allure.step("1、获取数据库链接信息"):
            a = obj.check_conn()
            if a == True:
                pass
            else:
                raise EnvError(a)

        with allure.step("2、获取业务表数据、获取ods 数据、比较数据"):
            dataBus = obj.getbusinessdb.values.tolist()
            dataOds = obj.getHiveOds.values.tolist()

            if (not dataBus) and (not dataOds):
                raise SkipError("业务库 与 ods 库都没有数据，跳过这条用例")

        with allure.step("3、业务 与 ods 层 表字段数量统计对比"):
            obj.checkCols()

    for kwargs in CheckOds.datadriven(date=[date]):
        name = kwargs.get("tb_name")
        exec(f"""
@testPlatform
def test_{name}(self, kwargs={kwargs}):
      
    obj = CheckOds(**kwargs)
    print(kwargs)
    with allure.step("1、获取数据库链接信息"):
        a = obj.check_conn()
        if a == True:
            pass
        else:
            raise EnvError(a)

    with allure.step("2、获取业务表数据、获取ods 数据、比较数据"):
        dataBus = obj.getbusinessdb.values.tolist()
        dataOds = obj.getHiveOds.values.tolist()

        if (not dataBus) and (not dataOds):
            raise SkipError("业务库 与 ods 库都没有数据，跳过这条用例")

    with allure.step("3、业务 与 ods 层 表字段数量统计对比"):
        obj.checkCols()
        """)


if __name__ == "__main__":
    pass
