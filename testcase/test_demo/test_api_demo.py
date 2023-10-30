# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/7/28
# **************************************
import allure

from business.APITestDemo.script_demo import Sham_API_Demo


class TestDemo:

    @allure.feature("沙特可疑报表")
    @allure.story("所有数据")
    def test_4Report(self,):
        """
        :return:
        """
        obj = Sham_API_Demo()
        with allure.step("1、执行数据对比"):
            obj.assertDataRows()



if __name__ == "__main__":
    pass
