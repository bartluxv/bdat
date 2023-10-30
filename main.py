import os
import sys
import pytest
from config import ROOT_PATH as ROOT_DIR
from datetime import date

taskName = input("设置本次执行任务的任务名称【每次任务名称请不要重复】\n:")

if len(sys.argv) > 1:
    date = sys.argv[1]
    os.environ["date"] = date
    print(os.getenv("date"))
else:
    date = input("设置当前执行任务的时间 Excutedate 【'YYYY-MM_DD'】 \n:")
    os.environ['date'] = date
    print("date:    ",os.environ.get('date', "未设置当前任务的时间"))

    last_trade_date = input("date 对应的 last_trade_date 【'YYYY-MM_DD'】,若不设置 则等于上一步设置的 date \n:")
    os.environ['last_trade_date'] = last_trade_date if last_trade_date else date
    print("last_trade_date: ",os.environ.get('last_trade_date', "未设置当前任务的时间"))

os.environ['taskName'] = f"{taskName}_{date}"
ReportTaskName = f"{taskName}_{date}"
print(f"本次报告的任务名称为：【{ReportTaskName}】")

# allure_result = f"{ROOT_DIR}report/"  # 存放allure结果数据
# allure_report = f"{ROOT_DIR}report/"
#
# # command = f"""pytest.main(['-s','---alluredir={allure_result}'])"""
# # os.system(command)
#
# "-s --alluredir=./report/allure_files --html=./report/test.html --self-contained-html"
pytest.main()
