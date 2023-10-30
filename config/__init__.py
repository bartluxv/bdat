import re
from datetime import date

ROOT_PATH = re.compile(".+?(?=config)").search(str(__file__)).group()

current_date = str(date.today())


class ReportPlatm:
    url = "http://10.73.11.54:9999/api/bigdata/case/report/"
    header = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}


class LogSettings:
    """

    """
    loglevel = 'info'
    logpath = f'report/task'
    tmplog = 'report/tmp.log'
    module = 'console++'
    name = 'testat'


class Starrocks:
    """
    starrocks 连接信息
    """
    host = "172.33.69.173"
    user = "starrocks"
    password = "starrocks"
    port = 9030


class StarrocksFeature:
    """
    startrocks feature 连接信息
    """
    host = "172.33.69.17"
    user = "starrocks"
    password = "starrocks"
    port = 9030

class Server:
    """
    远程运行调度、spark写数据地址
    """
    host = "127.0.0.1"
    port = 9999


class MasLightAPI:
    """马来生产接口配置信息"""
    url = "http://one-service-devops-mas-uat.mbkr.com.my/lightApi"
    api_key = "hs_bss"
    api_secret = "MoqH0EpQos"
    header = {"Content-Type": "application/json"}


class MasUATLightAPI:
    """马来daily接口配置信息"""
    url = "http://one-service-devops-daily.hszq8.com/lightApi"
    api_key = "hs_bss"
    api_secret = "123456"
    header = {"Content-Type": "application/json"}


class ShamFeatureLightAPI:
    """马来daily接口配置信息"""
    url = "http://one-service-devops-feature.hszq8.com/lightApi/api"
    api_key = "hs_bss"
    api_secret = "123456"
    header = {"Content-Type": "application/json"}


class LightAPI(MasUATLightAPI):
    """"""


class HIVE:
    """
    hive 信息
    """
    host = "172.33.69.37"
    username = "hadoop"
    port = 10000


class HiveUat:
    """
    所有的数仓 hive uat环境 地址
    sham|vbroker|tickrs|mas
    {"host":str,"username":str,"port":int}
    """
    sham = {"host": "172.33.69.134", "username": "hadoop", "port": 10000}
    vbroker = {"host": "172.33.69.212", "username": "hadoop", "port": 10000}
    tickrs = {"host": "172.33.69.37", "username": "hadoop", "port": 10000}
    mas = {"host": "172.33.69.37", "username": "hadoop", "port": 10000}
