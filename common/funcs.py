"""

"""
from functools import wraps
import time
import json
import logging
import os.path
from datetime import datetime
import requests
from requests import request

from config import LogSettings, ROOT_PATH, ReportPlatm

requests.packages.urllib3.disable_warnings()


class SkipError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"SkipError: {self.message}"


class EnvError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"EnvError: {self.message}"


def requestBase(method, apiParh, data, conf, **kwargs):
    """

    :param method: 请求方式、post、get...
    :param apiParh: 接口路径
    :param data: 接口请求参赛
    :param conf: 接口请求信息包含 url、header、密钥【api_key、api_secret】
    :return: resp
    """

    url = f"{conf.url}{apiParh}"
    data["api_key"] = conf.api_key
    data["api_secret"] = conf.api_secret
    header = conf.header
    print(url)
    print(data)
    try:
        resp = request(method, url, json=data, headers=header, verify=False, **kwargs)
    except Exception as e:
        raise AssertionError(f'接口url = {url} 请求失败:  {e} ')
    else:
        return resp


def w(func):
    def execu(*args, **kwargs):
        return func(*args, **kwargs)

    return execu()


def log():
    level = LogSettings.loglevel
    logoutput = LogSettings.module
    name = os.environ.get('nowCaseName', LogSettings.name)
    logger = logging.Logger.manager.getLogger(name)
    # logger = logging.getLogger(name)

    logFilePath = os.path.join(ROOT_PATH, f"{LogSettings.logpath}_{os.environ.get('taskName', 'debug')}.log")
    tmpFile = os.path.join(ROOT_PATH, LogSettings.tmplog)
    if not logger.handlers:
        # 设置日志显示级别
        if level == "info":
            logger.setLevel(logging.INFO)
        elif level == "debug":
            logger.setLevel(logging.DEBUG)
        elif level == "warning":
            logger.setLevel(logging.WARNING)
        elif level == "error":
            logger.setLevel(logging.ERROR)
        else:
            logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] [%(module)s.%(funcName)s:%(lineno)d] - %(message)s')

        fh = logging.FileHandler(logFilePath, encoding="utf-8")
        tmp = logging.FileHandler(tmpFile, encoding="utf-8")
        ch = logging.StreamHandler()

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        tmp.setFormatter(formatter)

        # log_file_handler = TimedRotatingFileHandler(filename=logpath, when="M", interval=1, backupCount=1)
        # log_file_handler.suffix = "%Y-%m-%d_%H-%M.userlog"
        # log_file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}.userlog$")
        # logger.addHandler(log_file_handler)

        # 设置日志输出位置
        if logoutput == "file":
            logger.addHandler(fh)
        elif logoutput == "console":
            logger.addHandler(ch)
        else:
            logger.addHandler(fh)
            logger.addHandler(ch)
        logger.addHandler(tmp)

    return logger


def clearTmpLog():
    with open(os.path.join(ROOT_PATH, LogSettings.tmplog), mode="w") as fp:
        fp.write("")


def readTmpLog():
    with open(os.path.join(ROOT_PATH, LogSettings.tmplog), encoding='utf-8') as fp:
        value = fp.read()
    return value


def testPlatform(func):
    """
    测试结构传入测试平台
    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):

        # if kwargs:
        #
        #     """有数据驱动时"""
        #     funcNameParam = f"{str(func.__name__)}_{kwargs['kwargs'].get('name', '_')}"
        #     if funcNameParam.endswith("_"):
        #         """数据驱动的模版不执行"""
        #         return
        # else:
        #     """无数据驱动时"""
        #     funcNameParam = f"{str(func.__name__)}"
        # funcNameParam = funcNameParam.replace("test_", "")
        funcNameParam = str(func.__name__)
        if funcNameParam == "functioncase":
            """模版不执行"""
            return None

        start_time = time.time()  # 用例执行开始时间
        clearTmpLog()
        os.environ['nowCaseName'] = funcNameParam

        try:
            res = func(*args, **kwargs)
        except AssertionError as e:
            status = 2  # 失败
            log().exception(e)
        except SkipError as e:
            status = 4  # 跳过
            log().exception(e)
        except EnvError as e:
            status = 5  # 其它
            log().exception(e)
        except BaseException as e:
            status = 3  # 异常
            log().exception(e)
        else:
            status = 1  # 成功
        finally:
            ## 清空临时日志文件
            text = readTmpLog()
            clearTmpLog()
        end_time = time.time()  # 用例执行结束时间
        data = {
            "taskName": os.environ.get('taskName', "debug"),
            "caseName": str(func.__name__).replace("test_", ""),
            "time": round(end_time - start_time, 4),
            "status": status,
            "execTime": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "text": text
        }
        log().debug(data)
        data = json.dumps(data)
        resp = requests.post(url=ReportPlatm.url, json=data, data=data, headers=ReportPlatm.header, verify=False)
        log().debug(resp.url)
        log().debug(resp.text)

    return wrapper


if __name__ == '__main__':
    # log().error(111)
    # @test
    pass
