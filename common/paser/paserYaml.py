# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : yaml 转 json
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/9/21
# **************************************
import os
import yaml
from config import ROOT_PATH


def yamltomap(file="config/oracle.yaml"):
    """

    :param file: 文件在项目中的相对路径
    :return: dict
    """

    file = os.path.join(ROOT_PATH, file)
    with open(file) as file:
        # 加载yaml文件
        data = yaml.load(file, Loader=yaml.FullLoader)
    return data


if __name__ == "__main__":
    pass
