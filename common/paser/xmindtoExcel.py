# *************************************************************************************************
# Scripts Name          : ximdtoExcel.py
# Program Version       : V1.00
# Description           : xmind 测试用例转化 excel ，支持导入禅道, 子主题少于5个的用例，
# Rules of use          : 1、最后一个子主题作为预期结果，倒数第二个作为步骤，倒数第三个作为前置条件，第二个子主题到倒数第四列使用/拼接作为标题；
#                         2、识别xmind中的1、2、3、4、5作为优先级P0,P1,P2,P3,P4,倒数第4个主题的优先级作为整个用例的优先级，默认P3
# Author                : bart.x.lu
# Create Date           : 2023/05/15
# **************************************

import os
import re
import xmindparser
import pandas

pattern_priority = re.compile("(?<=priority-)\d")
pattern_symbol_exclam = re.compile("symbol-exclam")
patternpplit = re.compile("(.+)@@@@@(\d)#####(.+)")
all = []  #


def recursion(data, TitlePriority=""):
    """
    2～用例标题	前置条件	步骤	预期结果
    :return:  title0+title2...titlen
    """
    # 当前层级的 title、priority
    title = str(data.get("title", "")).replace("\n", " ")
    title = title.replace("\r", " <br/> ")

    makers = "*".join(data.get("makers", []))
    prioritys = pattern_priority.findall(makers)
    priority = prioritys[0] if prioritys else "4"
    symbol_exclams = pattern_symbol_exclam.findall(makers)
    symbol_exclam = symbol_exclams[0] if symbol_exclams else "xxx"
    TitlePriority += f"{title}@@@@@{priority}#####{symbol_exclam}\n"

    if "topics" in data:
        for i in data.get("topics"):
            recursion(data=i, TitlePriority=TitlePriority)

    else:  # 不存在 topic/topics 时候终止,返回之前所有路径的 （title,priority）
        text = patternpplit.findall(TitlePriority)
        if len(text) >= 5:
            expected = text[-1][0]
            step = text[-2][0]
            Preconditions = text[-3][0]
            casetitle = "/".join([i[0] for i in text[1:-3]])
            priority = f"P{int(text[-4][1]) - 1}"
            isable = ["symbol_exclams" for i in text if i[2] == "symbol-exclam"]  # 是否禁用
            if not isable:
                all.append([casetitle, Preconditions, step, expected, priority])


def xmindtoExcel(filepath, sheet, module=""):
    """

    :param path:
    :param sheet:
    :param module:模块列指定值
    :return:
    """

    # 解析成dict数据类型
    content = xmindparser.xmind_to_dict(filepath)
    caseFileName = os.path.basename(filepath).replace(".xmind", f"_{sheet}")
    try:
        out_dict = [i for i in content if i["title"] == sheet][0]["topic"]
    except IndexError:
        sheets = [i["title"] for i in content]
        raise IndexError(f"检查你的sheet 设置的是否正确，你的sheet 页有 {sheets} \n"
              f"例如 sheet = {sheets[0]}")
    else:
        recursion(out_dict)
    df = pandas.DataFrame(all, columns=["用例标题", "前置条件", "步骤", "预期", "优先级"])
    df["所属模块"] = module
    df["关键词"] = ""
    df["用例类型"] = "手动功能测试"
    df["适用阶段"] = "特性测试阶段"
    df2 = df.loc[:, ['所属模块', '用例标题', '前置条件', '步骤', '预期', '关键词', '优先级', '用例类型', '适用阶段']]
    df2.to_csv(f"{caseFileName}.csv", index=False, )
    pandas.DataFrame(df2).to_excel(f"{caseFileName}.xlsx", index=False, sheet_name="用例库")


if __name__ == '__main__':
    # xmind 路径
    xmind_path = "/Users/luxu/Documents/caseList/sham.xmind"
    # sheet 名称
    sheet = "R10_P"
    modlue="/数据中心/sahm/报表/10ACR001(#6598)"
    filterTitle = ["需求", "提测单", "后台地址"]
    xmindtoExcel(filepath=xmind_path, sheet=sheet,module=modlue)
