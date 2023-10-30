# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/10/18
# **************************************
import xmind
import os
import re
import xmindparser
import pandas

pattern_priority = re.compile("(?<=priority-)\d")
pattern_symbol_exclam = re.compile("symbol-exclam")
patternpplit = re.compile("(.+)@@@@@(\d)#####(.+)")
alldata = []  #


class TransXmind:

    def __init__(self, ):
        """

        """
        self.filepath = ""
        self.sheet = ""
        self.sheets = []
        self.alldata = []
        self.module = ""

    @property
    def getXmindFile(self):
        filelist = [i for i in os.listdir("./") if str(i).endswith("xmind")]
        return filelist

    @property
    def getSheet(self):
        content = xmindparser.xmind_to_dict(self.filepath)
        return [i["title"] for i in content]

    def load_xmind(self):
        """

        :return:
        """
        content = xmindparser.xmind_to_dict(self.filepath)
        print([i["title"] for i in content])
        try:
            out_dict = [i for i in content if i["title"] == self.sheet][0]["topic"]

        except IndexError:
            self.sheets = [i["title"] for i in content]
            raise IndexError(f"检查你的sheet 设置的是否正确，你的sheet 页有 {self.sheets} \n"
                             f"例如 sheet = {self.sheets[0]}")
        else:
            self.recursion(out_dict)

    def recursion(self, data, TitlePriority=""):

        """

        :return:
        """

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
                self.recursion(data=i, TitlePriority=TitlePriority)

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
                    self.alldata.append([casetitle, Preconditions, step, expected, priority])

    def to_csvExcel(self):
        """

        :return:
        """
        df = pandas.DataFrame(self.alldata, columns=["用例标题", "前置条件", "步骤", "预期", "优先级"])
        print(df)
        df["所属模块"] = self.module
        df["关键词"] = ""
        df["用例类型"] = "手动功能测试"
        df["适用阶段"] = "特性测试阶段"
        df2 = df.loc[:, ['所属模块', '用例标题', '前置条件', '步骤', '预期', '关键词', '优先级', '用例类型', '适用阶段']]
        caseFileName = os.path.basename(self.filepath).replace(".xmind", f"_{self.sheet}")
        df2.to_csv(f"{caseFileName}.csv", index=False, )
        pandas.DataFrame(df2).to_excel(f"{caseFileName}.xlsx", index=False, sheet_name="用例库")

    def to_xmind(self):
        """
        生成可以导入 飞书用例库的 xmind
        :param data:
        :return:
        """
        # 创建 xmind 文件
        # 1、如果指定的XMind文件存在，则加载，否则创建一个新的
        workbook = xmind.load("tofeishu.xmind")
        # 2、获取第一个画布（Sheet），默认新建一个XMind文件时，自动创建一个空白的画布
        sheet1 = workbook.getPrimarySheet()

        sheet1.setTitle("用例集")  # 设置画布名称
        root_topic1 = sheet1.getRootTopic()
        root_topic1.setTitle("用例集(不会读取该节点)")  # 设置主题名称

        wordcount = {}
        for i in self.alldata:
            # 用例标题
            casetitle = i[0]
            if casetitle in wordcount:
                wordcount[casetitle] += 1
            else:
                wordcount[casetitle] = 1
            new_casetitle = f"{wordcount[casetitle]} : {casetitle}" if wordcount[casetitle] > 1 else casetitle
            sub_topic_casetitle = root_topic1.addSubTopic()
            sub_topic_casetitle.setTitle(new_casetitle)
            print(f"title:  {new_casetitle}")

            # 前置条件
            sub_topic_Preconditions = sub_topic_casetitle.addSubTopic()
            sub_topic_Preconditions.setTitle(i[1])
            print(f"前置:  {i[1]}")

            # 步骤
            sub_topic_step = sub_topic_Preconditions.addSubTopic()
            sub_topic_step.setTitle(i[2])
            print(f"步骤:  {i[2]}")

            # 预期结果
            sub_topic_expected = sub_topic_step.addSubTopic()
            sub_topic_expected.setTitle(i[3])
            print(f"预期结果:  {i[3]}")
        xmind.save(workbook, path='tofeishu.xmind')


if __name__ == "__main__":
    # 用例 xmind 路径【必填】
    xmind_path = "/Users/luxu/Documents/caseList/sham.xmind"
    # sheet 名称 【必填】
    sheet = "R10_P"


    ######################
    obj = TransXmind()
    obj.filepath = xmind_path
    obj.sheet = sheet
    obj.load_xmind()
    obj.to_xmind()
