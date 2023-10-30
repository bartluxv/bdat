# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/8/10
# **************************************
from common.zeeplink.transFileToHive import TransFile
from common.zeeplink.transFileToHive import ZeepLinAPI
from common.zeeplink.transFileToHive import toHive

host = "172.33.69.212"  # zeppelin 地址
port = 8890  # zeppelin 端口
user = "bart.x.lu"  # zeppelin 登陆用户名
password = "shenXY8066"  # zeppelin 登陆密码
notename = "notename"  # 请设置一个不存在的notename，防止与他人冲突
target_table = "ads.ads_app_client_real_profit_label_di"  # 待写入的hive 表名
file = '/Users/luxu/Desktop/lable.xlsx'  # 待读取待excel文件
sheet = "Sheet"  # excel sheet 页

a = TransFile()
a.transmation(file, 0, 0, sheet=sheet)
a.dftoJsonStr()
code = a.createSparkTemplate(table=target_table)
print(code)

obj = ZeepLinAPI(code=code,
                 host=host,
                 port=port,
                 # notename=notename
                 user=user,
                 password=password
                 )
print(obj.header)
obj.createNote()  # 创建note
obj.createParagraph()  # 创建段落
obj.runParagraph()  # 执行段落
obj.removeParagraph()  # 删除
obj.reomveNote()  # 删除note

if __name__ == "__main__":
    pass


toHive(file="ods_ksa_act_member_invitation_df",
           sheet="ods",
           startindex=0,
           startcol=0,
           target_table="sahm_ods.ods_ksa_act_member_invitation_df",
           host="172.33.69.212",
           port=8890,
           user="bart",
           password="123455",
           notename="testc")
