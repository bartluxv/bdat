# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/8/31
# **************************************



import functools

# datas  {funcname1:(parm1list),funcname2:(parm1list2).......}
datas = dict()


def data(funcparamlist=({"funcname": 1}, {"funcname": 2},)):


    def before(func):
        """加载被装饰的函数前，将驱动参数存放于全局变量 datas"""
        if not func.__name__ in datas:
            datas[func.__name__] = funcparamlist

        @functools.wraps(func)
        def executefunc(*args, **kwargs):
            """被装饰方法执行"""
            func(*args, **kwargs)

        return executefunc

    return before


def ddt(clas):
    """
    添加驱动函数
    :param clas:
    :return:
    """
    keylist = []
    for funcname, funcparamlist in datas.items():
        """遍历当前所有被装饰的方法的参数"""
        for param in funcparamlist:
            """遍历单个被装饰的方法参数"""
            if not isinstance(param, dict):
                raise Exception("uddt.data object is not dict")
            sourcefunc = getattr(clas, funcname)

            funcnamere = str(funcname).replace("test_", "")
            print('ddt',param)
            _newfuncname = f"test_{funcnamere}_{param.get('name', funcparamlist.index(param))}"
            print("ddt funcname",_newfuncname)

            setattr(clas, _newfuncname, functools.partialmethod(sourcefunc, kwargs=param))
        keylist.append(funcname)

    for funcnameKey in keylist:
        """删除当前函数存有的变量，避免与其它的冲突"""
        del datas[funcnameKey]

    return clas


# @ddt
# class MN():
#
#     @data([{"desc": "x"}, {"desc": "y"}, {"desc": "z"}])
#     def nputme(self, kwargs):
#         pass
#         print(f"func nputme  {kwargs}  param")
#
#     @data([{"desc": "x2"}, {"desc": "y2"}, {"desc": "z2"}])
#     def nputmey(self, kwargs):
#         pass
#         print(f"func nputmey  {kwargs}  param")


if __name__ == "__main__":
    # obj=MN()
    pass



