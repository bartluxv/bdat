import functools

import allure
import pytest

from common.funcs import testPlatform,log
from common.uddt import data,ddt


@ddt
class TestDemo:

    @testPlatform
    @data([{"name": "caseads"}, {"name": "caseods"},])
    def test_last(self,kwargs=19):
            log().warning(f"kwargs is  {kwargs}")



# for i in range(10):
#     func=functools.partialmethod(TestDemo.test_functioncase2, name=i)
#
#     setattr(TestDemo,f"test_functioncase_{i}",func)

