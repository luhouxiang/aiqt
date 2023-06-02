# -*- coding: utf-8 -*-
"""
@author: luhx
@file: formula.py
@time: 2023/6/2 11:31
@desc:  基础公式
"""


# 这个类计算给定价格列表的移动平均值
class MA:
    # 初始化变量的构造函数
    def __init__(self, e0=5):
        # 如果窗口大小小于1，则将其设置为1
        if e0 < 1:
            e0 = 1
        self.N = e0
        self.ma = 0
        self.v_list = []

    # 输入新价格并计算移动平均值的方法
    def input(self, price, multiplier=1):
        # 将新价格添加到价格列表中
        self.v_list.append(price)
        # 根据窗口大小和乘数仅保留最新价格
        self.v_list = self.v_list[-multiplier * self.N:]
        # 计算移动平均值
        self.ma = sum(self.v_list) / len(self.v_list)

    # 替换列表中的最后一个价格并重新计算移动平均值的方法
    def replace(self, price):
        # 用新价格替换列表中的最后一个价格
        self.v_list[-1] = price
        # 重新计算移动平均值
        self.ma = sum(self.v_list) / len(self.v_list)