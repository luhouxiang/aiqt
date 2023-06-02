# -*- coding: utf-8 -*-
import string
from typing import Dict
from highlow.dd_obj import TBreed, HoldItem, TContract
from sqlalchemy import create_engine
from highlow import helper
from kds_util.user_logbook import user_log as logger
from kds_util.mysql_database import PyMySql
import requests
# from highlow.store import insert_strategy_opt_position
import json
import traceback
from base.kbase import kBase
from base.baseobj import Contract


class DDAccount:
    def __init__(self, acc:kBase, dest_ip, dest_investor):
        self.holds: Dict[TBreed, HoldItem] = {}
        self.available = 0.0  # 可用资金
        self.amount = 0.0  # 当前总资产
        self.acc = acc
        self.dest_ip = dest_ip
        self.dest_investor = dest_investor
        self.breed_signals: Dict[str, int] = {}  # breed.side
        self.params = {}

    def __str__(self):
        return f"amount:{self.amount},available:{self.available},holds:{self.holds}"

    def __repr__(self):
        return self.__str__()

    def calc_contract_opt_profit(self):
        # for key in self.contract_opt_positions.keys():
        #     contract = key.split(".")[0]
        #     price = self.acc.LastPrice(contract)
        #     self.contract_opt_positions[key].calc_profit(price)
        return

    def get_hold(self, breed: TBreed) -> HoldItem:
        if breed not in self.holds:
            return None
        return self.holds[breed]

    def get_holds(self) -> Dict[TBreed, HoldItem]:
        return self.holds

    def update_account_status(self):
        self._更新资金信息()
        self._更新持仓信息()

    def update_open_position(self, hold_item: HoldItem, hold_price, kt, multiplier, hold_side):
        trade_day = hold_item.trade_day
        contract = hold_item.contract
        opt_vol = self.calc_expected_position(0.06, contract, hold_price, hold_side)  # 预期仓位
        mysql = PyMySql(f"mysql+pymysql://root:2234567@{self.dest_ip}:3306/hqdb?charset=utf8")
        # insert_strategy_opt_position("open", "open", contract, hold_side, abs(opt_vol), abs(opt_vol),
        #                              margin=0.0, profit=0.0, opt_status=1, opt_price=hold_price, kt=kt,
        #                              multiplier=multiplier,
        #                              opts=self.contract_opt_positions,
        #                              mysql=mysql, remark="from_hold")
        return abs(opt_vol)

    def _更新持仓信息(self):
        holds: Dict[TContract, HoldItem] = {}
        breed_holds: Dict[TBreed, HoldItem] = {}
        # dest_ip = self.dest_ip
        try:
            # engine = create_engine(
            #     f'mysql+pymysql://root:2234567@{dest_ip}:3306/hqdb?charset=utf8')  ###
            # con = engine.connect()
            # sql = "SELECT InstrumentID,Direction,Position,OpenAmount,CloseAmount,LastPrice,TradingDay,PositionProfitByTrade from tbl_future_position_general WHERE " \
            #       "TradingDay=(select MAX(TradingDay) from tbl_future_position_general)"
            # # logger.info(sql)
            # df = pd.read_sql(sql, con)
            # con.close()
            # if len(df) == 0:
            #     self.holds.clear()
            #     return
            # for i in range(len(df)):
            for h in self.acc.Holds.values():
                contract = h.InstrumentID
                if contract not in holds:
                    holds[contract] = HoldItem(contract)
                    holds[contract].trade_day = self.acc.intTradeDay
                if h.Direction == "2":
                    holds[contract].long_position = h.Position
                    holds[contract].long_profit = h.CloseProfit+h.PositionProfitByTrade
                    holds[contract].long_today_profit = h.PositionProfit + h.CloseProfit - h.Commission
                if h.Direction == "3":
                    holds[contract].short_position = -h.Position
                    holds[contract].short_profit = h.CloseProfit+h.PositionProfitByTrade
                    holds[contract].short_today_profit = h.PositionProfit + h.CloseProfit - h.Commission
                holds[contract].last_price = h.LastPrice
                
            for contract in holds.keys():
                breed = helper.get_breed_from_contract(contract)
                if breed not in breed_holds:
                    breed_holds[breed] = holds[contract]
                else:
                    if breed_holds[breed].long_position + breed_holds[breed].short_position == 0:
                        breed_holds[breed] = holds[contract]
            for breed in breed_holds:  # 合并开仓数据到持仓中
                hold_item = breed_holds[breed]
                if breed not in breed_holds:
                    continue
                if breed + "L9" not in self.params:
                    continue
                p2 = self.params[breed+"L9"]
                price = p2.contract_info.LastPrice
                kt = p2.contract_info.Last_minute_Kline.trade_time
                multiplier = p2.contract_info.Multiplier
                # if breed_holds[breed].long_position != 0:
                #     key = "{}.{}".format(breed_holds[breed].contract, 1)
                #     if key in opens:
                #         breed_holds[breed].long_open_position = opens[key].open_volume
                #     else:
                #         breed_holds[breed].long_open_position = self.update_open_position(hold_item, price, kt, multiplier, hold_side=1)

                # if breed_holds[breed].short_position != 0:
                #     key = "{}.{}".format(breed_holds[breed].contract, -1)
                #     if key in opens:
                #         breed_holds[breed].short_open_position = opens[key].open_volume
                #     else:
                #         breed_holds[breed].short_open_position = self.update_open_position(hold_item, price, kt, multiplier,
                #                                                                            hold_side=-1)
            self.holds = breed_holds
        except Exception as exception:
            logger.error(str(exception))
            logger.error(traceback.format_exc())
        return

    def _更新资金信息(self):
        self.available = self.acc.kyzj
        self.amount = self.acc.dtqy
        # holds: Dict[TBreed, HoldItem] = {}
        # dest_ip = self.dest_ip
        
        # try:
        #     engine = create_engine(
        #         f'mysql+pymysql://root:2234567@{dest_ip}:3306/hqdb?charset=utf8')  ###
        #     con = engine.connect()
        #     sql = "SELECT CurBalance,Available from tbl_future_trading_account " \
        #           "WHERE TradingDay=(select max(TradingDay) from tbl_future_trading_account)"
        #     logger.debug(sql)
        #     df = pd.read_sql(sql, con)
        #     con.close()
        #     if len(df) == 0:
        #         self.available = 0.0
        #         self.amount = 0.0
        #         return
        #     for i in range(len(df)):
        #         self.amount = float(df["CurBalance"].iloc[i])
        #         self.available = float(df["Available"].iloc[i])
        # except Exception as exception:
        #     logger.error(str(exception))

    def calc_expected_position(self, rate: float, contract_id: TContract, contract_price: float,
                               signal: int = 1) -> int:
        """
        计算出预期仓位
        """
        risk_rate = 0.2
        amount = self.amount * rate * risk_rate
        one_margin = self.calc_one_margin(contract_id, contract_price)
        if contract_price == 0.0 or one_margin == 0.0:
            print(contract_price)
        # logger.info(f"新开仓预分配资金：contract:{contract_id}:{self.amount:.2f} * {rate:.2f} * {risk_rate:.2f} = {amount:.2f}")
        vol = amount // one_margin
        if vol == 0:
            vol = (amount + amount) // one_margin
        elif vol < 3:
            vol = (amount + amount / 3) // one_margin
        # logger.info(f"新开仓实际分配资金：contract:{contract_id}:{one_margin:.2f} * {vol} = {one_margin * vol:.2f}")
        return vol * signal

    def risk_control(self, opt_vol: int, contract_id: str, contract_price: float, risk_rate=0.2) -> int:
        """针对开仓，需要风控在一定风险度，比如20%, 返回可开的笔数"""
        if opt_vol == 0:
            return 0
        one_margin = self.calc_one_margin(contract_id, contract_price)
        ava = self.available - self.amount * (1 - risk_rate)
        if ava <= 0:
            return 0
        try:
            tmp = ava // one_margin
        except Exception as e:
            print(e)
        abs_vol = min(abs(tmp), abs(opt_vol))
        if opt_vol > 0:
            return abs_vol
        if opt_vol < 0:
            return -abs_vol

    def calc_one_margin(self, contract: str, contract_price):
        """
        计算一手的保证金
        """
        breed = contract.rstrip(string.digits)
        p2 = self.params[breed+"L9"]
        price = p2.contract_info.Last_minute_Kline.close
        one_price = round(price * (p2.contract_info.Multiplier * p2.contract_info.MarginRatioByMoney), 2)
        if one_price == 0:
            logger.error(f"{breed}.one_price:{one_price}")
        # logger.info(f"calc_one_margin:[{p2.contract_info.code}]:{price} * {p2.contract_info.Multiplier} * "
        #             f"{p2.contract_info.MarginRatioByMoney} = {OnePrice}")
        return one_price

    def get_multiplier(self, contract) -> float:
        breed = contract.rstrip(string.digits)
        p2 = self.params[breed+"L9"]
        return p2.contract_info.Multiplier

    # %% 推送信号
    def SendBS(self, MHTP: int = 1, OCBS: str = "0", LSBS: str = "3", price: float = 0, VOL: int = 1, CODE='FG209'):
        '''
        MHTP:#0涨跌停价,1市价成交
        OCBS:#0:开仓   1:平仓
        LSBS:#3:空头  2:多头
        '''
        ip = self.dest_ip
        ACCOUNT = self.dest_investor
        url = "http://%s:8084/api/virtual/orderbuysell" % (ip)
        data = {
            'account_id': ACCOUNT,
            'account_type': 'virtual',
            'admin_id': '10000',
            'instrument_id': CODE,
            'effect_type': OCBS,  # 开平;0:开仓    1:平仓
            'posi_direction': LSBS,  # 多空3:空头2多头
            'match_type': MHTP,  # 0涨跌停价,1现价成交
            'price': price,
            'token': 'token',
            'user': 'user',
            'volume': VOL
        }
        breed = CODE.rstrip(string.digits)
        p = self.params[breed + "L9"]
        trade_time = p.contract_info.Last_minute_Kline.trade_time
        open_or_close = ['开仓', '平仓'][int(OCBS)]
        logger.info(f"[{trade_time}]发送【{open_or_close}】买卖信号{data}")
        headers = {'content-type': 'application/json;charset=utf-8'}
        r = requests.post(url, json.dumps(data), headers=headers)
        logger.info(f"[{trade_time}]【{open_or_close}】||交易反馈|{r.text}")
        return r.text
