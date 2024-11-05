import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from model.clickhouse import Test1CK, Test2CK
from data_clear import DataClear, Config
from config import Configs, Log


class DataManager:
    __slots__ = (
        "__test1",
        "__test2",
        "data_clear"
    )
    __test1: Test1CK
    __test2: Test2CK
    data_clear: DataClear

    def __init__(self, test2: Test2CK, test1: Test1CK):
        self.__test1 = test1
        self.__test2 = test2
        self.data_clear = DataClear()

    def list_to_dataframe(self, data: list) -> pd.DataFrame:
        df = pd.DataFrame(data, columns=Config.Bill_cols)
        return df

    def get_hn_data(self, start_time: int, end_time: int) -> pd.DataFrame:
        Log.debug(f"get_hn_data, start_time: {start_time}, end_time: {end_time}")
        cloud = "xx"
        region = "xx"
        # 获取华南地区的数据
        data_hn = self.__test1.get_bill_by_region(cloud, region, start_time, end_time)
        if data_hn is None:
            return pd.DataFrame()
        df = self.list_to_dataframe(data_hn)
        # 取出 包年包月 数据
        data_month = df[df['paymode'] == Config.Pay_By_Month].reset_index()
        data_month1 = self.data_clear.deal_bymonth(data_month)
        # 取出 按量计费 数据
        data_payGo = df[df['paymode'] == Config.Pay_Go].reset_index()
        data_payGo1 = self.data_clear.deal_payGo(data_payGo)
        # 数据拼接
        data = pd.concat([data_month1, data_payGo1], axis=0)
        data['mw_unique'] = 1
        return data

    def concat_data(self, start_time: int, end_time: int) -> pd.DataFrame:
        Log.debug(f"concat_data, start_time: {start_time}, end_time: {end_time}")
        data_hn = self.get_hn_data(start_time=start_time, end_time=end_time)

        # 转换所有 object 类型的列为字符串类型
        for col in data_hn.select_dtypes(include=['object']).columns:
            data_hn[col] = data_hn[col].astype(str)
        return data_hn

    def handle_data_day(self):
        # 获取当前时间
        current_time = datetime.now()
        # 计算当天的凌晨时间
        today_midnight = datetime(current_time.year, current_time.month, current_time.day)
        # 将当天的凌晨时间转换为时间戳
        today_midnight_timestamp = int(today_midnight.timestamp())
        # 计算上一天的凌晨时间
        last_day_midnight = today_midnight - timedelta(days=1)
        # 将上一天 凌晨时间转换为时间戳
        last_day_midnight_timestamp = int(last_day_midnight.timestamp())
        # 获取当前日期
        current_date = datetime.now()
        # 格式化日期为字符串，例如 'YYYY-MM-DD'
        date_str = current_date.strftime('%Y-%m-%d')

        data = self.concat_data(last_day_midnight_timestamp, today_midnight_timestamp)
        self.__test2.insert_dataframe(date_str, data)

    def handle_history_data(self):
        # 重新生成每6个月间隔的时间点列表
        start_time = datetime(2022, 1, 1)
        end_time = datetime.now()
        six_month_intervals = pd.date_range(start=start_time, end=end_time, freq='2ME')
        # 重新生成时间戳列表
        six_month_timestamps = [date.timestamp() for date in six_month_intervals]

        if len(six_month_timestamps) < 1:
            return
        # 取出最小的时间戳作为开始时间戳
        start_time1 = six_month_timestamps[0]
        for end_time1 in six_month_timestamps[1:]:
            data = self.concat_data(start_time1, end_time1)
            self.__test2.insert_dataframe(f"start_time: {datetime.fromtimestamp(start_time1)}, "
                                       f"end_time: {datetime.fromtimestamp(end_time1)}", data)
            # 将上一轮的结束时间作为下一路开始时间
            start_time1 = end_time1

        # 从上一轮时间到当前时间的计算
        data = self.concat_data(start_time1, end_time)
        self.__test2.insert_dataframe(f"start_time: {datetime.fromtimestamp(start_time1)}, "
                                   f"end_time: {datetime.fromtimestamp(end_time)}", data)

    def exec(self):
        Log.debug("ds_manager exec")
        self.handle_data_day()
        # if Configs.server().first:
        #     Log.info("handle history data, from 2020-01-01 to now")
        #     self.handle_history_data()
        # else:
        #     self.handle_data_day()

