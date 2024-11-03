import pandas as pd
import numpy as np
from .const import Config as config

class DealData:
    # 处理时间段
    @staticmethod
    def get_chargePeriod(df):
        if len(df) < 1:
            return df
        # 从chargePeriod字段获取 开始和结束计算时间点，并返回以小时为单位的时间点
        df[['start_chargePeriod', 'end_chargePeriod']] = df['chargePeriod'].str.split(' -- ', expand=True)
        # 将时间戳转换为datetime格式
        df['start_chargePeriod'] = pd.to_datetime(df['start_chargePeriod'])
        df['end_chargePeriod'] = pd.to_datetime(df['end_chargePeriod'])
        return df


    # 处理为天的时间差
    @staticmethod
    def get_chargeDay(df):
        if len(df) < 1:
            return df
        # 从chargePeriod字段获取 开始和结束计算时间点，并返回以小时为单位的时间点
        df['diff_hour'] = (df['end_chargePeriod'] - df['start_chargePeriod']).dt.total_seconds() / (3600 * 24)
        # 四舍五入
        df['diff_hour'] = df['diff_hour'].round(0)
        return df

    # 处理为小时的时间差
    @staticmethod
    def get_chargeHour(df):
        # 从chargePeriod字段获取 开始和结束计算时间点，并返回以小时为单位的时间点
        df['diff_hour'] = (df['end_chargePeriod'] - df['start_chargePeriod']).dt.total_seconds() / 3600
        # 向上取整
        df['diff_hour'] = np.ceil(df['diff_hour'])
        return df

    # 将time拆分到天
    @staticmethod
    def get_date(df):
        df['date'] = df['time'].dt.date
        return df

    # 按照product, name, realPrice, originPrice, cloud, chargePeriod去重
    @staticmethod
    def get_unique(df):
        df_1 = df.drop_duplicates(subset=config.Dupicate_cols).reset_index()
        return df_1

    # 统一处理包年包月数据
    @staticmethod
    def deal_payByMonth(df, cols):
        groupByFieds = ['time', 'product', 'name']
        # 取出所有 包年包月付费数据
        df_month = df[df['paymode'] == config.Pay_Go]
        # 按照time和name对其做数据清洗，并筛选掉 已经退掉的机器
        df_month1 = df_month.groupby(groupByFieds)['realPrice'].sum().reset_index(name='realPrice')
        df_month2 = df_month.groupby(groupByFieds)['originPrice'].sum().reset_index(name='originPrice')
        # 去重并保留指定的cols
        df_month3 = df_month.drop_duplicates(subset=groupByFieds).reset_index()
        df_month3 = df_month3[cols]
        # 合并
        merge_df_month = pd.merge(df_month1, df_month2, on=groupByFieds, how='inner')
        merge_df_month = pd.merge(merge_df_month, df_month3, on=groupByFieds, how='inner')
        # 剔除<0的数据：机器退掉了或其他情况数据
        merge_df_month = merge_df_month[merge_df_month['realPrice'] > 0].reset_index()
        return merge_df_month

    # 将包年包月的数据处理为 每小时计费
    @staticmethod
    def payByMonth_to_hour(df, day):
        hours = 24 * day
        df['realPrice'] = df['realPrice'] / hours
        df['originPrice'] = df['originPrice'] / hours
        return df

    # 将按量计费中的 以天计算的计费均摊到小时上
    @staticmethod
    def get_payByGoDay(df):
        df_payByGo = df[df['paymode'] == config.Pay_Go]
        df_payByGo_day = df_payByGo[df_payByGo['chargeUnit'] == config.ChargeUnit_day]
        df_payByGo_day['realPrice'] = df_payByGo_day['realPrice'] / 24
        df_payByGo_day['originPrice'] = df_payByGo_day['originPrice'] / 24
        return df_payByGo_day

    # 将按量计费中的 以小时计算的计费均摊到小时上
    @staticmethod
    def get_payByGoHour(df):
        df_payByGo = df[df['paymode'] == config.Pay_Go]
        df_payByGo_hour = df_payByGo[df_payByGo['chargeUnit'] == config.ChargeUnit_hour]
        df_payByGo_hour['realPrice'] = df_payByGo_hour['realPrice'] / 24
        df_payByGo_hour['originPrice'] = df_payByGo_hour['originPrice'] / 24
        return df_payByGo_hour

    # 获取 按量计费中不同的 计费方式的数据
    @staticmethod
    def get_payByGo(df):
        # 取出 按量计费
        df_payByGo = df[df['paymode'] == config.Pay_Go]
        df_payByGo_month = df_payByGo[df_payByGo['chargeUnit'] == config.ChargeUnit_month].reset_index()
        df_payByGo_day = df_payByGo[df_payByGo['chargeUnit'] == config.ChargeUnit_day].reset_index()
        df_payByGo_hour = df_payByGo[df_payByGo['chargeUnit'] == config.ChargeUnit_hour].reset_index()
        return df_payByGo_hour, df_payByGo_day, df_payByGo_month

    # 合并
    @staticmethod
    def merge_dataframe(dfs):
        # 拼接数据
        merge_df = pd.concat(dfs, axis=0)
        return merge_df

    @staticmethod
    def deal_originPrice(df, ori, real):
        df[ori] = np.where(df[ori] == 0, df[real], df[ori])
        return df

    @staticmethod
    def get_dayPrice(df, day_price, unique, price):
        df[day_price] = 0
        for group in df[unique].unique():
            mask = df[unique] == group
            df.loc[mask, day_price] = df.loc[mask, price].diff().fillna(0)
        # day_price<0将其填充为 price: 处理跨月数据以及第一个数据情况
        df[day_price] = np.where(df[day_price] <= 0, df[price], df[day_price])
        return df



