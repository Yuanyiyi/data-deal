import pandas as pd
import numpy as np

from .const import Config as config
from .common import DealData


class DataClear:
    # 获取 按量计费 模式清洗后的数据
    def get_payGo(self, data):
        df_month = data[data['chargeUnit'] == config.ChargeUnit_month].reset_index()
        df_day = data[data['chargeUnit'] == config.ChargeUnit_day].reset_index()
        df_hour = data[data['chargeUnit'] == config.ChargeUnit_hour].reset_index()

        merge_df_month = self.deal_payGo(df_month)
        merge_df_day = self.deal_payGo(df_day)
        merge_df_hour = self.deal_payGo(df_hour)

        df = DealData.merge_dataframe([merge_df_month, merge_df_day, merge_df_hour])
        return df

    # 获取 包年包月 模式清洗后的数据
    def get_bymonth(self, data):
        return self.deal_bymonth(data)

    # 处理 按量计费数据
    def deal_payGo(self, data):
        data_real = data.groupby(config.Groupby_cols)['realPrice'].sum().reset_index(name='realPrice')
        data_ori = data.groupby(config.Groupby_cols)['originPrice'].sum().reset_index(name='originPrice')
        data_unique = data.drop_duplicates(subset=config.Groupby_cols).reset_index()
        data_unique = data_unique[config.Cols]
        merge_data = pd.merge(data_real, data_ori, on=config.Groupby_cols, how='inner')
        merge_data = pd.merge(merge_data, data_unique, on=config.Groupby_cols, how='inner')
        merge_data_unique = DealData.get_chargePeriod(merge_data)
        merge_data_unique = DealData.get_chargeHour(merge_data_unique)

        # 按照product, name, realPrice, originPrice, cloud, chargePeriod去重
        merge_data_unique = merge_data_unique.drop_duplicates(subset=config.Dupicate_cols).reset_index()
        # 将time拆分到天
        merge_data_unique['date'] = pd.to_datetime(merge_data_unique['time'].dt.date).astype(str)
        # 先按时间顺序排序，然后计算行差为每天的真实计费
        merge_data_unique = merge_data_unique.sort_values(by='date', ascending=False)

        merge_data_unique = DealData.get_dayPrice(merge_data_unique, 'day_realPrice', 'name', 'realPrice')
        merge_data_unique = DealData.get_dayPrice(merge_data_unique, 'day_originPrice', 'name', 'originPrice')
        merge_data_unique = DealData.deal_originPrice(merge_data_unique, 'day_originPrice', 'day_realPrice')

        # 按照name中包含有ds和非ds进行划分
        merge_data_unique['dd_unique'] = np.where(merge_data_unique['name'].astype(str).str.contains('ds'), 1, 0)

        # 按product汇总
        merge_data_unique0 = merge_data_unique.groupby(config.Groupby_cols1)['day_realPrice'].sum().reset_index(
            name='day_realPrice')
        merge_data_unique1 = merge_data_unique.groupby(config.Groupby_cols1)['day_originPrice'].sum().reset_index(
            name='day_originPrice')
        merge_data_unique2 = merge_data_unique.groupby(config.Groupby_cols1)['name'].nunique().reset_index()

        # 按照某列进行合并
        merge_df = pd.merge(merge_data_unique0, merge_data_unique1, on=config.Groupby_cols1, how='outer')
        merge_df = pd.merge(merge_df, merge_data_unique2, on=config.Groupby_cols1, how='outer')
        # merge_df['mw_unique'] = 0

        merge_df = merge_df.rename(columns={'name': 'name_number'})
        merge_df['not_fill'] = 0
        merge_df['not_fill'] = np.where(
            merge_df['day_originPrice'] <= 0, 1, 0)
        merge_df['day_originPrice'] = np.where(
            merge_df['day_originPrice'] <= 0,
            merge_df['day_realPrice'],
            merge_df['day_originPrice'])
        merge_df['day_realPrice'] = merge_df['day_realPrice'].round(3)
        merge_df['day_originPrice'] = merge_df['day_originPrice'].round(3)

        return merge_df

    # 处理包年包月的数据
    def deal_bymonth(self, data) -> pd.DataFrame:
        if len(data) < 1:
            return pd.DataFrame()
        # 部分会存在折扣情况，需要将 实际使用费用：已付费-折扣费用
        data_real = data.groupby(config.Groupby_cols)['realPrice'].sum().reset_index(name='realPrice')
        data_ori = data.groupby(config.Groupby_cols)['originPrice'].sum().reset_index(name='originPrice')
        data_unique = data.drop_duplicates(subset=config.Groupby_cols).reset_index()
        data_unique = data_unique[config.Cols]
        merge_df_month = pd.merge(data_real, data_ori, on=config.Groupby_cols, how='inner')
        merge_df_month = pd.merge(merge_df_month, data_unique, on=config.Groupby_cols, how='inner')
        # 将time拆分到天
        merge_df_month['date'] = pd.to_datetime(merge_df_month['time'].dt.date).astype(str)
        # 按照product, name, realPrice, originPrice, cloud, chargePeriod去重
        merge_df_unique = merge_df_month.drop_duplicates(subset=config.Dupicate_cols).reset_index()
        # 计算 包年包月 付费范围
        merge_df_unique = DealData.get_chargePeriod(merge_df_unique)
        merge_df_unique = DealData.get_chargeDay(merge_df_unique)
        # 剔除掉当天退掉的机器
        if len(merge_df_unique) > 0:
            merge_df_unique = merge_df_unique[merge_df_unique['diff_hour'] > 0].reset_index()
        # 当天
        merge_df_unique['day_realPrice'] = merge_df_unique['realPrice'] / merge_df_unique['diff_hour']
        # originPrice
        merge_df_unique['day_originPrice'] = merge_df_unique['originPrice'] / merge_df_unique['diff_hour']
        # 若originPrice的值为0，则按照realPrice填充
        merge_df_unique = DealData.deal_originPrice(merge_df_unique, 'day_originPrice', 'day_realPrice')

        # 按照name中包含有ds和非ds进行划分
        merge_df_unique['dd_unique'] = np.where(merge_df_unique['name'].astype(str).str.contains('ds'), 1, 0)

        # 按照 product汇总
        merge_df_unique0 = merge_df_unique.groupby(config.Groupby_cols1)['day_realPrice'].sum().reset_index(
            name='day_realPrice')
        merge_df_unique1 = merge_df_unique.groupby(config.Groupby_cols1)['day_originPrice'].sum().reset_index(
            name='day_originPrice')
        merge_df_unique2 = merge_df_unique.groupby(config.Groupby_cols1)['name'].nunique().reset_index()
        # 按照某列进行合并
        merge_df = pd.merge(merge_df_unique0, merge_df_unique1, on=config.Groupby_cols1, how='outer')
        merge_df = pd.merge(merge_df, merge_df_unique2, on=config.Groupby_cols1, how='outer')

        merge_df = merge_df.rename(columns={'name': 'name_number'})
        merge_df['not_fill'] = 0
        merge_df['not_fill'] = np.where(
            merge_df['day_originPrice'] <= 0, 1, 0)
        merge_df['day_originPrice'] = np.where(
            merge_df['day_originPrice'] <= 0,
            merge_df['day_realPrice'],
            merge_df['day_originPrice'])
        merge_df['day_realPrice'] = merge_df['day_realPrice'].round(3)
        merge_df['day_originPrice'] = merge_df['day_originPrice'].round(3)

        return merge_df
