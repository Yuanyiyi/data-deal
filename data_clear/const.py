class Config:
    Pay_Go = '按量计费'
    Pay_By_Month = '包年包月'
    ChargeUnit_hour = '小时'
    ChargeUnit_month = '月'
    ChargeUnit_day = '日'
    CVM = 'CVM'
    Bill_cols = ['time', 'cloud', 'product', 'productType', 'billKind', 'region',
                 'zone', 'id', 'name', 'config', 'department', 'paymode', 'chargeUnit',
                 'chargePeriod', 'originPrice', 'realPrice', 'discount', 'invoiceDiscount']
    Cols = ['time', 'cloud', 'product', 'productType', 'region', 'id', 'name', 'paymode', 'chargeUnit', 'chargePeriod']

    # 写入ck的表字段
    Mw_Bill_cols = ['date', 'cloud', 'product', 'productType', 'region', 'name_number',
                    'day_realPrice', 'day_originPrice', 'dd_unique', 'not_fill']

    Dupicate_cols = ['cloud', 'product', 'region', 'name', 'realPrice', 'originPrice', 'chargePeriod']
    Groupby_cols = ['time', 'cloud', 'product', 'region', 'name']
    Groupby_cols1 = ['date', 'cloud', 'region', 'product', 'productType', 'dd_unique']
