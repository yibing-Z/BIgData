from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
# 加载数据
data = pd.read_csv("user_balance_table.csv")
data['report_date'] = pd.to_datetime(data['report_date'], format='%Y%m%d')

# 按日期聚合申购和赎回总额
daily_data = data.groupby('report_date').agg({'total_purchase_amt': 'sum', 'total_redeem_amt': 'sum'}).reset_index()
# 设置日期为索引
daily_data.set_index('report_date', inplace=True)

# 训练 ARIMA 模型
model_purchase = ARIMA(daily_data['total_purchase_amt'], order=(5, 1, 0))
model_redeem = ARIMA(daily_data['total_redeem_amt'], order=(5, 1, 0))

fit_purchase = model_purchase.fit()
fit_redeem = model_redeem.fit()

# 预测 2014 年 9 月
forecast_purchase = fit_purchase.forecast(steps=30)
forecast_redeem = fit_redeem.forecast(steps=30)

# 将预测结果四舍五入为整数
forecast_purchase = forecast_purchase.round().astype(int)
forecast_redeem = forecast_redeem.round().astype(int)

# 保存预测结果
result = pd.DataFrame({
    'report_date': pd.date_range(start='2014-09-01', end='2014-09-30'),
    'purchase': forecast_purchase,
    'redeem': forecast_redeem
})
result.to_csv("tc_comp_predict_table.csv", index=False, header=True)
