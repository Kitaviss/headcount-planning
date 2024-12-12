# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=line-too-long

import pandas as pd
# import pandas_gbq

# project_id = "tnsl-dwh"
# sql_raw_data_forecast_fulfillment_volume = """
# select 
# full_date,
# hour,
# warehouse,
# client,
# sub_client,
# channel,
# # để counter later on với max(day_type) của panda khi day_type là string và có thể có giá trị null
# ifnull(day_type, '') day_type,
# exsd,
# sla_platform,
# round(orders_forecast) orders_forecast,
# round(units_forecast) units_forecast,
# from `tnsl-dwh.snop.raw_data_forecast_fulfillment_volume_new`
# where 1 = 1
# """

# df_forecast_origin = pandas_gbq.read_gbq(sql_raw_data_forecast_fulfillment_volume, project_id=project_id)

df_forecast_origin = pd.read_csv("data/forecast.csv")

df_forecast_origin['full_date'] = pd.to_datetime(df_forecast_origin['full_date'])
df_forecast_origin['exsd'] = pd.to_datetime(df_forecast_origin['exsd'])
df_forecast_origin['sla_platform'] = pd.to_datetime(df_forecast_origin['sla_platform'])
df_forecast_origin['created_time'] = df_forecast_origin['full_date'].astype(str) + ' ' + df_forecast_origin['hour'].astype(str) + ':00:00'
df_forecast_origin['created_time'] = pd.to_datetime(df_forecast_origin['created_time'])
df_forecast_origin.insert(0, 'created_time', df_forecast_origin.pop('created_time'))

df_forecast_origin.rename(columns={'full_date':'created_time_date',
                                   'hour':'created_time_hour',}, inplace=True)

df_day_type = df_forecast_origin.groupby(['created_time']).agg({'day_type':'max'},)
df_day_type.reset_index(inplace=True)

def main():
    # pd.set_option('display.max_rows',99999)
    # pd.set_option('display.max_columns',99999)
    print(df_forecast_origin)
    print(df_day_type)

if __name__ == "__main__":
    main()