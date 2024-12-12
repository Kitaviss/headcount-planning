# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=C0200
# pylint: disable=line-too-long
# pylint: disable=W0702
# pylint: disable=W0201

import copy
from datetime import datetime, timedelta
import warnings
import pandas as pd
import numpy as np
import json
import timeit
import gspread_pandas
from gspread_pandas import Spread, Client
from module.Labor import Labor
from module.Forecast import Forecast
from module.Warehouse import Warehouse
from module.Backlog import Backlog

class Simulation:
    def __init__(
            self,
            warehouse: Warehouse,
            labor: tuple,
            uph_fte_pick: dict = None,
            uph_ow_pick: dict = None,
            uph_fte_pack: dict = None,
            uph_ow_pack: dict = None,
            normalize_day: bool = True,
            forecast: Forecast = Forecast(),
            backlog: Backlog = Backlog()
        #  forecast: pd.DataFrame = Forecast().df_forecast,
        #  backlog: pd.DataFrame = Backlog().default()
            ) -> None:
        '''
        Class để ghi nhận việc Simulation về Outbound (cũng như Ontime) cho các input Forecast và Capacity tương ứng. \n
        Các Input đầu vào:
        - warehouse: Ghi nhận Warehouse để tính toán (vì mỗi lần chỉ tiến hành simulate được cho 1 Warehouse duy nhất)
        - forecast: Ghi nhận lượng Forecast volume phát sinh
        - capacity: Ghi nhận lượng công suất Capacity mà Warehouse có (dựa trên các Workforce) trong thời gian tương ứng
        - backlog: Ghi nhận lượng Backlog tồn đọng còn lại đầu kì
        '''
        # Log all parameter:
        self.log_warehouse= warehouse
        self.log_forecast = forecast
        self.log_backlog = backlog
        self.log_labor = labor
        # -----------
        self.df_warehouse = warehouse.warehouse
        self.df_max_outbound_station = warehouse.max_outbound_station

        capacity = self.log_warehouse.total_working_shift(
            self.log_warehouse.uph(
                uph_fte_pick=uph_fte_pick,
                uph_ow_pick=uph_ow_pick,
                uph_fte_pack=uph_fte_pack,
                uph_ow_pack=uph_ow_pack,
                normalize_day=normalize_day), *self.log_labor)
        
        try:
            self.df_forecast = copy.deepcopy(self.log_forecast.df_forecast.loc[self.log_forecast.df_forecast['warehouse'] == self.df_warehouse])

            self.df_forecast['unit_per_order'] = self.df_forecast['units_forecast']/self.df_forecast['orders_forecast']
            self.df_forecast.drop(columns=['orders_forecast'], inplace=True)
            self.df_forecast.insert(0, 'current_time', self.df_forecast['created_time'])
            self.df_forecast.rename(columns={'units_forecast':'volume',}, inplace=True)

            self.df_forecast.insert(9, 'exsd_date', self.df_forecast['exsd'].dt.date)
            self.df_forecast['exsd_date'] = pd.to_datetime(self.df_forecast['exsd_date'])
            self.df_forecast.insert(10, 'exsd_hour', self.df_forecast['exsd'].dt.hour)

            self.df_forecast.insert(12, 'sla_platform_date', self.df_forecast['sla_platform'].dt.date)
            self.df_forecast['sla_platform_date'] = pd.to_datetime(self.df_forecast['sla_platform_date'])
            self.df_forecast.insert(13, 'sla_platform_hour', self.df_forecast['sla_platform'].dt.hour)
        except:
        #     # không tìm thấy Forecast cho Warehouse này
            pass

        try:
            # sau này sẽ thay thế dòng này bằng Backlog actual của Warehouse này
            self.df_backlog_volume = copy.deepcopy(self.log_backlog.default().loc[backlog['warehouse'] == self.df_warehouse])
        except:
            self.df_backlog_volume = self.log_backlog.default()

        try:
            # không tìm thấy Capacity cho Warehouse này
            self.df_capacity = capacity.loc[capacity['warehouse'] == self.df_warehouse]
        except:
            pass

    def gen_df_forecast(self) -> None:
        try:
            self.df_forecast = pd.DataFrame()

            self.df_forecast = copy.deepcopy(self.log_forecast.df_forecast.loc[self.log_forecast.df_forecast['warehouse'] == self.df_warehouse])

            self.df_forecast['unit_per_order'] = self.df_forecast['units_forecast']/self.df_forecast['orders_forecast']
            self.df_forecast.drop(columns=['orders_forecast'], inplace=True)
            self.df_forecast.insert(0, 'current_time', self.df_forecast['created_time'])
            self.df_forecast.rename(columns={'units_forecast':'volume',}, inplace=True)

            self.df_forecast.insert(9, 'exsd_date', self.df_forecast['exsd'].dt.date)
            self.df_forecast['exsd_date'] = pd.to_datetime(self.df_forecast['exsd_date'])
            self.df_forecast.insert(10, 'exsd_hour', self.df_forecast['exsd'].dt.hour)

            self.df_forecast.insert(12, 'sla_platform_date', self.df_forecast['sla_platform'].dt.date)
            self.df_forecast['sla_platform_date'] = pd.to_datetime(self.df_forecast['sla_platform_date'])
            self.df_forecast.insert(13, 'sla_platform_hour', self.df_forecast['sla_platform'].dt.hour)
        except:
            pass

    def forecast(self) -> pd.DataFrame:
        """Method để gọi Data Forecast Raw của Warehouse trong khoảng thòi gian tương ứng

        Returns:
            pd.DataFrame: Data Forecast
        """
        # self.gen_df_forecast()
        df_forecast = copy.deepcopy(self.df_forecast)
        df_forecast['order'] = df_forecast['volume'] / df_forecast['unit_per_order']
        return df_forecast
    
    def capacity(self) -> pd.DataFrame:
        df_capacity = copy.deepcopy(self.df_capacity)
        return df_capacity

    def total_staff_fte(self):
        total_staff_fte = 0
        for i in range(len(self.log_labor)):
            if self.log_labor[i].contract_type == 'FTE':
                total_staff_fte += self.log_labor[i].number_of_staff
        return total_staff_fte
    
    def total_staff_ow(self):
        total_staff_ow = 0
        for i in range(len(self.log_labor)):
            if self.log_labor[i].contract_type == 'OW':
                total_staff_ow += self.log_labor[i].number_of_staff
        return total_staff_ow
    
    def total_cost(self):
        total_cost = 0
        for i in range(len(self.log_labor)):
            if self.log_labor[i].cost is None:
                cost = 0
            else:
                cost = self.log_labor[i].cost

            start_time = self.log_labor[i].start_time
            end_time = self.log_labor[i].end_time

            working_hour = (end_time - start_time).total_seconds()/3600

            total_cost += self.log_labor[i].number_of_staff * cost * working_hour
        return total_cost

    def forecast_summary(self,
                         attribute: list = ['created_time_date'],
                         by: list = ['exsd_date'],
                         view: str = 'Unit') -> pd.DataFrame:
        """_summary_

        Parameters
        ----------
        attribute : list, optional
            _description_, by default None
        by : str, optional
            _description_, by default 'exsd_date'
        view : str, optional
            _description_, by default 'Unit'

        Returns
        -------
        pd.DataFrame
            _description_
        """
        # if attribute is None:
        #     attribute_final = ['created_time_date', 'created_time_hour']
        # else:
        #     attribute_final = ['created_time_date', 'created_time_hour'] + attribute

        # self.gen_df_forecast()
        df_forecast = copy.deepcopy(self.df_forecast.drop(columns=['day_type']))
        df_forecast['order'] = df_forecast['volume'] * df_forecast['unit_per_order']

        df_forecast_summary_unit = pd.pivot_table(df_forecast, values='volume', index=attribute, columns=by, aggfunc={'volume':'sum'}, fill_value=0, margins=True)
        df_forecast_summary_order = pd.pivot_table(df_forecast, values='order', index=attribute, columns=by, aggfunc={'order':'sum'}, fill_value=0, margins=True)

        if view == "Unit":
            return df_forecast_summary_unit
        else:
            return df_forecast_summary_order

    def maximum_allowable_backlog_exsd(self):
        '''
        Method để ghi nhận Maximum Allowable Backlog (MAB) detail của từng ExSD trong mỗi khung giờ
        '''
        # self.gen_df_forecast()
        df_forecast = copy.deepcopy(self.df_forecast.drop(columns=[
            'day_type',
            'exsd_date',
            'exsd_hour',
            'sla_platform_date',
            'sla_platform_hour'
            ]))
        df_backlog_volume = copy.deepcopy(self.df_backlog_volume)

        df_temp_1 = pd.concat(
            [df_backlog_volume, df_forecast], 
            ignore_index=True)
        df_temp_2 = pd.merge(
            df_temp_1, 
            df_temp_1, 
            on=[
                'warehouse',
                'client',
                # 'sub_client',
                'channel'], 
            how='left', 
            suffixes=['_df1','_df2'], 
            indicator=True)
        df_maximum_allowable_backlog_exsd = copy.deepcopy(
            df_temp_2.loc[
                (df_temp_2['created_time_df2'] <= df_temp_2['created_time_df1']) &
                (df_temp_2['exsd_df2'] > df_temp_2['created_time_df1'])])

        df_maximum_allowable_backlog_exsd.rename(columns={'current_time_df1':'current_time',
                                                          'created_time_date_df1':'current_time_date',
                                                          'created_time_hour_df1':'current_time_hour',
                                                          'created_time_df2':'created_time',
                                                          'created_time_date_df2':'created_time_date',
                                                          'created_time_hour_df2':'created_time_hour',
                                                          'exsd_df2':'exsd',
                                                        #   'exsd_date_df2':'exsd_date',
                                                        #   'exsd_hour_df2':'exsd_hour',
                                                          'sla_platform_df2':'sla_platform',
                                                        #   'sla_platform_date_df2':'sla_platform_date',
                                                        #   'sla_platform_hour_df2':'sla_platform_hour',
                                                          'volume_df2':'maximum_allowable_backlog_exsd',
                                                          'unit_per_order_df2':'unit_per_order'}, inplace=True)
        
        df_maximum_allowable_backlog_exsd.drop(columns=['created_time_df1',
                                                        'exsd_df1',
                                                        # 'exsd_date_df1',
                                                        # 'exsd_hour_df1',
                                                        'sla_platform_df1',
                                                        # 'sla_platform_date_df1',
                                                        # 'sla_platform_hour_df1',
                                                        'volume_df1',
                                                        'unit_per_order_df1',
                                                        'current_time_df2',
                                                        '_merge'], inplace=True)

        return df_maximum_allowable_backlog_exsd
    
    def maximum_allowable_backlog_exsd_summary(self):
        '''
        Method để ghi nhận Summary Maximum Allowable Backlog trong mỗi khung giờ
        '''
        df_maximum_allowable_backlog_exsd = self.maximum_allowable_backlog_exsd()

        df_maximum_allowable_backlog_exsd_summary = df_maximum_allowable_backlog_exsd.groupby(
            ['current_time',
             'warehouse',
             'client',
             'channel']
             ).agg({'maximum_allowable_backlog_exsd':'sum'})

        df_maximum_allowable_backlog_exsd_summary.reset_index(inplace=True)

        return df_maximum_allowable_backlog_exsd_summary

    def maximum_allowable_backlog_sla_platform(self):
        '''
        Method để ghi nhận Maximum Allowable Backlog (MAB) detail của từng SLA Platform trong mỗi khung giờ
        '''
        # self.gen_df_forecast()
        df_forecast = copy.deepcopy(self.df_forecast.drop(columns=[
            'day_type',
            'exsd_date',
            'exsd_hour',
            'sla_platform_date',
            'sla_platform_hour'
            ]))
        df_backlog_volume = copy.deepcopy(self.df_backlog_volume)

        df_temp_1 = pd.concat(
            [df_backlog_volume, df_forecast], 
            ignore_index=True)
        df_temp_2 = pd.merge(
            df_temp_1, 
            df_temp_1, 
            on=[
                'warehouse',
                'client',
                # 'sub_client',
                'channel'], 
            how='left', 
            suffixes=['_df1','_df2'], 
            indicator=True)
        df_maximum_allowable_backlog_sla_platform = copy.deepcopy(
            df_temp_2.loc[
                (df_temp_2['created_time_df2'] <= df_temp_2['created_time_df1']) &
                (df_temp_2['sla_platform_df2'] > df_temp_2['created_time_df1'])])

        df_maximum_allowable_backlog_sla_platform.rename(columns={'current_time_df1':'current_time',
                                                                  'created_time_date_df1':'current_time_date',
                                                                  'created_time_hour_df1':'current_time_hour',
                                                                  'created_time_df2':'created_time',
                                                                  'created_time_date_df2':'created_time_date',
                                                                  'created_time_hour_df2':'created_time_hour',
                                                                  'exsd_df2':'exsd',
                                                                #   'exsd_date_df2':'exsd_date',
                                                                #   'exsd_hour_df2':'exsd_hour',
                                                                  'sla_platform_df2':'sla_platform',
                                                                #   'sla_platform_date_df2':'sla_platform_date',
                                                                #   'sla_platform_hour_df2':'sla_platform_hour',
                                                                  'volume_df2':'maximum_allowable_backlog_sla_platform',
                                                                  'unit_per_order_df2':'unit_per_order'}, inplace=True)
        
        df_maximum_allowable_backlog_sla_platform.drop(columns=['created_time_df1',
                                                                'exsd_df1',
                                                                # 'exsd_date_df1',
                                                                # 'exsd_hour_df1',
                                                                'sla_platform_df1',
                                                                # 'sla_platform_date_df1',
                                                                # 'sla_platform_hour_df1',
                                                                'volume_df1',
                                                                'unit_per_order_df1',
                                                                'current_time_df2',
                                                                '_merge'], inplace=True)

        return df_maximum_allowable_backlog_sla_platform
    
    def maximum_allowable_backlog_sla_platform_summary(self):
        '''
        Method để ghi nhận Summary Maximum Allowable Backlog trong mỗi khung giờ
        '''
        df_maximum_allowable_backlog_sla_platform = self.maximum_allowable_backlog_sla_platform()

        df_maximum_allowable_backlog_sla_platform_summary = df_maximum_allowable_backlog_sla_platform.groupby(
            ['current_time',
             'warehouse',
             'client',
             'channel']
             ).agg({'maximum_allowable_backlog_sla_platform':'sum'})

        df_maximum_allowable_backlog_sla_platform_summary.reset_index(inplace=True)

        return df_maximum_allowable_backlog_sla_platform_summary
    
    def outbound_simulation(
            self,
            adjust_uph: int = 1):
        '''
        Method để ghi nhận việc simulation volume Đơn hàng được Kho xử lý xong trong từng giờ
        '''
        # self.gen_df_forecast()
        df_outbound_simulation = pd.DataFrame()
        df_backlog_volume_self_temp = copy.deepcopy(self.df_backlog_volume)
        df_forecast = copy.deepcopy(self.df_forecast.drop(columns=[
            'day_type',
            'exsd_date',
            'exsd_hour',
            'sla_platform_date',
            'sla_platform_hour'
            ]))
        df_capacity = copy.deepcopy(self.df_capacity)

        for i in np.sort(df_forecast['current_time'].unique()).astype('datetime64[us]'):
            df_created_volume = df_forecast.loc[df_forecast['current_time'] == i]

            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=FutureWarning)
                df_total_volume = pd.concat([df_backlog_volume_self_temp, df_created_volume], ignore_index=True)

            # để ghi nhận current_time cho df_backlog_volume_self_temp (còn df_created_volume thì đã có sẵn)
            df_total_volume['current_time'] = i

            df_capacity_temp = df_capacity.loc[df_capacity['working_hour'] == i]

            if df_capacity_temp.empty:
                capacity = 0
            else:
                capacity = df_capacity_temp['capacity'].item()
            df_total_volume['capacity'] = capacity * adjust_uph
            
            # chỉ sort by 'exsd' và 'created time' để thể hiện sự random đối với các đơn hàng của các Client khác nhau có chung mốc phát sinh này
            df_total_volume.sort_values(by=['exsd','created_time'], ignore_index=True, inplace=True)
            df_total_volume['cumulative_volume'] = df_total_volume['volume'].cumsum()

            df_total_volume['outbound'] = np.where(
                # case
                # when
                df_total_volume['cumulative_volume'] <= df_total_volume['capacity'],
                    # then
                    df_total_volume['volume'],
                    np.where(
                # when
                (df_total_volume['cumulative_volume'] > df_total_volume['capacity']) & ((df_total_volume['cumulative_volume'] - df_total_volume['volume']) <= df_total_volume['capacity']),
                    # then
                    df_total_volume['capacity'] - (df_total_volume['cumulative_volume'] - df_total_volume['volume']),
                    # else
                    0))
            df_total_volume['backlog'] = df_total_volume['volume'] - df_total_volume['outbound']

            df_backlog_volume_temp = df_total_volume.drop(
                columns=[
                    'volume',
                    'capacity',
                    'cumulative_volume',
                    'outbound',
                    'current_time']
                    ).rename(columns={'backlog':'volume'})
            
            df_backlog_volume_temp['current_time'] = np.nan
            df_backlog_volume_self_temp = df_backlog_volume_temp[[
                'current_time',
                'created_time_date',
                'created_time_hour',
                'created_time',
                'warehouse',
                'client',
                # 'sub_client',
                'channel',
                'exsd',
                # 'exsd_date',
                # 'exsd_hour',
                'sla_platform',
                # 'sla_platform_date',
                # 'sla_platform_hour',
                'unit_per_order',
                'volume']]
            
            # UPDATE OPTIMIZATION - App v1.2: Remove các transaction đã không còn backlog (để không còn quét qua ở các vòng lặp sau)
            df_backlog_volume_self_temp = df_backlog_volume_self_temp.loc[df_backlog_volume_self_temp['volume'] > 0]
            # ----------------

            df_total_volume.sort_values(by=['current_time','created_time'], ignore_index=True, inplace=True)

            df_outbound_simulation = pd.concat([df_outbound_simulation, df_total_volume], ignore_index=True)
        
        df_outbound_simulation.insert(1, 'current_time_date', df_outbound_simulation['current_time'].dt.date)
        df_outbound_simulation['current_time_date'] = pd.to_datetime(df_outbound_simulation['current_time_date'])
        df_outbound_simulation.insert(2, 'current_time_hour', df_outbound_simulation['current_time'].dt.hour)

        return df_outbound_simulation
    
    def exsd_ontime_simulation(
            self,
            adjust_uph: int = 1,
            attribute: list = [
                'current_time',
                'current_time_date',
                'current_time_hour',
                'created_time', 
                'created_time_date',
                'created_time_hour',
                'client',
                # 'sub_client',
                'channel']):
        '''
        Method để tính %Ontime ExSD dựa trên các input Forecast và Capacity tương ứng
        '''
        df_outbound_simulation = self.outbound_simulation(adjust_uph=adjust_uph)
        df_outbound_simulation['ontime_exsd'] = np.where(df_outbound_simulation['current_time'] < df_outbound_simulation['exsd'], True, False)
        
        df_exsd_ontime_simulation = pd.pivot_table(df_outbound_simulation, values='outbound', index=attribute, columns=['ontime_exsd'], aggfunc={'outbound':'sum'}, fill_value=0)
        
        df_exsd_ontime_simulation.reset_index(inplace=True)
        df_exsd_ontime_simulation.rename_axis(None, axis=1, inplace=True)
        df_exsd_ontime_simulation.rename(columns={False:'volume_late_exsd', True:'volume_ontime_exsd'}, inplace=True)

        try:
            df_exsd_ontime_simulation['volume_late_exsd']
        except:
            df_exsd_ontime_simulation['volume_late_exsd'] = 0

        try:
            df_exsd_ontime_simulation['volume_ontime_exsd']
        except:
            df_exsd_ontime_simulation['volume_ontime_exsd'] = 0

        df_exsd_ontime_simulation['volume_total'] = df_exsd_ontime_simulation['volume_ontime_exsd'] + df_exsd_ontime_simulation['volume_late_exsd']

        return df_exsd_ontime_simulation
    
    def sla_platform_ontime_simulation(
            self,
            adjust_uph: int = 1,
            attribute: list = [
                'current_time',
                'current_time_date',
                'current_time_hour',
                'created_time', 
                'created_time_date',
                'created_time_hour',
                'client',
                # 'sub_client',
                'channel']) -> pd.DataFrame:
        """Method để tính toán ra tỉ lệ Ontime SLA Platform dựa trên input Forecast và Capacity tương ứng

        Args:
            attribute (list, optional): [description]. Defaults to ['current_time', 'current_time_date', 'current_time_hour', 'created_time', 'created_time_date', 'created_time_hour', 'client', 'channel'].

        Returns:
            pd.DataFrame: [description]
        """
        df_outbound_simulation = self.outbound_simulation(adjust_uph=adjust_uph)
        df_outbound_simulation['ontime_sla_platform'] = np.where(df_outbound_simulation['current_time'] < df_outbound_simulation['sla_platform'], True, False)
        
        df_sla_platform_ontime_simulation = pd.pivot_table(df_outbound_simulation, values='outbound', index=attribute, columns=['ontime_sla_platform'], aggfunc={'outbound':'sum'}, fill_value=0)
        
        df_sla_platform_ontime_simulation.reset_index(inplace=True)
        df_sla_platform_ontime_simulation.rename_axis(None, axis=1, inplace=True)
        df_sla_platform_ontime_simulation.rename(columns={False:'volume_late_sla_platform', True:'volume_ontime_sla_platform'}, inplace=True)

        try:
            df_sla_platform_ontime_simulation['volume_late_sla_platform']
        except:
            df_sla_platform_ontime_simulation['volume_late_sla_platform'] = 0

        try:
            df_sla_platform_ontime_simulation['volume_ontime_sla_platform']
        except:
            df_sla_platform_ontime_simulation['volume_ontime_sla_platform'] = 0

        df_sla_platform_ontime_simulation['volume_total'] = df_sla_platform_ontime_simulation['volume_ontime_sla_platform'] + df_sla_platform_ontime_simulation['volume_late_sla_platform']

        return df_sla_platform_ontime_simulation
    
    def outbound_simulation_summary(self):
        '''
        Method để summary lượng Volume Outbound (Unit & Order) theo từng khung giờ (như format gửi cho Onpoint)
        '''
        df_outbound_simulation = self.outbound_simulation()
        df_outbound_simulation['unit_outbound'] = df_outbound_simulation['outbound']
        df_outbound_simulation['order_outbound'] = df_outbound_simulation['outbound']/df_outbound_simulation['unit_per_order']
        df_outbound_simulation_summary = pd.pivot_table(df_outbound_simulation, values=['unit_outbound', 'order_outbound'], index=['current_time'], columns=['client', 'channel'], aggfunc={'unit_outbound':'sum', 'order_outbound':'sum'}, fill_value=0,)

        return df_outbound_simulation_summary

    def backlog_progress(self):
        '''
        Method để check lượng Backlog còn lại ở mỗi khung giờ (cùng với MAB của khung giờ đó)
        '''
        df_outbound_simulation = self.outbound_simulation()
        df_maximum_allowable_backlog_exsd = self.maximum_allowable_backlog_exsd()
        df_maximum_allowable_backlog_sla_platform = self.maximum_allowable_backlog_sla_platform()

        df_backlog = df_outbound_simulation.groupby([
            'current_time',
            'current_time_date',
            'current_time_hour',
            'created_time',
            'created_time_date',
            'created_time_hour',
            'warehouse',
            'client',
            # 'sub_client',
            'channel',
            'exsd',
            # 'exsd_date',
            # 'exsd_hour',
            'sla_platform',
            # 'sla_platform_date',
            # 'sla_platform_hour'
            ], dropna=False).agg({'backlog':'sum'})
        df_backlog.reset_index(inplace=True)

        df_maximum_allowable_backlog_exsd.drop(columns=['unit_per_order'], inplace=True)
        df_maximum_allowable_backlog_sla_platform.drop(columns=['unit_per_order'], inplace=True)
        
        # vì "df_backlog" đang là lượng Backlog ở từng 'current_time' theo từng 'created_time', do đó cần join với data MAB ở level detail tương ứng (thay vì 'df_maximum_allowable_backlog_exsd_summary')
        df_backlog_progress = df_backlog.merge(
            df_maximum_allowable_backlog_exsd,
            # UPDATE v1.5: change from "left join" to "full join" among 3 info "Actual Backlog - MAB ExSD - MAB SLA Platform", due to the update v1.2 (remove duplicate created_time which has 0 backlog so as not to include in the next loop)
            # how='left',
            how='outer',
            on=[
                'current_time',
                'current_time_date',
                'current_time_hour',
                'created_time',
                'created_time_date',
                'created_time_hour',
                'warehouse',
                'client',
                # 'sub_client',
                'channel',
                'exsd',
                # 'exsd_date',
                # 'exsd_hour',
                'sla_platform',
                # 'sla_platform_date',
                # 'sla_platform_hour'
                ]).merge(
            df_maximum_allowable_backlog_sla_platform,
            # UPDATE v1.5: change from "left join" to "full join" among 3 info "Actual Backlog - MAB ExSD - MAB SLA Platform", due to the update v1.2 (remove duplicate created_time which has 0 backlog so as not to include in the next loop)
            # how='left',
            how='outer',
            on=[
                'current_time',
                'current_time_date',
                'current_time_hour',
                'created_time',
                'created_time_date',
                'created_time_hour',
                'warehouse',
                'client',
                # 'sub_client',
                'channel',
                'exsd',
                # 'exsd_date',
                # 'exsd_hour',
                'sla_platform',
                # 'sla_platform_date',
                # 'sla_platform_hour'
                ])

        df_backlog_progress['backlog'] = np.where(df_backlog_progress['backlog'].isna(), 0, df_backlog_progress['backlog'])
        df_backlog_progress['maximum_allowable_backlog_exsd'] = np.where(df_backlog_progress['maximum_allowable_backlog_exsd'].isna(), 0, df_backlog_progress['maximum_allowable_backlog_exsd'])
        df_backlog_progress['maximum_allowable_backlog_sla_platform'] = np.where(df_backlog_progress['maximum_allowable_backlog_sla_platform'].isna(), 0, df_backlog_progress['maximum_allowable_backlog_sla_platform'])

        # df_backlog_progress['backlog_exceed_exsd'] = np.where(df_backlog_progress['backlog'] <= df_backlog_progress['maximum_allowable_backlog_exsd'], False, True)
        # df_backlog_progress['backlog_exceed_sla_platform'] = np.where(df_backlog_progress['backlog'] <= df_backlog_progress['maximum_allowable_backlog_sla_platform'], False, True)
        # df_backlog_progress['current_time_date'] = pd.to_datetime(df_backlog_progress['current_time_date'])
        
        return df_backlog_progress
    
    def backlog_progress_summary(self):
        df_backlog_progress = self.backlog_progress()
        df_backlog_progress_backlog = copy.deepcopy(df_backlog_progress)
        df_backlog_progress_mab_exsd = copy.deepcopy(df_backlog_progress)
        df_backlog_progress_mab_sla_platform = copy.deepcopy(df_backlog_progress)

        df_backlog_progress_backlog['type'] = 'Backlog'
        df_backlog_progress_backlog['volume'] = df_backlog_progress_backlog['backlog']
        
        df_backlog_progress_mab_exsd['type'] = 'MAB ExSD'
        df_backlog_progress_mab_exsd['volume'] = df_backlog_progress_backlog['maximum_allowable_backlog_exsd']

        df_backlog_progress_mab_sla_platform['type'] = 'MAB SLA Platform'
        df_backlog_progress_mab_sla_platform['volume'] = df_backlog_progress_mab_sla_platform['maximum_allowable_backlog_sla_platform']
        
        df_backlog_progress_summary = pd.concat([df_backlog_progress_backlog, df_backlog_progress_mab_exsd, df_backlog_progress_mab_sla_platform], ignore_index=True)
        df_backlog_progress_summary.drop(columns=[
            'backlog',
            'maximum_allowable_backlog_exsd',
            'maximum_allowable_backlog_sla_platform',
            # 'backlog_exceed_exsd',
            # 'backlog_exceed_sla_platform'
            ], inplace=True)
        
        return df_backlog_progress_summary

    def end_time(self):
        # self.gen_df_forecast()
        df_outbound_simulation = self.outbound_simulation()

        df_end_time_temp1 = copy.deepcopy(self.df_forecast)
        df_end_time_temp1['created_time_temp'] = df_end_time_temp1['created_time'] + timedelta(hours=1)
        df_end_time1 = df_end_time_temp1.groupby([
            'client',
            # 'sub_client',
            'channel',
            'exsd',
            'sla_platform']
            ).agg({
                'volume':'sum',
                'created_time':'min',
                'created_time_temp':'max'}
                ).rename(columns={
                    'created_time':'from_created_time',
                    'created_time_temp':'to_created_time'})
        df_end_time1.reset_index(inplace=True)
        
        df_end_time_temp2 = copy.deepcopy(df_outbound_simulation.loc[df_outbound_simulation['outbound'] > 0])
        df_end_time_temp2['current_time_temp'] = df_end_time_temp2['current_time'] + timedelta(hours=1)
        df_end_time2 = df_end_time_temp2.groupby([
            'client',
            # 'sub_client',
            'channel',
            'exsd',
            'sla_platform']
            ).agg({
                'current_time':'min',
                'current_time_temp':'max'}
                ).rename(columns={
                    'current_time':'start_time',
                    'current_time_temp':'end_time'})
        df_end_time2.reset_index(inplace=True)
        
        df_end_time = pd.merge(
            df_end_time1, 
            df_end_time2,
            how='left',
            on=[
                'client',
                # 'sub_client',
                'channel',
                'exsd',
                'sla_platform'])
        df_end_time.sort_values(
            by=[
                'client',
                # 'sub_client',
                'channel',
                'from_created_time',
                'to_created_time'], 
            ignore_index=True,
            inplace=True)
        
        return df_end_time
    
    def fundamental_report(self):
        # self.gen_df_forecast()
        df_forecast = copy.deepcopy(self.df_forecast.drop(columns=['day_type']))
        df_capacity = copy.deepcopy(self.df_capacity)
        df_backlog_progress = self.backlog_progress()
        df_outbound_simulation_summary = self.outbound_simulation_summary().drop(columns=['order_outbound'])

        df_forecast_fundamental_report = pd.pivot_table(
            df_forecast, 
            values=['volume', 'exsd', 'sla_platform'],
            index=['created_time'],
            columns=['client','channel'],
            aggfunc={'volume':'sum', 'exsd':'max', 'sla_platform':'max'},
            fill_value=0)
        
        df_forecast_fundamental_report.columns = df_forecast_fundamental_report.columns.to_flat_index().str.join('_')
        df_forecast_fundamental_report.reset_index(inplace=True)

        df_outbound_simulation_summary.columns = df_outbound_simulation_summary.columns.to_flat_index().str.join('_')
        df_outbound_simulation_summary.reset_index(inplace=True)

        df_backlog_progress_fundamental_report = pd.pivot_table(
            df_backlog_progress,
            values=['backlog','maximum_allowable_backlog_exsd','maximum_allowable_backlog_sla_platform'],
            index=['current_time'],
            columns=['client','channel'],
            aggfunc={'backlog':'sum', 'maximum_allowable_backlog_exsd':'sum', 'maximum_allowable_backlog_sla_platform':'sum'}, 
            fill_value=0)
        
        df_backlog_progress_fundamental_report.columns = df_backlog_progress_fundamental_report.columns.to_flat_index().str.join('_')
        df_backlog_progress_fundamental_report.reset_index(inplace=True)
        
        df_fundamental_report_temp = df_forecast_fundamental_report.merge(
            df_capacity,
            how='outer',
            left_on='created_time',
            right_on='working_hour',
            suffixes=['_forecast','_capacity']
            ).merge(
                df_outbound_simulation_summary,
                how='outer',
                left_on='created_time',
                right_on='current_time',
                suffixes=['_forecast','_outbound']
                ).merge(
                    df_backlog_progress_fundamental_report,
                    how='outer',
                    left_on='created_time',
                    right_on='current_time',
                    suffixes=['_outbound','_backlog'])

        df_fundamental_report_temp['number_of_working_picker'] = df_fundamental_report_temp['number_of_fte_picker'] + df_fundamental_report_temp['number_of_ow_picker']
        df_fundamental_report_temp['number_of_working_packer'] = df_fundamental_report_temp['number_of_fte_packer'] + df_fundamental_report_temp['number_of_ow_packer']
        df_fundamental_report_temp['number_of_working_staff'] = df_fundamental_report_temp['number_of_working_picker'] + df_fundamental_report_temp['number_of_working_packer']

        df_fundamental_report_temp['volume_TikiCorp_Total'] = df_fundamental_report_temp['volume_TikiCorp_Others']
        
        df_fundamental_report_temp['volume_Onpoint_Total'] = df_fundamental_report_temp['volume_Onpoint_Lazada'] + df_fundamental_report_temp['volume_Onpoint_Others'] + df_fundamental_report_temp['volume_Onpoint_Shopee'] + df_fundamental_report_temp['volume_Onpoint_Tiki'] + df_fundamental_report_temp['volume_Onpoint_Tiktokshop']

        df_fundamental_report_temp['volume_HappySkin_Total'] = df_fundamental_report_temp['volume_HappySkin_Lazada'] + df_fundamental_report_temp['volume_HappySkin_Others'] + df_fundamental_report_temp['volume_HappySkin_Shopee'] + df_fundamental_report_temp['volume_HappySkin_Tiki'] + df_fundamental_report_temp['volume_HappySkin_Tiktokshop']

        df_fundamental_report_temp['volume_Others_Total'] = df_fundamental_report_temp['volume_Others_Lazada'] + df_fundamental_report_temp['volume_Others_Others'] + df_fundamental_report_temp['volume_Others_Shopee'] + df_fundamental_report_temp['volume_Others_Tiki'] + df_fundamental_report_temp['volume_Others_Tiktokshop']

        df_fundamental_report_temp['volume_Total'] = df_fundamental_report_temp['volume_TikiCorp_Total'] + df_fundamental_report_temp['volume_Onpoint_Total'] + df_fundamental_report_temp['volume_HappySkin_Total'] + df_fundamental_report_temp['volume_Others_Total']
        # -----
        df_fundamental_report_temp['unit_outbound_TikiCorp_Total'] = df_fundamental_report_temp['unit_outbound_TikiCorp_Others']
        
        df_fundamental_report_temp['unit_outbound_Onpoint_Total'] = df_fundamental_report_temp['unit_outbound_Onpoint_Lazada'] + df_fundamental_report_temp['unit_outbound_Onpoint_Others'] + df_fundamental_report_temp['unit_outbound_Onpoint_Shopee'] + df_fundamental_report_temp['unit_outbound_Onpoint_Tiki'] + df_fundamental_report_temp['unit_outbound_Onpoint_Tiktokshop']

        df_fundamental_report_temp['unit_outbound_HappySkin_Total'] = df_fundamental_report_temp['unit_outbound_HappySkin_Lazada'] + df_fundamental_report_temp['unit_outbound_HappySkin_Others'] + df_fundamental_report_temp['unit_outbound_HappySkin_Shopee'] + df_fundamental_report_temp['unit_outbound_HappySkin_Tiki'] + df_fundamental_report_temp['unit_outbound_HappySkin_Tiktokshop']

        df_fundamental_report_temp['unit_outbound_Others_Total'] = df_fundamental_report_temp['unit_outbound_Others_Lazada'] + df_fundamental_report_temp['unit_outbound_Others_Others'] + df_fundamental_report_temp['unit_outbound_Others_Shopee'] + df_fundamental_report_temp['unit_outbound_Others_Tiki'] + df_fundamental_report_temp['unit_outbound_Others_Tiktokshop']

        df_fundamental_report_temp['unit_outbound_Total'] = df_fundamental_report_temp['unit_outbound_TikiCorp_Total'] + df_fundamental_report_temp['unit_outbound_Onpoint_Total'] + df_fundamental_report_temp['unit_outbound_HappySkin_Total'] + df_fundamental_report_temp['unit_outbound_Others_Total']
        # -----
        # -----
        df_fundamental_report_temp['backlog_TikiCorp_Total'] = df_fundamental_report_temp['backlog_TikiCorp_Others']
        
        df_fundamental_report_temp['backlog_Onpoint_Total'] = df_fundamental_report_temp['backlog_Onpoint_Lazada'] + df_fundamental_report_temp['backlog_Onpoint_Others'] + df_fundamental_report_temp['backlog_Onpoint_Shopee'] + df_fundamental_report_temp['backlog_Onpoint_Tiki'] + df_fundamental_report_temp['backlog_Onpoint_Tiktokshop']

        df_fundamental_report_temp['backlog_HappySkin_Total'] = df_fundamental_report_temp['backlog_HappySkin_Lazada'] + df_fundamental_report_temp['backlog_HappySkin_Others'] + df_fundamental_report_temp['backlog_HappySkin_Shopee'] + df_fundamental_report_temp['backlog_HappySkin_Tiki'] + df_fundamental_report_temp['backlog_HappySkin_Tiktokshop']

        df_fundamental_report_temp['backlog_Others_Total'] = df_fundamental_report_temp['backlog_Others_Lazada'] + df_fundamental_report_temp['backlog_Others_Others'] + df_fundamental_report_temp['backlog_Others_Shopee'] + df_fundamental_report_temp['backlog_Others_Tiki'] + df_fundamental_report_temp['backlog_Others_Tiktokshop']

        df_fundamental_report_temp['backlog_Total'] = df_fundamental_report_temp['backlog_TikiCorp_Total'] + df_fundamental_report_temp['backlog_Onpoint_Total'] + df_fundamental_report_temp['backlog_HappySkin_Total'] + df_fundamental_report_temp['backlog_Others_Total']
        # -----
        # -----
        df_fundamental_report_temp['maximum_allowable_backlog_exsd_TikiCorp_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_exsd_TikiCorp_Others']
        
        df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Lazada'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Others'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Shopee'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Tiki'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Tiktokshop']

        df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Lazada'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Others'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Shopee'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Tiki'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Tiktokshop']

        df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Lazada'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Others'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Shopee'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Tiki'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Tiktokshop']

        df_fundamental_report_temp['maximum_allowable_backlog_exsd_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_exsd_TikiCorp_Total'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Onpoint_Total'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_HappySkin_Total'] + df_fundamental_report_temp['maximum_allowable_backlog_exsd_Others_Total']
        # -----
        # -----
        df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_TikiCorp_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_TikiCorp_Others']
        
        df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Lazada'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Others'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Shopee'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Tiki'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Tiktokshop']

        df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Lazada'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Others'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Shopee'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Tiki'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Tiktokshop']

        df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Lazada'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Others'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Shopee'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Tiki'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Tiktokshop']

        df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Total'] = df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_TikiCorp_Total'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Onpoint_Total'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_HappySkin_Total'] + df_fundamental_report_temp['maximum_allowable_backlog_sla_platform_Others_Total']

        df_fundamental_report_temp['wasted_capacity'] = df_fundamental_report_temp['capacity'] - df_fundamental_report_temp['unit_outbound_Total']
        # -----
        df_fundamental_report_temp['time'] = np.where(df_fundamental_report_temp['created_time'].notna(), 
                                               df_fundamental_report_temp['created_time'], 
                                               np.where(df_fundamental_report_temp['working_hour'].notna(), 
                                                        df_fundamental_report_temp['working_hour'],
                                                        np.where(df_fundamental_report_temp['current_time_outbound'].notna(),
                                                                 df_fundamental_report_temp['current_time_outbound'],
                                                                 df_fundamental_report_temp['current_time_backlog'])))
        
        df_fundamental_report = df_fundamental_report_temp[[
            'time',
            'volume_TikiCorp_Others',
            'volume_TikiCorp_Total',
            'volume_Onpoint_Tiki',
            'volume_Onpoint_Lazada',
            'volume_Onpoint_Shopee',
            'volume_Onpoint_Tiktokshop',
            'volume_Onpoint_Others',
            'volume_Onpoint_Total',
            
            'volume_HappySkin_Tiki',
            'volume_HappySkin_Lazada',
            'volume_HappySkin_Shopee',
            'volume_HappySkin_Tiktokshop',
            'volume_HappySkin_Others',
            'volume_HappySkin_Total',

            'volume_Others_Tiki',
            'volume_Others_Lazada',
            'volume_Others_Shopee',
            'volume_Others_Tiktokshop',
            'volume_Others_Others',
            'volume_Others_Total',
            'volume_Total',
            'exsd_TikiCorp_Others',
            'exsd_Onpoint_Tiki',
            'exsd_Onpoint_Lazada',
            'exsd_Onpoint_Shopee',
            'exsd_Onpoint_Tiktokshop',
            'exsd_Onpoint_Others',

            'exsd_HappySkin_Tiki',
            'exsd_HappySkin_Lazada',
            'exsd_HappySkin_Shopee',
            'exsd_HappySkin_Tiktokshop',
            'exsd_HappySkin_Others',

            'exsd_Others_Tiki',
            'exsd_Others_Lazada',
            'exsd_Others_Shopee',
            'exsd_Others_Tiktokshop',
            'exsd_Others_Others',
            'sla_platform_TikiCorp_Others',
            'sla_platform_Onpoint_Tiki',
            'sla_platform_Onpoint_Lazada',
            'sla_platform_Onpoint_Shopee',
            'sla_platform_Onpoint_Tiktokshop',
            'sla_platform_Onpoint_Others',

            'sla_platform_HappySkin_Tiki',
            'sla_platform_HappySkin_Lazada',
            'sla_platform_HappySkin_Shopee',
            'sla_platform_HappySkin_Tiktokshop',
            'sla_platform_HappySkin_Others',

            'sla_platform_Others_Tiki',
            'sla_platform_Others_Lazada',
            'sla_platform_Others_Shopee',
            'sla_platform_Others_Tiktokshop',
            'sla_platform_Others_Others',
            'day_type',
            'uph_fte_pick',
            'uph_ow_pick',
            'uph_fte_pack',
            'uph_ow_pack',
            'number_of_fte',
            'number_of_ow',
            'number_of_fte_picker',
            'number_of_ow_picker',
            'number_of_fte_packer',
            'number_of_ow_packer',
            'number_of_working_picker',
            'number_of_working_packer',
            'number_of_working_staff',
            'excess_staff',
            'capacity_fte_pick',
            'capacity_ow_pick',
            'capacity_fte_pack',
            'capacity_ow_pack',
            'capacity_total_pick',
            'capacity_total_pack',
            'capacity',
            'unit_outbound_TikiCorp_Others',
            'unit_outbound_TikiCorp_Total',
            'unit_outbound_Onpoint_Tiki',
            'unit_outbound_Onpoint_Lazada',
            'unit_outbound_Onpoint_Shopee',
            'unit_outbound_Onpoint_Tiktokshop',
            'unit_outbound_Onpoint_Others',
            'unit_outbound_Onpoint_Total',

            'unit_outbound_HappySkin_Tiki',
            'unit_outbound_HappySkin_Lazada',
            'unit_outbound_HappySkin_Shopee',
            'unit_outbound_HappySkin_Tiktokshop',
            'unit_outbound_HappySkin_Others',
            'unit_outbound_HappySkin_Total',

            'unit_outbound_Others_Tiki',
            'unit_outbound_Others_Lazada',
            'unit_outbound_Others_Shopee',
            'unit_outbound_Others_Tiktokshop',
            'unit_outbound_Others_Others',
            'unit_outbound_Others_Total',
            'unit_outbound_Total',
            'wasted_capacity',
            'backlog_TikiCorp_Others',
            'backlog_TikiCorp_Total',
            'backlog_Onpoint_Tiki',
            'backlog_Onpoint_Lazada',
            'backlog_Onpoint_Shopee',
            'backlog_Onpoint_Tiktokshop',
            'backlog_Onpoint_Others',
            'backlog_Onpoint_Total',

            'backlog_HappySkin_Tiki',
            'backlog_HappySkin_Lazada',
            'backlog_HappySkin_Shopee',
            'backlog_HappySkin_Tiktokshop',
            'backlog_HappySkin_Others',
            'backlog_HappySkin_Total',

            'backlog_Others_Tiki',
            'backlog_Others_Lazada',
            'backlog_Others_Shopee',
            'backlog_Others_Tiktokshop',
            'backlog_Others_Others',
            'backlog_Others_Total',
            'backlog_Total',
            'maximum_allowable_backlog_exsd_TikiCorp_Others',
            'maximum_allowable_backlog_exsd_TikiCorp_Total',
            'maximum_allowable_backlog_exsd_Onpoint_Tiki',
            'maximum_allowable_backlog_exsd_Onpoint_Lazada',
            'maximum_allowable_backlog_exsd_Onpoint_Shopee',
            'maximum_allowable_backlog_exsd_Onpoint_Tiktokshop',
            'maximum_allowable_backlog_exsd_Onpoint_Others',
            'maximum_allowable_backlog_exsd_Onpoint_Total',

            'maximum_allowable_backlog_exsd_HappySkin_Tiki',
            'maximum_allowable_backlog_exsd_HappySkin_Lazada',
            'maximum_allowable_backlog_exsd_HappySkin_Shopee',
            'maximum_allowable_backlog_exsd_HappySkin_Tiktokshop',
            'maximum_allowable_backlog_exsd_HappySkin_Others',
            'maximum_allowable_backlog_exsd_HappySkin_Total',

            'maximum_allowable_backlog_exsd_Others_Tiki',
            'maximum_allowable_backlog_exsd_Others_Lazada',
            'maximum_allowable_backlog_exsd_Others_Shopee',
            'maximum_allowable_backlog_exsd_Others_Tiktokshop',
            'maximum_allowable_backlog_exsd_Others_Others',
            'maximum_allowable_backlog_exsd_Others_Total',
            'maximum_allowable_backlog_exsd_Total',
            'maximum_allowable_backlog_sla_platform_TikiCorp_Others',
            'maximum_allowable_backlog_sla_platform_TikiCorp_Total',
            'maximum_allowable_backlog_sla_platform_Onpoint_Tiki',
            'maximum_allowable_backlog_sla_platform_Onpoint_Lazada',
            'maximum_allowable_backlog_sla_platform_Onpoint_Shopee',
            'maximum_allowable_backlog_sla_platform_Onpoint_Tiktokshop',
            'maximum_allowable_backlog_sla_platform_Onpoint_Others',
            'maximum_allowable_backlog_sla_platform_Onpoint_Total',

            'maximum_allowable_backlog_sla_platform_HappySkin_Tiki',
            'maximum_allowable_backlog_sla_platform_HappySkin_Lazada',
            'maximum_allowable_backlog_sla_platform_HappySkin_Shopee',
            'maximum_allowable_backlog_sla_platform_HappySkin_Tiktokshop',
            'maximum_allowable_backlog_sla_platform_HappySkin_Others',
            'maximum_allowable_backlog_sla_platform_HappySkin_Total',

            'maximum_allowable_backlog_sla_platform_Others_Tiki',
            'maximum_allowable_backlog_sla_platform_Others_Lazada',
            'maximum_allowable_backlog_sla_platform_Others_Shopee',
            'maximum_allowable_backlog_sla_platform_Others_Tiktokshop',
            'maximum_allowable_backlog_sla_platform_Others_Others',
            'maximum_allowable_backlog_sla_platform_Others_Total',
            'maximum_allowable_backlog_sla_platform_Total']]
        
        df_fundamental_report = df_fundamental_report.sort_values(by=['time'])

        return df_fundamental_report

    def sensitivity_analysis(self) -> pd.DataFrame:
        sens_range = [-0.25, -0.20, -0.15, -0.10, -0.05, 0, 0.05, 0.10, 0.15, 0.20, 0.25]
        df_sensitivity_analysis = pd.DataFrame()

        for i in sens_range:
            self.log_forecast.adjust(ratio_order=1+i, ratio_unit=1+i)
            self.gen_df_forecast()            

            for j in sens_range:
                df_sensitivity_analysis_tmp = pd.DataFrame()
                # create a list for first value for blank dataframe
                df_sensitivity_analysis_tmp['forecast'] = [('+' if i > 0 else '' if i == 0 else '-') + ('' if abs(i) >= 0.1 else '0') + str(int(abs(i)*100)) + '%']
                df_sensitivity_analysis_tmp['uph'] = ('+' if j > 0 else '' if j == 0 else '-') + ('' if abs(j) >= 0.1 else '0') + str(int(abs(j)*100)) + '%'

                df_ontime_exsd = self.exsd_ontime_simulation(adjust_uph=1+j)
                df_ontime_sla_platform = self.sla_platform_ontime_simulation(adjust_uph=1+j)

                ontime_exsd = round(df_ontime_exsd['volume_ontime_exsd'].sum()/df_ontime_exsd['volume_total'].sum(),2)
                ontime_sla_platform = round(df_ontime_sla_platform['volume_ontime_sla_platform'].sum()/df_ontime_sla_platform['volume_total'].sum(),2)

                df_sensitivity_analysis_tmp['ontime_exsd'] = ontime_exsd
                df_sensitivity_analysis_tmp['ontime_sla_platform'] = ontime_sla_platform

                df_sensitivity_analysis = pd.concat([df_sensitivity_analysis, df_sensitivity_analysis_tmp], ignore_index=True)

            self.log_forecast.reset()
            self.gen_df_forecast()

        return df_sensitivity_analysis

    def plan_assessment(self):
        # METRIC (The more the better):
        # SCORE:
        # Pool (p) = AVG(SLA - DT)
        # Safety (s) = AVG(SLA - ExSD)
        # Effort (e) = AVG(ExSD - DT)
        # COUNTER METRIC (The less the better):
        # COST
        # DT ------ ExSD ------- SLA
        # |   Effort  |   Safety  |
        # |         Pool          |
        df_end_time = self.end_time()
        df_ontime_exsd = self.exsd_ontime_simulation()
        df_ontime_sla_platform = self.sla_platform_ontime_simulation()
        df_fundamental_report = self.fundamental_report()
        total_staff_fte = self.total_staff_fte()
        total_staff_ow = self.total_staff_ow()
        total_cost = self.total_cost()
        # -----------
        df_end_time['p_score'] = df_end_time['sla_platform'] - df_end_time['end_time']
        df_end_time['p_score'] = df_end_time['p_score'].dt.total_seconds()/3600
        df_end_time['s_score'] = df_end_time['sla_platform'] - df_end_time['exsd']
        df_end_time['s_score'] = df_end_time['s_score'].dt.total_seconds()/3600
        df_end_time['e_score'] = df_end_time['exsd'] - df_end_time['end_time']
        df_end_time['e_score'] = df_end_time['e_score'].dt.total_seconds()/3600

        try:
            p_score_avg = round(df_end_time['p_score'].mean())
        except:
            p_score_avg = np.nan

        try:
            s_score_avg = round(df_end_time['s_score'].mean())
        except:
            s_score_avg = np.nan

        try:
            e_score_avg = round(df_end_time['e_score'].mean())
        except:
            e_score_avg = np.nan

        try:
            df_end_time['p_score_avg_weighted'] = df_end_time['p_score'] * df_end_time['volume']
            p_score_avg_weighted = round(df_end_time['p_score_avg_weighted'].sum()/df_end_time['volume'].sum())
        except:
            p_score_avg_weighted = np.nan

        try:
            df_end_time['s_score_avg_weighted'] = df_end_time['s_score'] * df_end_time['volume']
            s_score_avg_weighted = round(df_end_time['s_score_avg_weighted'].sum()/df_end_time['volume'].sum())
        except:
            s_score_avg_weighted = np.nan

        try:
            df_end_time['e_score_avg_weighted'] = df_end_time['e_score'] * df_end_time['volume']
            e_score_avg_weighted = round(df_end_time['e_score_avg_weighted'].sum()/df_end_time['volume'].sum())
        except:
            e_score_avg_weighted = np.nan

        try:
            p_score_tp25 = round(df_end_time['p_score'].quantile(0.25))
        except:
            p_score_tp25 = np.nan

        try:
            s_score_tp25 = round(df_end_time['s_score'].quantile(0.25))
        except:
            s_score_tp25 = np.nan

        try:
            e_score_tp25 = round(df_end_time['e_score'].quantile(0.25))
        except:
            e_score_tp25 = np.nan
        
        try:
            p_score_tp50 = round(df_end_time['p_score'].quantile(0.5))
        except:
            p_score_tp50 = np.nan

        try:   
            s_score_tp50 = round(df_end_time['s_score'].quantile(0.5))
        except:
            s_score_tp50 = np.nan
        
        try:
            e_score_tp50 = round(df_end_time['e_score'].quantile(0.5))
        except:
            e_score_tp50 = np.nan

        try:
            p_score_tp75 = round(df_end_time['p_score'].quantile(0.75))
        except:
            p_score_tp75 = np.nan

        try:
            s_score_tp75 = round(df_end_time['s_score'].quantile(0.75))
        except:
            s_score_tp75 = np.nan

        try:
            e_score_tp75 = round(df_end_time['e_score'].quantile(0.75))
        except:
            e_score_tp75 = np.nan

        try:
            p_score_negative = df_end_time['p_score'].loc[df_end_time['p_score'] < 0].count()
        except:
            p_score_negative = np.nan

        try:
            e_score_negative = df_end_time['e_score'].loc[df_end_time['e_score'] < 0].count()
        except:
            e_score_negative = np.nan
        # -------
        ontime_exsd = round(df_ontime_exsd['volume_ontime_exsd'].sum()/df_ontime_exsd['volume_total'].sum()*100,2)
        ontime_sla_platform = round(df_ontime_sla_platform['volume_ontime_sla_platform'].sum()/df_ontime_sla_platform['volume_total'].sum()*100,2)
        # -------
        volume_forecast = df_fundamental_report['volume_Total'].sum()
        volume_outbound = df_fundamental_report['unit_outbound_Total'].sum()
        wasted_capacity = df_fundamental_report['wasted_capacity'].sum()

        number_of_excess_staff = df_fundamental_report['excess_staff'].sum()

        plan_assessment = {}
        for i in ['total_staff_fte',
                  'total_staff_ow',
                  'total_cost',
                  'p_score_avg',
                  's_score_avg',
                  'e_score_avg',
                  'p_score_tp25',
                  's_score_tp25',
                  'e_score_tp25',
                  'p_score_tp50',
                  's_score_tp50',
                  'e_score_tp50',
                  'p_score_tp75',
                  's_score_tp75',
                  'e_score_tp75',
                  'p_score_negative',
                  'e_score_negative',
                  'ontime_exsd',
                  'ontime_sla_platform',
                  'volume_forecast',
                  'volume_outbound',
                  'wasted_capacity',
                  'number_of_excess_staff']:
            plan_assessment[i] = eval(i)
        
        return plan_assessment
        # print(f"p-score: AVG = {p_score_avg}, WEIGHTED AVG = {p_score_avg_weighted}, TP25 = {p_score_tp25}, TP50 = {p_score_tp50}, TP75 = {p_score_tp75}")
        # print(f"e-score: AVG = {e_score_avg}, WEIGHTED AVG = {e_score_avg_weighted}, TP25 = {e_score_tp25}, TP50 = {e_score_tp50}, TP75 = {e_score_tp75}")
        # print(f"s-score: AVG = {s_score_avg}, WEIGHTED AVG = {s_score_avg_weighted}, TP25 = {s_score_tp25}, TP50 = {s_score_tp50}, TP75 = {s_score_tp75}")
        # print(f"Số p-score âm: {p_score_negative}")
        # print(f"Số e-score âm: {e_score_negative}")

    def export(self,
               data: list) -> None:
        """Method để export data ra Google Sheet tương ứng

        Args:
            data (list, optional): [description]. Defaults to ['MAB Detail', 'MAB Summary', 'Outbound Plan Raw', 'Ontime ExSD', 'Ontime SLA Platform', 'Outbound Plan Summary', 'Backlog Progress'].
        """
        # Opening JSON file
        file_key = open("key/dulcet-bliss-369609-ac8508571c0a.json")

        # returns JSON object as a dictionary
        config = json.load(file_key)

        # try:
        #     config = gspread_pandas.conf.get_config(conf_dir='/Users/lap-01102/Library/Mobile Documents/com~apple~CloudDocs/Desktop/Tiki/Jupyter Notebook', file_name='dulcet-bliss-369609-ac8508571c0a.json')
        # except:
        # config = gspread_pandas.conf.get_config(conf_dir='https://github.com/namnguyen8-tiki/headcount-planning/blob/main/', file_name='dulcet-bliss-369609-ac8508571c0a.json')
        spread = Spread(spread='1t5NIizsote5y8jdMdkctkKqnNSlVevbZX1YGPnwF9M8',config=config)
        
        if data != []:
            for i in data:
                if i == 'Forecast Raw':
                    spread.df_to_sheet(self.forecast(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Forecast Summary':
                    spread.df_to_sheet(self.forecast_summary(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'MAB ExSD Detail':
                    spread.df_to_sheet(self.maximum_allowable_backlog_exsd(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'MAB ExSD Summary':
                    spread.df_to_sheet(self.maximum_allowable_backlog_exsd_summary(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'MAB SLA Platform Detail':
                    spread.df_to_sheet(self.maximum_allowable_backlog_sla_platform(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'MAB SLA Platform Summary':
                    spread.df_to_sheet(self.maximum_allowable_backlog_sla_platform_summary(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Outbound Plan Raw':
                    spread.df_to_sheet(self.outbound_simulation(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Ontime ExSD':
                    spread.df_to_sheet(self.exsd_ontime_simulation(), index=True, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Ontime SLA Platform':
                    spread.df_to_sheet(self.sla_platform_ontime_simulation(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Outbound Plan Summary':
                    spread.df_to_sheet(self.outbound_simulation_summary(), index=True, merge_headers=True, merge_index=True, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Backlog Progress Raw':
                    spread.df_to_sheet(self.backlog_progress(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Backlog Progress Summary':
                    spread.df_to_sheet(self.backlog_progress_summary(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'End Time':
                    spread.df_to_sheet(self.end_time(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Sensitivity Analysis':
                    spread.df_to_sheet(self.sensitivity_analysis(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Plan Assessment':
                    spread.df_to_sheet(self.plan_assessment(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='A1', replace=True)
                elif i == 'Fundamental Report':
                    spread.df_to_sheet(self.fundamental_report(), index=False, freeze_headers=True, sheet=(str(self.df_warehouse) + ' ' + i), start='B3', replace=True)
                    spread.df_to_sheet(pd.DataFrame([self.df_warehouse]), index=False, headers=False, freeze_headers=False, sheet=(str(self.df_warehouse) + ' ' + i), start='A3', replace=False)
                    spread.df_to_sheet(pd.DataFrame([self.df_max_outbound_station]), index=False, headers=False, freeze_headers=False, sheet=(str(self.df_warehouse) + ' ' + i), start='AZ2', replace=False)
                else:
                    print('Không tìm thấy dữ liệu theo yêu cầu')
        
        file_key.close()

def main():
    SGN = Warehouse('SGN', 90)
    labor = (# 6/7
            Labor('FTE', 30, '2024-09-06 06:00:00', '2024-09-06 14:00:00', 'FTE ca sáng'),
            Labor('FTE', 22, '2024-09-06 14:00:00', '2024-09-06 22:00:00', 'FTE ca chiều'),
            Labor('FTE', 60, '2024-09-06 06:00:00', '2024-09-06 14:00:00', 'OW ca sáng'),
            Labor('FTE', 60, '2024-09-06 14:00:00', '2024-09-06 22:00:00', 'OW ca chiều'),
            # 7/7
            Labor('OW', 90, '2024-09-06 22:00:00', '2024-09-07 06:00:00', 'OW ca đêm'),
            Labor('OW', 126, '2024-09-07 06:00:00', '2024-09-07 14:00:00', 'OW ca đêm'),
            Labor('OW', 126, '2024-09-07 14:00:00', '2024-09-07 22:00:00', 'OW ca đêm'),
            Labor('FTE', 61, '2024-09-07 05:00:00', '2024-09-07 17:00:00', 'FTE ca sáng'),
            Labor('FTE', 23, '2024-09-07 10:00:00', '2024-09-07 22:00:00', 'FTE ca chiều'),
            # 8/7
            Labor('OW', 90, '2024-09-07 22:00:00', '2024-09-08 06:00:00', 'OW ca đêm'),
            Labor('OW', 126, '2024-09-08 06:00:00', '2024-09-08 14:00:00', 'OW ca đêm'),
            Labor('OW', 126, '2024-09-08 14:00:00', '2024-09-08 22:00:00', 'OW ca đêm'),
            Labor('FTE', 53, '2024-09-08 05:00:00', '2024-09-08 17:00:00', 'FTE ca sáng'),
            Labor('FTE', 13, '2024-09-08 10:00:00', '2024-09-08 22:00:00', 'FTE ca chiều'),
            # 9/7
            Labor('OW', 10, '2024-09-09 08:00:00', '2024-09-09 17:00:00', 'OW ca hành chính'),
            Labor('FTE', 30, '2024-09-09 06:00:00', '2024-09-09 14:00:00', 'FTE ca sáng'),
            Labor('FTE', 22, '2024-09-09 14:00:00', '2024-09-09 22:00:00', 'FTE ca chiều'))
    SGN = Warehouse('SGN', 90)

    sgn_20240707 = Simulation(warehouse=SGN,
                              forecast=Forecast(start_time='2024-09-06 22:00:00', end_time='2024-09-10 00:00:00'),
                              labor=labor)

    pd.set_option('display.max_columns',150)
    pd.set_option('display.max_rows',150)
    # a = datetime.now()
    # sgn_20240707.export(['Outbound Plan Raw'])
    print(sgn_20240707.exsd_ontime_simulation())
    sgn_20240707.export(['Fundamental Report'])
    print(sgn_20240707.sensitivity_analysis())
    # b = datetime.now()
    # print(b-a)
    # sgn_20240707.gen_df_forecast()
    # print(sgn_20240707.df_forecast)

if __name__ == "__main__":
    main()