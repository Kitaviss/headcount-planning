# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=line-too-long

from datetime import datetime, timedelta
import copy
import pandas as pd
import numpy as np
from module.Forecast_Raw import df_forecast_origin

class Forecast:
    def __init__(self,
                 warehouse: list = None,
                 client: list = None,
                #  sub_client: list = None,
                 channel: list = None,
                 start_time: str = None,
                 end_time: str = None
                 ):
        '''
        Class để load Forecast, mỗi object đại diện cho một checkpoint/scenario của Forecast. \n
        Giá trị Forecast nằm ở Attribute 'df_forecast' --> Cách sử dụng: Forecast().df_forecast \n
        Các input đầu vào:
        - warehouse: List các warehouse cần filter để view Forecast của nó. Mặc định là lấy toàn bộ warehouse có trong Forecast gốc.
        - client: List các client cần filter để view Forecast của nó. Mặc định là lấy toàn bộ client có trong Forecast gốc.
        - channel: List các channel cần filter để view Forecast của nó. Mặc định là lấy toàn bộ channel có trong Forecast gốc.
        - start_time: Mốc created_time đầu tiên của ĐH cần filter để view Forecast của nó. Mặc định là lấy mốc đầu tiên của Forecast gốc.
        - end_time: Mốc created_time cuối cùng của ĐH cần filter để view Forecast của nó. Mặc định là lấy mốc cuối cùng của Forecast gốc. Lưu ý rằng "end_time" đại diện cho "mốc thời gian phát sinh cuối cùng" của ĐH. Ví dụ: 'start_time' = '2024-07-07 00:00:00' & 'end_time' = '2024-07-07 02:00:00' là toàn bộ ĐH phát sinh từ '2024-07-07 00:00:00' đến hết '2024-07-07 01:59:59'.
        ---
        Các Method của Class:
        - adjust: Method để thay đổi giá trị của Forecast
        - reset: Method để khôi phục giá trị forecast lại như ban đầu, trước khi thực hiện các adjust
        '''
        self.df_forecast = copy.deepcopy(df_forecast_origin)

        if warehouse is not None:
            self.df_forecast = self.df_forecast[self.df_forecast['warehouse'].isin(warehouse)]

        if client is not None:
            self.df_forecast = self.df_forecast[self.df_forecast['client'].isin(client)]

        # if sub_client is not None:
        #     self.df_forecast = self.df_forecast[self.df_forecast['sub_client'].isin(sub_client)]

        if channel is not None:
            self.df_forecast = self.df_forecast[self.df_forecast['channel'].isin(channel)]

        if start_time is not None:
            self.df_forecast = self.df_forecast[self.df_forecast['created_time'] >= datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')]

        if end_time is not None:
            self.df_forecast = self.df_forecast[self.df_forecast['created_time'] < datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')]

        self.df_forecast.reset_index(drop=True, inplace=True)

        # self.warehouse = self.df_forecast['warehouse'].unique().tolist()
        # self.client = self.df_forecast['client'].unique().tolist()
        # self.sub_client = self.df_forecast['sub_client'].unique().tolist()
        # self.channel = self.df_forecast['channel'].unique().tolist()

        self.start_time = self.df_forecast['created_time'].min()
        self.end_time = self.df_forecast['created_time'].max()
        self.warehouse = pd.NA
        self.client = pd.NA
        # self.sub_client = pd.NA
        self.channel = pd.NA
        self.day_type = pd.NA
        self.exsd = pd.NaT
        self.sla_platform = pd.NaT

        self.df_forecast_checkpoint = copy.deepcopy(self.df_forecast)

    def adjust(self,
               warehouse: list = None,
               client: list = None,
            #    sub_client: list = None,
               channel: list = None,
               start_time: str = None,
               end_time: str = None,
               day_type: str = None,
               exsd: str = None,
               sla_platform: str = None,
               ratio_order: float = 1.0,
               ratio_unit: float = 1.0,
               absolute_order: float = 0.0,
               absolute_unit: float = 0.0
               ):
        """
        Method để thay đổi giá trị của Forecast

        Parameters
        ----------
        warehouse : list, optional
            List các Warehouse cần được thay đổi giá trị forecast. Mặc định là tất cả các giá trị Client có trong Forecast truyền vào. By default None
        client : list, optional
            List các Client cần được thay đổi giá trị forecast. Mặc định là tất cả các giá trị Client có trong Forecast truyền vào. By default None
        sub_client : list, optional
            List các Sub Client cần được thay đổi giá trị forecast. Mặc định là tất cả các giá trị Sub Client có trong Forecast truyền vào. By default None
        channel : list, optional
            List các Channel cần được thay đổi giá trị forecast. Mặc định là tất cả các giá trị Channel có trong Forecast truyền vào. By default None \n
            Please note rằng chỉ có các giá trị parameter được input sẽ ảnh hưởng bởi method này. Ví dụ: Input là "warehouse = ['SGN']" & "client = ['TikiCorp']" thì chỉ có các record của "SGN + TikiCorp" sẽ được adjust bởi method này và quá trình này sẽ không gây ảnh hưởng tới các record khác, cũng như không tạo ra thêm các record mới của các tổ hợp giá trị.
        start_time : str, optional
            Thời điểm ĐH phát sinh đầu tiên cần được thay đổi giá trị forecast theo format "YYYY-MM-DD HH:MM:SS". "MM:SS" hiện tại chỉ hỗ trợ là "00:00". Mặc định là giá trị thời gian đầu tiên của created_time trong Forecast truyền vào. By default None
        end_time : str, optional
            Thời điểm ĐH phát sinh cuối cùng cần được thay đổi giá trị forecast theo format "YYYY-MM-DD HH:MM:SS". "MM:SS" hiện tại chỉ hỗ trợ là "00:00". Mặc định là giá trị thời gian cuối cùng của created_time trong Forecast truyền vào. Lưu ý: end_time = "2024-01-01 18:00:00" nghĩa là các đơn hàng phát sinh trước "18:00:00", chứ không phải là các đơn hàng phát sinh từ "18:00:00" đến "18:59:59". By default None
        day_type : str, optional
            Define lại kiểu "day_type" cho khoảng thời gian start_time - end_time truyền vào. By default None \n
            Bao gồm 4 loại (cập nhật lúc 2024-07-25):
            - Normal: Ngày thường
            - MidMonth: Campaign ngày 15 mỗi tháng
            - Clearance: Campaign ngày 25 mỗi tháng
            - DoubleDay: Campaign Double Day
        exsd : str, optional
            Define lại giá trị Expected Shipping Date (ExSD) của loại ĐH trong khoảng thời gian truyền vào. Có thể được sử dụng để only điều chỉnh ExSD mà ko thay đổi các giá trị Forecast volume. By default None
        sla_platform : str, optional
            Define lại giá trị SLA Platform của loại ĐH trong khoảng thời gian truyền vào. Có thể được sử dụng để only điều chỉnh SLA Platform mà ko thay đổi các giá trị Forecast volume. By default None
        ratio_order : float, optional
            Tỉ lệ giá trị Order forecast được thay đổi, trong đó New Order = Current Order x ratio_order. By default 1.0
        ratio_unit : float, optional
            Tỉ lệ giá trị Unit forecast được thay đổi, trong đó New Unit = Current Unit x ratio_unit. By default 1.0
        absolute_order : float, optional
            Số lượng giá trị Order forecast được thay đổi, trong đó New Order = Current Order + absolute_order. Có thể mang giá trị âm. By default 0.0
        absolute_unit : float, optional
            Số lượng giá trị Unit forecast được thay đổi, trong đó New Unit = Current Unit + absolute_unit. Có thể mang giá trị âm. By default 0.0
        Trình tự ghi nhận giá trị thay đổi của volume mới sẽ là "x ratio" trước và "+ absolute" sau.
        Từ ver 1.6 (2024-10-05) của Headcount Planning, sub_client sẽ không còn được support

        Returns
        -------
        NoneType
            Thay đổi giá trị của self.df_forecast
            
        Raises
        ------
        ValueError
            Khi giá trị end_time <= start_time

        Examples
        --------
        >>> forecast_20240707 = Forecast(client=['Onpoint', 'TikiCorp'], 
        ...                              warehouse=['SGN'],
        ...                              start_time='2024-07-07 00:00:00', 
        ...                              end_time='2024-07-07 02:00:00')
        >>> forecast_20240707.df_forecast
            created_time            created_time_date   created_time_hour   warehouse
        0   2024-07-07 00:00:00     2024-07-07          0                   SGN   
        1   2024-07-07 00:00:00     2024-07-07          0                   SGN   
        2   2024-07-07 01:00:00     2024-07-07          1                   SGN   
        3   2024-07-07 01:00:00     2024-07-07          1                   SGN   
            client      sub_client  channel     day_type    exsd 
        0   TikiCorp    TikiCorp    Others      DoubleDay   2024-07-07 11:00:00   
        1   Onpoint     Onpoint     Others      DoubleDay   2024-07-08 23:00:00   
        2   TikiCorp    TikiCorp    Others      DoubleDay   2024-07-07 11:00:00   
        3   Onpoint     Onpoint     Others      DoubleDay   2024-07-08 23:00:00
            sla_platform            orders_forecast     units_forecast  
        0   2024-07-07 11:00:00     1322.0              9574.0  
        1   2024-07-08 23:59:59     0.0                 0.0  
        2   2024-07-07 11:00:00     854.0               5879.0  
        3   2024-07-08 23:59:59     0.0                 0.0
        >>> forecast_20240707.adjust(client=['TikiCorp'], 
        ...                          channel=['Others'],
        ...                          start_time='2024-07-07 00:00:00',
        ...                          end_time='2024-07-07 01:00:00',
        ...                          ratio_order=10000,
        ...                          ratio_unit=10000,
        ...                          absolute_order=9999,
        ...                          absolute_unit=9999)
        >>> forecast_20240707.df_forecast                         
            created_time            created_time_date   created_time_hour   warehouse
        0   2024-07-07 00:00:00     2024-07-07          0                   SGN   
        1   2024-07-07 00:00:00     2024-07-07          0                   SGN   
        2   2024-07-07 01:00:00     2024-07-07          1                   SGN   
        3   2024-07-07 01:00:00     2024-07-07          1                   SGN   
            client      sub_client  channel     day_type    exsd 
        0   TikiCorp    TikiCorp    Others      DoubleDay   2024-07-07 11:00:00   
        1   Onpoint     Onpoint     Others      DoubleDay   2024-07-08 23:00:00   
        2   TikiCorp    TikiCorp    Others      DoubleDay   2024-07-07 11:00:00   
        3   Onpoint     Onpoint     Others      DoubleDay   2024-07-08 23:00:00
            sla_platform            orders_forecast     units_forecast  
        0   2024-07-07 11:00:00     13229999.0          95749999.0
        1   2024-07-08 23:59:59     0.0                 0.0  
        2   2024-07-07 11:00:00     854.0               5879.0  
        3   2024-07-08 23:59:59     0.0                 0.0
        """
        key_join = []

        # start_time & end_time là 2 giá trị đặc biệt, giá trị default ko phải là Null
        if start_time is not None:
            self.start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        if end_time is not None:
            self.end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        if self.start_time > self.end_time:
            raise ValueError('Start Time is after End Time')
        key_join.append('created_time')
        # df_forecast_adjust_created_time = pd.DataFrame(pd.date_range(start=self.start_time, end=self.end_time - timedelta(hours=1), freq='h'), columns=['created_time'])
        df_forecast_adjust_created_time = pd.DataFrame(pd.date_range(start=self.start_time, end=(self.end_time - timedelta(hours=1)), freq='h'), columns=['created_time'])
        df_forecast_adjust = df_forecast_adjust_created_time

        if warehouse is not None:
            self.warehouse = warehouse
            key_join.append('warehouse')
            df_forecast_adjust_warehouse = pd.DataFrame(data={'warehouse':self.warehouse})
            df_forecast_adjust = df_forecast_adjust_warehouse

        if client is not None:
            self.client = client
            key_join.append('client')
            df_forecast_adjust_client = pd.DataFrame(data={'client':self.client})
            df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_client, how='cross')

        # if sub_client is not None:
        #     self.sub_client = sub_client
        #     key_join.append('sub_client')
        #     df_forecast_adjust_sub_client = pd.DataFrame(data={'sub_client':self.sub_client})
        #     df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_sub_client, how='cross')

        if channel is not None:
            self.channel = channel
            key_join.append('channel')
            df_forecast_adjust_channel = pd.DataFrame(data={'channel':self.channel})
            df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_channel, how='cross')

        # 3 giá trị "day_type", "exsd", "sla_platform" cũng là các giá trị đặc biệt, sẽ luôn luôn được tạo cột dù method có input giá trị mới hay không
        if day_type is not None:
            self.day_type = day_type
        df_forecast_adjust_day_type = pd.DataFrame(data={'day_type':[self.day_type]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_day_type, how='cross')

        if exsd is not None:
            self.exsd = datetime.strptime(exsd, '%Y-%m-%d %H:%M:%S')
        df_forecast_adjust_exsd = pd.DataFrame(data={'exsd':[self.exsd]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_exsd, how='cross')
        
        if sla_platform is not None:
            self.sla_platform = datetime.strptime(sla_platform, '%Y-%m-%d %H:%M:%S')
        df_forecast_adjust_sla_platform = pd.DataFrame(data={'sla_platform':[self.sla_platform]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_sla_platform, how='cross')

        self.ratio_order = ratio_order
        df_forecast_adjust_ratio_order = pd.DataFrame(data={'ratio_order':[self.ratio_order]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_ratio_order, how='cross')

        self.ratio_unit = ratio_unit
        df_forecast_adjust_ratio_unit = pd.DataFrame(data={'ratio_unit':[self.ratio_unit]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_ratio_unit, how='cross')

        self.absolute_order = absolute_order
        df_forecast_adjust_absolute_order = pd.DataFrame(data={'absolute_order':[self.absolute_order]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_absolute_order, how='cross')

        self.absolute_unit = absolute_unit
        df_forecast_adjust_absolute_unit = pd.DataFrame(data={'absolute_unit':[self.absolute_unit]})
        df_forecast_adjust = pd.merge(df_forecast_adjust, df_forecast_adjust_absolute_unit, how='cross')

        self.df_forecast = pd.merge(self.df_forecast, df_forecast_adjust, how='outer', on=key_join, suffixes=['_origin','_adjust'])

        self.df_forecast['day_type'] = np.where(self.df_forecast['day_type_adjust'].notna(), self.df_forecast['day_type_adjust'], self.df_forecast['day_type_origin'])
        self.df_forecast['exsd'] = np.where(self.df_forecast['exsd_adjust'].notna(), self.df_forecast['exsd_adjust'], self.df_forecast['exsd_origin'])
        self.df_forecast['sla_platform'] = np.where(self.df_forecast['sla_platform_adjust'].notna(), self.df_forecast['sla_platform_adjust'], self.df_forecast['sla_platform_origin'])

        self.df_forecast['units_forecast'] = np.where(self.df_forecast['units_forecast'].isna(), 0, self.df_forecast['units_forecast']) * np.where(self.df_forecast['ratio_unit'].isna(), 1, self.df_forecast['ratio_unit']) + np.where(self.df_forecast['absolute_unit'].isna(), 0, self.df_forecast['absolute_unit'])

        self.df_forecast['orders_forecast'] = np.where(self.df_forecast['orders_forecast'].isna(), 0, self.df_forecast['orders_forecast']) * np.where(self.df_forecast['ratio_order'].isna(), 1, self.df_forecast['ratio_order']) + np.where(self.df_forecast['absolute_order'].isna(), 0, self.df_forecast['absolute_order'])

        self.df_forecast.drop(columns=['day_type_origin',
                                       'day_type_adjust',
                                       'exsd_origin',
                                       'exsd_adjust',
                                       'sla_platform_origin',
                                       'sla_platform_adjust',
                                       'ratio_order',
                                       'ratio_unit',
                                       'absolute_order',
                                       'absolute_unit'], inplace=True)
        
        self.reset_attribute()

    def reset_attribute(self):
        self.start_time = self.df_forecast['created_time'].min()
        self.end_time = self.df_forecast['created_time'].max()
        self.warehouse = pd.NA
        self.client = pd.NA
        # self.sub_client = pd.NA
        self.channel = pd.NA
        self.day_type = pd.NA
        self.exsd = pd.NaT
        self.sla_platform = pd.NaT

    def reset(self):
        """
        Method để khôi phục lại Forecast như ban đầu, trước khi điều chỉnh
        """
        self.df_forecast = copy.deepcopy(self.df_forecast_checkpoint)

        self.start_time = self.df_forecast['created_time'].min()
        self.end_time = self.df_forecast['created_time'].max()
        self.warehouse = pd.NA
        self.client = pd.NA
        # self.sub_client = pd.NA
        self.channel = pd.NA
        self.day_type = pd.NA
        self.exsd = pd.NaT
        self.sla_platform = pd.NaT

def main():
    forecast_20240707 = Forecast(client=['Onpoint','TikiCorp'], channel=['Others'], warehouse=['SGN'],start_time='2024-07-07 00:00:00', end_time='2024-07-07 02:00:00')
    forecast_20240707.adjust(client=['TikiCorp'],
                             start_time='2024-07-07 00:00:00',
                             end_time='2024-07-07 01:00:00',
                             ratio_order=10000,
                             ratio_unit=10000,
                             absolute_order=9999,
                             absolute_unit=9999)
    # pd.set_option('display.max_rows',99999)
    # pd.set_option('display.max_columns',99999)
    print(forecast_20240707.df_forecast)

if __name__ == "__main__":
    main()