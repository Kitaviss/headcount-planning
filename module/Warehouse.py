# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=line-too-long

import copy
import pandas as pd
import numpy as np
from module.Labor import Labor
from module.Forecast_Raw import df_day_type
from module.UPH_Raw import df_uph

class Warehouse:
    def __init__(self, warehouse, max_outbound_station) -> None:
        '''
        Khởi tạo 1 Warehouse để ghi nhận tập hợp toàn bộ các Workforce của FC đó.
        Các input:
        - warehouse: Ghi nhận tên gọi của Warehouse
        - max_outbound_station: Ghi nhận lượng Outbound Station tối đa mà Warehouse đó có
        ---
        Các Method của Class:
        - uph: Method để tính ra UPH của Staff
        - total_working_shift: Method để tính ra lượng Picker:Packer dựa trên input UPH và toàn bộ các workforce có trong mỗi khung giờ làm việc
        '''
        self.warehouse = warehouse
        self.max_outbound_station = max_outbound_station

    def uph(self,
            uph_fte_pick: dict = None,
            uph_fte_pack: dict = None,
            uph_ow_pick: dict = None,
            uph_ow_pack: dict = None,
            normalize_day: bool = True):
        '''
        Lấy các giá trị UPH của Warehouse đó trong quá khứ làm giá trị mặc định. \n
        Nếu có input các giá trị UPH mong muốn thì sẽ sử dụng thay thế giá trị mặc định. Các giá trị UPH mong muốn bao gồm:
        - uph_fte_pick: UPH Pick của FTE
        - uph_fte_pack: UPH Pack của FTE
        - uph_ow_pick: UPH Pick của OW
        - uph_ow_pack: UPH Pack của OW \n
        Trong đó:
        - Các giá trị UPH được input theo format dictionary ({'day_type':uph,})
        - List các day_type khả dụng (cập nhật 2024-07-17):
            - DoubleDay
            - Clearance
            - MidMonth
            - Normal \n
        Giá trị 'normalize_day' đại diện cho việc có/không việc apply giá trị UPH của day_type = 'Normal' cho các ngày còn lại trừ 'DoubleDay' (bao gồm 'Clearance', 'Midmonth' - cập nhật 2024-07-17):
        - True (mặc định): Toàn bộ các day_type khác 'DoubleDay' sẽ được ghi nhận theo giá trị UPH 'Normal' input vào. Lưu ý rằng việc này không làm mất đi các loại day_type này mà chỉ thay đổi giá trị UPH của nó. Ngoài ra, với 'normalize_day' = True thì sẽ chỉ ghi nhận các giá trị day_type đang có trong default database (Nghĩa là, việc input thêm các giá trị 'day_type' khác 'DoubleDay' và 'Normal' là không có tác dụng trong trường hợp này).
        - False: Không apply giá trị UPH 'Normal' cho các day_type khác (ngoại trừ 'DoubleDay').
        '''
        df_uph_warehouse = df_uph.loc[df_uph['warehouse'] == self.warehouse]

        if uph_fte_pick is not None:
            for i, k in enumerate(uph_fte_pick):
                if ((normalize_day is True) & (k != 'DoubleDay')):
                    lists = df_uph_warehouse.loc[(df_uph_warehouse['day_type'] != 'DoubleDay'), 'day_type'].unique().tolist()
                    k = 'Normal'
                else:
                    lists = [k]

                for l in lists:
                    if df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_fte_pick'].size == 0:
                        df_uph_temp = pd.DataFrame([{'day_type':l,
                                                     'warehouse':self.warehouse,
                                                     'uph_fte_pick':uph_fte_pick[k]}])
                        df_uph_warehouse = pd.concat([df_uph_warehouse, df_uph_temp], ignore_index=True)
                    else:
                        df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_fte_pick'] = uph_fte_pick[k]

        if uph_fte_pack is not None:
            for i, k in enumerate(uph_fte_pack):
                if ((normalize_day is True) & (k != 'DoubleDay')):
                    lists = df_uph_warehouse.loc[(df_uph_warehouse['day_type'] != 'DoubleDay'), 'day_type'].unique().tolist()
                    k = 'Normal'
                else:
                    lists = [k]

                for l in lists:
                    if df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_fte_pack'].size == 0:
                        df_uph_temp = pd.DataFrame([{'day_type':l,
                                                     'warehouse':self.warehouse,
                                                     'uph_fte_pack':uph_fte_pack[k]}])
                        df_uph_warehouse = pd.concat([df_uph_warehouse, df_uph_temp], ignore_index=True)
                    else:
                        df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_fte_pack'] = uph_fte_pack[k]

        if uph_ow_pick is not None:
            for i, k in enumerate(uph_ow_pick):
                if ((normalize_day is True) & (k != 'DoubleDay')):
                    lists = df_uph_warehouse.loc[(df_uph_warehouse['day_type'] != 'DoubleDay'), 'day_type'].unique().tolist()
                    k = 'Normal'
                else:
                    lists = [k]

                for l in lists:
                    if df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_ow_pick'].size == 0:
                        df_uph_temp = pd.DataFrame([{'day_type':l,
                                                     'warehouse':self.warehouse,
                                                     'uph_ow_pick':uph_ow_pick[k]}])
                        df_uph_warehouse = pd.concat([df_uph_warehouse, df_uph_temp], ignore_index=True)
                    else:
                        df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_ow_pick'] = uph_ow_pick[k]

        if uph_ow_pack is not None:
            for i, k in enumerate(uph_ow_pack):
                if ((normalize_day is True) & (k != 'DoubleDay')):
                    lists = df_uph_warehouse.loc[(df_uph_warehouse['day_type'] != 'DoubleDay'), 'day_type'].unique().tolist()
                    k = 'Normal'
                else:
                    lists = [k]

                for l in lists:
                    if df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_ow_pack'].size == 0:
                        df_uph_temp = pd.DataFrame([{'day_type':l,
                                                     'warehouse':self.warehouse,
                                                     'uph_ow_pack':uph_ow_pack[k]}])
                        df_uph_warehouse = pd.concat([df_uph_warehouse, df_uph_temp], ignore_index=True)
                    else:
                        df_uph_warehouse.loc[(df_uph_warehouse['day_type'] == l), 'uph_ow_pack'] = uph_ow_pack[k]

        return df_uph_warehouse
       
    def total_working_shift(self, df_uph_warehouse, *args:Labor):
        '''
        Phân chia lượng Picker:Packer dựa trên input (default hoặc manual) UPH và toàn bộ các workforce có trong mỗi khung giờ làm việc
        '''
        df_total_working_shift = pd.DataFrame()
        for i, k in enumerate(args):
            df_total_working_shift = pd.concat([df_total_working_shift, k.working_shift()], ignore_index=True)
        df_total_working_shift['number_of_staff_actual'] = df_total_working_shift['number_of_staff'] * df_total_working_shift['efficiency']

        df_total_working_shift_actual = df_total_working_shift.groupby(['working_hour',
                                                                        'contract_type',]
                                                                        ).agg({'number_of_staff_actual':'sum'})
        df_total_working_shift_actual.reset_index(inplace=True)

        df_total_working_shift_actual = pd.pivot_table(df_total_working_shift_actual, values='number_of_staff_actual', index=['working_hour'], columns=['contract_type'], aggfunc={'number_of_staff_actual':'sum'}, fill_value=0)

        df_total_working_shift_actual.reset_index(inplace=True)
        df_total_working_shift_actual.rename_axis(None, axis=1, inplace=True)
        df_total_working_shift_actual = df_total_working_shift_actual.rename(columns={'FTE':'number_of_fte', 'OW':'number_of_ow',})
        
        # chỗ này phải try except vì có khả năng df_total_working_shift_actual['number_of_fte'] còn chưa được tạo ra từ các bước trên
        try:
            df_total_working_shift_actual['number_of_fte'] = df_total_working_shift_actual['number_of_fte']
        except:
            df_total_working_shift_actual['number_of_fte'] = 0
        try:
            df_total_working_shift_actual['number_of_ow'] = df_total_working_shift_actual['number_of_ow']
        except:
            df_total_working_shift_actual['number_of_ow'] = 0

        # mapping để lấy thông tin day_type cho các ca làm việc của Staff
        df_total_working_shift_final_step1 = pd.merge(df_total_working_shift_actual, df_day_type, how='left', left_on='working_hour', right_on='created_time').drop(columns=['created_time',])
        # mapping để lấy được UPH tương ứng cho day_type đó của Staff
        df_total_working_shift_final_step2 = pd.merge(df_total_working_shift_final_step1, df_uph_warehouse, how='left', on='day_type')
    
        df_total_working_shift_final = copy.deepcopy(df_total_working_shift_final_step2)

        df_total_working_shift_final['number_of_fte_picker'] = np.where(
            # case
            # when
            df_total_working_shift_final['number_of_fte'] <= 0,
                # then
                0,
                np.where(
            # when
            (df_total_working_shift_final['number_of_ow'] > 0) & ((df_total_working_shift_final['number_of_fte']*df_total_working_shift_final['uph_fte_pack']) <= df_total_working_shift_final['number_of_ow']*df_total_working_shift_final['uph_ow_pick']),
                # then
                0,
                np.where(
            # when
            (df_total_working_shift_final['number_of_ow'] > 0) & ((df_total_working_shift_final['number_of_fte']*df_total_working_shift_final['uph_fte_pack']) > (df_total_working_shift_final['number_of_ow']*df_total_working_shift_final['uph_ow_pick'])),
                # then
                ((df_total_working_shift_final['number_of_fte']*df_total_working_shift_final['uph_fte_pack'] - df_total_working_shift_final['number_of_ow']*df_total_working_shift_final['uph_ow_pick'])/(df_total_working_shift_final['uph_fte_pick'] + df_total_working_shift_final['uph_fte_pack'])),
            # else
                ((df_total_working_shift_final['number_of_fte']*df_total_working_shift_final['uph_fte_pack'])/(df_total_working_shift_final['uph_fte_pick'] + df_total_working_shift_final['uph_fte_pack'])))))
        
        df_total_working_shift_final['number_of_fte_packer'] = df_total_working_shift_final['number_of_fte'] - df_total_working_shift_final['number_of_fte_picker']

        df_total_working_shift_final['number_of_ow_picker'] = np.where(
            # case
            # when
            df_total_working_shift_final['number_of_ow'] <= 0,
                # then
                0,
                np.where(
            # when
            (df_total_working_shift_final['number_of_fte_packer']*df_total_working_shift_final['uph_fte_pack'] < df_total_working_shift_final['number_of_ow']*df_total_working_shift_final['uph_ow_pick']),
                # then
                ((df_total_working_shift_final['number_of_fte_packer']*df_total_working_shift_final['uph_fte_pack'] + df_total_working_shift_final['number_of_ow']*df_total_working_shift_final['uph_ow_pack'])/(df_total_working_shift_final['uph_ow_pick'] + df_total_working_shift_final['uph_ow_pack'])),
            # else
                df_total_working_shift_final['number_of_ow']))
        
        df_total_working_shift_final['number_of_ow_packer'] = df_total_working_shift_final['number_of_ow'] - df_total_working_shift_final['number_of_ow_picker']

        # -- TÍNH NĂNG CHECK SỐ OB STATION
        df_total_working_shift_final['number_of_fte_packer_mod'] = np.where(df_total_working_shift_final['number_of_fte_packer'] > self.max_outbound_station, self.max_outbound_station, df_total_working_shift_final['number_of_fte_packer'])

        df_total_working_shift_final['number_of_ow_packer_mod'] = np.where(df_total_working_shift_final['number_of_fte_packer_mod'] + df_total_working_shift_final['number_of_ow_packer'] > self.max_outbound_station, self.max_outbound_station - df_total_working_shift_final['number_of_fte_packer_mod'], df_total_working_shift_final['number_of_ow_packer'])

        df_total_working_shift_final['number_of_fte_picker_mod'] = np.where(((df_total_working_shift_final['number_of_fte_packer_mod']*df_total_working_shift_final['uph_fte_pack'] + df_total_working_shift_final['number_of_ow_packer_mod']*df_total_working_shift_final['uph_ow_pack'])/df_total_working_shift_final['uph_fte_pick']) > (df_total_working_shift_final['number_of_fte'] - df_total_working_shift_final['number_of_fte_packer_mod']), df_total_working_shift_final['number_of_fte'] - df_total_working_shift_final['number_of_fte_packer_mod'], ((df_total_working_shift_final['number_of_fte_packer_mod']*df_total_working_shift_final['uph_fte_pack'] + df_total_working_shift_final['number_of_ow_packer_mod']*df_total_working_shift_final['uph_ow_pack'])/df_total_working_shift_final['uph_fte_pick']))

        df_total_working_shift_final['number_of_ow_picker_mod'] = np.where(((df_total_working_shift_final['number_of_fte_packer_mod']*df_total_working_shift_final['uph_fte_pack'] + df_total_working_shift_final['number_of_ow_packer_mod']*df_total_working_shift_final['uph_ow_pack'] - df_total_working_shift_final['number_of_fte_picker_mod']*df_total_working_shift_final['uph_fte_pick'])/df_total_working_shift_final['uph_ow_pick']) < (df_total_working_shift_final['number_of_ow'] - df_total_working_shift_final['number_of_ow_packer_mod']), ((df_total_working_shift_final['number_of_fte_packer_mod']*df_total_working_shift_final['uph_fte_pack'] + df_total_working_shift_final['number_of_ow_packer_mod']*df_total_working_shift_final['uph_ow_pack'] - df_total_working_shift_final['number_of_fte_picker_mod']*df_total_working_shift_final['uph_fte_pick'])/df_total_working_shift_final['uph_ow_pick']), (df_total_working_shift_final['number_of_ow'] - df_total_working_shift_final['number_of_ow_packer_mod']))

        df_total_working_shift_final['excess_staff'] = round((df_total_working_shift_final['number_of_fte_picker'] + df_total_working_shift_final['number_of_fte_packer'] + df_total_working_shift_final['number_of_ow_picker'] + df_total_working_shift_final['number_of_ow_packer']) - (df_total_working_shift_final['number_of_fte_picker_mod'] + df_total_working_shift_final['number_of_fte_packer_mod'] + df_total_working_shift_final['number_of_ow_picker_mod'] + df_total_working_shift_final['number_of_ow_packer_mod']))

        df_total_working_shift_final['number_of_fte_picker'] = abs(round(df_total_working_shift_final['number_of_fte_picker_mod']))
        df_total_working_shift_final['number_of_fte_packer'] = abs(round(df_total_working_shift_final['number_of_fte_packer_mod']))
        df_total_working_shift_final['number_of_ow_picker'] = abs(round(df_total_working_shift_final['number_of_ow_picker_mod']))
        df_total_working_shift_final['number_of_ow_packer'] = abs(round(df_total_working_shift_final['number_of_ow_packer_mod']))

        df_total_working_shift_final.drop(columns=['number_of_fte_picker_mod',
                                                   'number_of_fte_packer_mod',
                                                   'number_of_ow_picker_mod',
                                                   'number_of_ow_packer_mod'], inplace=True)
        # ----------------
        df_total_working_shift_final['capacity_fte_pick'] = df_total_working_shift_final['number_of_fte_picker'] * df_total_working_shift_final['uph_fte_pick']
        df_total_working_shift_final['capacity_ow_pick'] = df_total_working_shift_final['number_of_ow_picker'] * df_total_working_shift_final['uph_ow_pick']
        df_total_working_shift_final['capacity_fte_pack'] = df_total_working_shift_final['number_of_fte_packer'] * df_total_working_shift_final['uph_fte_pack']
        df_total_working_shift_final['capacity_ow_pack'] = df_total_working_shift_final['number_of_ow_packer'] * df_total_working_shift_final['uph_ow_pack']

        df_total_working_shift_final['capacity_total_pick'] = df_total_working_shift_final['capacity_fte_pick'] + df_total_working_shift_final['capacity_ow_pick']

        df_total_working_shift_final['capacity_total_pack'] = df_total_working_shift_final['capacity_fte_pack'] + df_total_working_shift_final['capacity_ow_pack']

        df_total_working_shift_final['capacity'] = df_total_working_shift_final[['capacity_total_pick', 'capacity_total_pack']].min(axis=1)

        if max(df_total_working_shift_final['excess_staff']) > 0:
            print('NOTICE: Lượng nhân viên đang dư thừa so với số Outbound Station sẵn có')
            
        return df_total_working_shift_final

def main():
    SGN = Warehouse('SGN', 90)
    l4 = Labor('FTE', 30, '2024-07-06 06:00:00', '2024-07-06 14:00:00', 'FTE ca sáng')
    l5 = Labor('FTE', 22, '2024-07-06 14:00:00', '2024-07-06 22:00:00', 'FTE ca chiều')
    l6 = Labor('OW', 60, '2024-07-06 06:00:00', '2024-07-06 14:00:00', 'OW ca sáng')
    l7 = Labor('OW', 60, '2024-07-06 14:00:00', '2024-07-06 22:00:00', 'OW ca chiều')
    pick_fte = {'DoubleDay':9999,'Normal':7777,}
    pick_ow = {'DoubleDay':8888,'Normal':6666,}
    pack_ow = {'DoubleDay':0,'Normal':1,'MidMonth':1000}
    df_sgn_capacity = SGN.total_working_shift(SGN.uph(uph_fte_pick=pick_fte, uph_ow_pick=pick_ow, uph_ow_pack=pack_ow, normalize_day=False), l4, l5, l6, l7,)
    # print(df_sgn_capacity)
    print(SGN.warehouse)
      
if __name__ == "__main__":
    main()