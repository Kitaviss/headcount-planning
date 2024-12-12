# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=line-too-long

import math
from datetime import datetime, timedelta
import pandas as pd

class Labor:
    def __init__(self,
                 contract_type: str,
                 number_of_staff: int,
                 start_time: str,
                 end_time: str,
                 cost: float = None,
                 note: str = None):
        '''
        Các ca Workforce của 1 kho. Bao gồm các thông tin Input (bắt buộc, theo thứ tự):
        - contract_type: Thông tin hợp đồng lao động của các bạn nhân viên trong ca làm việc đó, bao gồm 2 loại:
            - FTE: Nhân viên Fulltime
            - OW: Nhân viên Outsource
        - number_of_staff: Số lượng các bạn nhân viên có trong ca làm việc đó.
        - start_time: Thời gian bắt đầu ca làm theo format "YYYY-MM-DD HH:MM:SS". "MM:SS" hiện tại chỉ hỗ trợ là "00:00".
        - end_time: Thời gian kết thúc ca làm theo format "YYYY-MM-DD HH:MM:SS". "MM:SS" hiện tại chỉ hỗ trợ là "00:00". Lưu ý: end_time = "2024-01-01 18:00:00" nghĩa là các bạn sẽ đi về lúc "18:00:00", chứ ko phải là các bạn sẽ làm luôn từ "18:00:00" đến "18:59:59" và đi về lúc "19:00:00".
        - note: Note thêm. Có thể null.
        ---
        Các Method của Class:
        - break_time: Method để tự nội suy ra thời gian nghỉ ngơi của các bạn nhân viên.
        - working_shift: Method để tính ra lịch làm việc của từng nhóm Workforce.
        '''
        self.contract_type = contract_type
        self.number_of_staff = number_of_staff
        self.start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        self.end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        self.cost = cost
        self.note = note

    def __str__(self) -> str:
        newline = '\n'
        return f'{self.note}{newline}Contract Type: {self.contract_type}{newline}Number of Headcount: {self.number_of_staff}{newline}Start Working At: {self.start_time}{newline}End Working At: {self.end_time}'

    def break_time(self):
        '''
        Method để tự nội suy ra thời gian nghỉ ngơi của các bạn nhân viên, dựa trên thời gian ca làm việc của các bạn.
        '''
        working_period = self.end_time - self.start_time
        break_time_list = []
        # dưới 6 tiếng thì ko có ca nghỉ
        if working_period <= timedelta(seconds=6*60*60):
            pass
        # dưới 10 tiếng thì 1 ca nghỉ
        elif working_period <= timedelta(seconds=10*60*60):
            if working_period.seconds/(60*60) % 2 == 0:
                break_time_list.append(working_period/2 + self.start_time)
            else:
                break_time_list.append(timedelta(hours=math.floor((working_period.seconds/(60*60))/2)) + self.start_time)
        # trên 10 tiếng thì 2 ca nghỉ
        # sẽ ko có trường hợp có trên 2 ca nghỉ, do luật quy định chỉ đc làm tối đa 12 tiếng/ngày
        else:
            if working_period.seconds/(60*60) % 3 == 0:
                break_time_list.append(working_period/3 + self.start_time)
                break_time_list.append(self.end_time - working_period/3)
            else:
                break_time_list.append(timedelta(hours=math.floor((working_period.seconds/(60*60))/3)) + self.start_time)
                break_time_list.append(self.end_time - timedelta(hours=math.ceil((working_period.seconds/(60*60))/3)))
        return break_time_list
                
    def working_shift(self,
                      efficiency_lost_due_to_first_instruction: bool = True,
                      coefficency_lost: float = 0.5,
                      apply_to_all: bool = False):
        '''
        Lịch làm việc của từng nhóm Workforce, bao gồm các input:
        - efficiency_lost_due_to_first_instruction: Đại diện cho việc hao phí công suất ở khung giờ đầu tiên cho việc hướng dẫn các bạn nhân viên mới làm lần đầu hoặc các hao phí đầu giờ khác. Mặc định là True.
        - coefficency_lost: Là hệ số hao phí của 'efficiency_lost_due_to_first_instruction'. Mặc định bằng 50%.
        - apply_to_all: Đại diện cho việc áp dụng 'efficiency_lost_due_to_first_instruction' cho cả OW và FTE hay không. Mặc định là False, chỉ áp dụng cho OW.
        '''
        df_working_shift = pd.DataFrame(pd.date_range(start=self.start_time, end=(self.end_time - timedelta(hours=1)), freq='h'), columns=['working_hour'])
        df_working_shift['working_hour'] = pd.to_datetime(df_working_shift['working_hour'])
        df_working_shift['number_of_staff'] = self.number_of_staff
        df_working_shift['contract_type'] = self.contract_type

        efficiency = []
        for i in df_working_shift['working_hour']:
            if (efficiency_lost_due_to_first_instruction is True) & (i == df_working_shift['working_hour'][0]):
                if apply_to_all is True:
                    efficiency.append(coefficency_lost)
                elif self.contract_type == 'OW':
                    efficiency.append(coefficency_lost)
                else:
                    efficiency.append(1)
            elif i in self.break_time():
                # breaktime 30p, dẫn tới capacity sẽ giảm 50%
                efficiency.append(0.5)
            else:
                efficiency.append(1)
        df_working_shift['efficiency'] = pd.DataFrame(efficiency)
       
        return df_working_shift

def main():
    sgn_ca_sang = Labor(contract_type='FTE',
                        number_of_staff=36,
                        start_time='2024-07-06 06:00:00',
                        end_time='2024-07-06 18:00:00',
                        note='FTE ca sáng')
    print(sgn_ca_sang.working_shift())

if __name__ == "__main__":
    main()