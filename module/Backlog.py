# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=line-too-long

import pandas as pd
# from Forecast import Forecast

class Backlog:
    def __init__(self) -> None:
        pass

    def default(self):
        '''
        pass
        '''
        return pd.DataFrame()
        # return pd.DataFrame(columns=Forecast().df_forecast.columns)

    def actual(self):
        '''
        pass
        '''
        pass

def main():
    print(Backlog().default())

if __name__ == "__main__":
    main()