# pylint: disable=C0103
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=line-too-long

import pandas as pd
# import pandas_gbq

# project_id = "tnsl-dwh"

# sql_uph = """
# with
# data as
#     (
#     select distinct
#     day_type,
#     case
#     when contract_type = 'Fulltime' then 'FTE'
#     else 'OW'
#     end as contract_type,
#     warehouse,
#     sum(pick_units) over (partition by day_type, contract_type, warehouse)/sum(action_pick_hours) over (partition by day_type, contract_type, warehouse) avg_pick_uph,
#     sum(pack_units) over (partition by day_type, contract_type, warehouse)/sum(action_pack_hours) over (partition by day_type, contract_type, warehouse) avg_pack_uph,

#     percentile_disc(case when not pick_uph_action_outlier then pick_uph_action end, 0.5) over (partition by day_type, contract_type, warehouse) tp50_pick_uph,
#     percentile_disc(case when not pick_uph_action_outlier then pick_uph_action end, 0.8) over (partition by day_type, contract_type, warehouse) tp80_pick_uph,

#     percentile_disc(case when not pack_uph_action_outlier then pack_uph_action end, 0.5) over (partition by day_type, contract_type, warehouse) tp50_pack_uph,
#     percentile_disc(case when not pack_uph_action_outlier then pack_uph_action end, 0.8) over (partition by day_type, contract_type, warehouse) tp80_pack_uph,

#     from `tnsl-dwh.snop.wfm_ffm_uph_pick_pack_summary_by_labor`
#     where 1 = 1
#     and day >= date_sub(current_date('+7'), interval 60 day)
#     )

# select
# day_type,
# contract_type,
# warehouse,
# process,
# max(round(case
# when process = 'Pick' then avg_pick_uph
# else avg_pack_uph
# end)) as avg_uph,
# max(round(case
# when process = 'Pick' then tp50_pick_uph
# else tp50_pack_uph
# end)) as tp50_uph,
# max(round(case
# when process = 'Pick' then tp80_pick_uph
# else tp80_pack_uph
# end)) as tp80_uph
# from data
# cross join unnest(['Pick','Pack']) process
# group by 1,2,3,4
# """

# df_uph_raw = pandas_gbq.read_gbq(sql_uph, project_id=project_id)

df_uph_raw = pd.read_csv("data/uph.csv")
df_uph_raw.sort_values(by=['day_type','warehouse','contract_type'], ignore_index=True)

df_uph_pick = pd.pivot_table(df_uph_raw.loc[df_uph_raw.process == 'Pick'],
                             values='avg_uph',
                             index=['day_type','warehouse'],
                             columns=['contract_type',],
                             aggfunc={'avg_uph':'max'},
                             fill_value=70)

df_uph_pick.reset_index(inplace=True)
df_uph_pick.rename_axis(None, axis=1, inplace=True)
df_uph_pick.rename(columns={'FTE':'uph_fte_pick', 'OW':'uph_ow_pick', }, inplace=True)

df_uph_pack = pd.pivot_table(df_uph_raw.loc[df_uph_raw.process == 'Pack'],
                             values='avg_uph',
                             index=['day_type','warehouse'],
                             columns=['contract_type',],
                             aggfunc={'avg_uph':'max'},
                             fill_value=35)

df_uph_pack.reset_index(inplace=True)
df_uph_pack.rename_axis(None, axis=1, inplace=True)
df_uph_pack.rename(columns={'FTE':'uph_fte_pack', 'OW':'uph_ow_pack', }, inplace=True)

df_uph = pd.merge(df_uph_pick, df_uph_pack, how='outer', on=['day_type','warehouse'])

def main():
    print(df_uph)

if __name__ == "__main__":
    main()