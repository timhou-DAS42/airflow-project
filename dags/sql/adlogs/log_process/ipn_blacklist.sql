use warehouse DAS42DEV;

use airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }};

update airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{ params.table }}
set iab_flag = 'e'
from
    airflow_db_{{ params.env }}.dimensions.ipn_exclude
where
    ipn_exclude.ipn_range_end = 0
    and {{ params.table }}.ipn is not null
    and {{ params.table }}.ipn = ipn_exclude.ipn;
