use warehouse DAS42DEV;

use airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }};

update airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{ params.table }}
set {{ params.table }}.placementid = db.placementid
from airflow_db_{{ params.env }}.dimensions.placement_smartclip db
where
    {{ params.table }}.unhex_md5_smartclip = db.unhex_md5_smartclip
    and {{ params.table }}.placementid < 13000001
    and {{ params.table }}.smartclip = 1;
