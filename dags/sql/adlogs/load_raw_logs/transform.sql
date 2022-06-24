use warehouse DAS42DEV;

create schema if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }};

use airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }};

create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{params.table_name}}
    like airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.{{params.table_name}}
;

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{params.table_name}}
    with cte as (
        select * 
        from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.{{params.table_name}} t
        where date = {{ds_nodash}}
    )
    select *
    from 
        cte
    where not exists (
        select * 
        from 
            airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{params.table_name}}
        where date = {{ds_nodash}}
    )
;