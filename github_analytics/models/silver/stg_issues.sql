-- models/silver/stg_issues.sql
{{
config(
    materialized='incremental',
    schema='silver',
    unique_key='issue_number',
    incremental_strategy='merge'
)
}}
with source as (
    select *
    from {{ source('bronze', 'raw_issues') }}
),

cleaned as (
    select
        repo_full_name as repo_id,
        cast(issue_number as integer) as issue_number,
        title,
        state,
        user_login,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(closed_at as timestamp) as closed_at,
        comments,
        labels,
        cast(is_pull_request as boolean) as is_pull_request,

        case
          when updated_at is not null then 
            datediff('hour', cast(created_at as timestamp), cast(updated_at as timestamp))
          when closed_at is not null then 
            datediff('hour', cast(created_at as timestamp), cast(closed_at as timestamp))
          else
            null
        end as time_to_close_hours

    from source
    where issue_number is not null 
)

select *
from cleaned where is_pull_request = false

{% if is_incremental() %}

and updated_at >
    (select max(updated_at) from {{ this }})

{% endif %}


