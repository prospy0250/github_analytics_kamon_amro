-- models/silver/stg_pull_requests.sql
{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as (
    select
        repo_full_name,
        pr_number,
        title,
        state,
        user_login,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(closed_at as timestamp) as closed_at,
        cast(merged_at as timestamp) as merged_at,
        draft,
        comments,
        review_comments,
        labels,
        merged_at is not null as is_merged,
        cast(draft as boolean) as is_draft,
        case
          when merged_at is not null then 
            datediff('hour', cast(merged_at as timestamp), cast(created_at as timestamp))
          when closed_at is not null then 
            datediff('hour', cast(closed_at as timestamp), cast(created_at as timestamp))
          else
            null
        end as time_to_close_hours

    from source
    where pr_number is not null
)

select *
from cleaned