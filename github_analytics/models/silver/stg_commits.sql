-- models/silver/stg_commits.sql
{{
config(
    materialized='incremental',
    schema='silver',
    unique_key='sha',
    incremental_strategy='append'
)
}}

with source as (
    select *
    from {{ source('bronze', 'raw_commits') }}
),

cleaned as (
    select
        repo_full_name as repo_id,
        sha as commit_sha,
        coalesce(author_login, 'Unknown') as author_login,
        cast(author_date as timestamp) as author_date,
        committer_login,
        cast(committer_date as timestamp) as committer_date,
        extract(dow from cast(author_date as timestamp)) as day_of_week,
        extract(hour from cast(author_date as timestamp)) as hour_of_day,
        substr(message, 1, 200) as message

    from source
    where sha is not null
)

select *
from cleaned

{% if is_incremental() %}

where author_date >
    (select max(author_date) from {{ this }})

{% endif %}