{{ config(materialized='table') }}

with commits as (

    select
        author_login as login,
        author_date as activity_date,
        repo_id,
        'commit' as activity_type
    from {{ ref('stg_commits') }}

),

pull_requests as (

    select
        user_login as login,
        created_at as activity_date,
        repo_full_name as repo_id,
        'pr' as activity_type
    from {{ ref('stg_pull_requests') }}

),

all_activities as (

    select * from commits
    union all
    select * from pull_requests

),

filtered as (

    select *
    from all_activities
    where lower(login) != 'unknown'
      and login is not null

),

aggregated as (

    select
        login as contributor_id,

        min(activity_date) as first_contribution_at,

        count(distinct repo_id) as repos_contributed_to,

        count(*) as total_activities

    from filtered
    group by login

)

select *
from aggregated