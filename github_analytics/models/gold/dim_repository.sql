{{ config ( materialized ='table') }}

with repo as (
select * from {{ ref('stg_repositories') }}
)
select
    repo_id ,
    name as repo_name,
    owner_login ,
    description ,
    language ,
    license_name ,
    created_at ,
    stargazers_count as stars_count,
    forks_count,
    watchers_count,
    repo_age_days,
    default_branch,
    has_wiki,
    has_pages
from repo