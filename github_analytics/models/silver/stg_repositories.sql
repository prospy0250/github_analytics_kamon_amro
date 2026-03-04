-- models/silver/stg_repositories.sql
{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('bronze', 'raw_repositories') }}
),

cleaned as (
    select
        -- Rename business key
        full_name as repo_id,

        -- Keep the rest of the columns (typed/cleaned where needed)
        name,
        owner_login,
        coalesce(description, 'No description') as description,
        coalesce(language, 'Unknown') as language,

        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(pushed_at  as timestamp) as pushed_at,

        cast(stargazers_count  as integer) as stargazers_count,
        cast(watchers_count    as integer) as watchers_count,
        cast(forks_count       as integer) as forks_count,
        cast(open_issues_count as integer) as open_issues_count,
        cast(size              as integer) as size,
        cast(network_count     as integer) as network_count,
        cast(subscribers_count as integer) as subscribers_count,

        default_branch,
        has_wiki,
        has_pages,
        archived,
        disabled,
        license_name,
        topics,
        snapshot_date,

        -- Derived column
        datediff(
            'day',
            cast(created_at as timestamp),
            current_timestamp
        ) as repo_age_days

    from source
    where coalesce(cast(archived as boolean), false) = false
)

select *
from cleaned