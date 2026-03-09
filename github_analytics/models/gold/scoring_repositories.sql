{{ config(materialized='table') }}

with recent_activity as (

    select
        repo_id,

        -- activity last 30 days
        sum(commits_count) as commits_30d,
        sum(prs_merged) as merged_prs_30d,
        sum(unique_committers) as contributors_30d,
        avg(avg_pr_close_hours) as avg_pr_close_hours_30d,

        -- full history metrics
        sum(prs_opened) as total_prs,
        sum(prs_merged) as merged_prs,

        sum(issues_opened) as total_issues,
        sum(issues_closed) as closed_issues

    from {{ ref('fact_repo_activity') }}

    where activity_date >= current_date - interval 30 day

    group by repo_id

),

base_metrics as (

    select
        r.repo_id,
        r.repo_name,
        r.owner_login,
        r.language,

        r.stars_count,
        r.forks_count,

        a.commits_30d,
        a.merged_prs_30d,
        a.contributors_30d,
        a.avg_pr_close_hours_30d,

        a.total_prs,
        a.merged_prs,

        a.total_issues,
        a.closed_issues,

        case
            when a.total_prs > 0
            then a.merged_prs * 1.0 / a.total_prs
        end as pr_merge_ratio,

        case
            when a.total_issues > 0
            then a.closed_issues * 1.0 / a.total_issues
        end as issue_close_ratio

    from {{ ref('dim_repository') }} r
    left join recent_activity a
        on r.repo_id = a.repo_id

),

ranked as (

    select

        *,

        ntile(10) over (order by stars_count desc) as stars_rank,
        ntile(10) over (order by forks_count desc) as forks_rank,

        ntile(10) over (order by commits_30d desc) as commits_rank,
        ntile(10) over (order by contributors_30d desc) as contributors_rank,

        ntile(10) over (order by pr_merge_ratio desc) as pr_merge_rank,

        ntile(10) over (order by issue_close_ratio desc) as issue_close_rank,

        -- reaction time (lower = better)
        ntile(10) over (order by avg_pr_close_hours_30d asc) as pr_speed_rank

    from base_metrics

),

scored as (

    select

        *,

        -- popularity score
        (stars_rank + forks_rank) * 100.0 / 20 as score_popularity,

        -- activity score
        (commits_rank + contributors_rank) * 100.0 / 20 as score_activity,

        -- collaboration score
        (pr_merge_rank + issue_close_rank) * 100.0 / 20 as score_community,

        -- responsiveness score
        pr_speed_rank * 100.0 / 10 as score_responsiveness

    from ranked

)

select

    repo_id,
    repo_name,
    owner_login,
    language,

    score_popularity,
    score_activity,
    score_community,
    score_responsiveness,

    -- global score
    (
        score_popularity * 0.2 +
        score_activity * 0.3 +
        score_community * 0.2 +
        score_responsiveness * 0.3
    ) as score_global,

    row_number() over (
        order by
        (
            score_popularity * 0.2 +
            score_activity * 0.3 +
            score_community * 0.2 +
            score_responsiveness * 0.3
        ) desc
    ) as repo_rank

from scored
order by repo_rank