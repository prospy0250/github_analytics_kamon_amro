-- tests/assert_ranking_consistency.sql

with ranking_check as (

    select
        repo_id,
        score_global,
        repo_rank
    from {{ ref('scoring_repositories') }}

),

max_score as (

    select max(score_global) as max_score
    from {{ ref('scoring_repositories') }}

),

rank_one_violation as (

    select r.*
    from ranking_check r
    cross join max_score m
    where r.repo_rank = 1
      and r.score_global < m.max_score

),

duplicate_ranks as (

    select repo_rank
    from ranking_check
    group by repo_rank
    having count(*) > 1

)

select * from rank_one_violation
union all
select null as repo_id, null as score_global, repo_rank
from duplicate_ranks