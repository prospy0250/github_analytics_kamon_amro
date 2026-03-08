-- tests/assert_chronological_coherence.sql (closed_at < created_at)

select
    repo_full_name as repo_id,
    pr_number as record_id,
    created_at,
    closed_at
from {{ ref('stg_pull_requests') }}
where closed_at is not null
  and closed_at < created_at

union all

select
    repo_id,
    issue_number as record_id,
    created_at,
    closed_at
from {{ ref('stg_issues') }}
where closed_at is not null
  and closed_at < created_at