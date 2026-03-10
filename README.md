# GitHub Analytics – ELT Pipeline with dbt

## Authors
- Kamon SOURABIE
- Amro BENRAMDANE

## Overview

This project implements an **ELT data pipeline using dbt and DuckDB** to analyze activity from several GitHub repositories and produce a **repository ranking based on activity metrics**.

The pipeline follows the **Medallion Architecture**:

- **Bronze** → Raw ingested data  
- **Silver** → Cleaned and standardized staging models  
- **Gold** → Analytics-ready star schema and business metrics  

The final output is a **ranking of the 10 repositories**, with:
- a **global score between 0 and 100**
- a **rank from 1 to 10**
  
---

# Architecture

The project implements a **three-layer medallion architecture**.

## Bronze Layer

The **Bronze layer** contains the **raw data ingested from the GitHub API**.

The ingestion script loads the following datasets into DuckDB:

- repositories
- commits
- issues
- pull requests

These tables are declared as **sources in dbt**.

Bronze objectives:

- preserve raw data
- avoid transformations
- provide traceability

Source definitions are located in:
models/bronze/_sources.yml


---

# Silver Layer

The **Silver layer** cleans and structures the raw data.

Staging models perform:

- data type normalization
- column selection
- derived attributes
- basic data cleaning

Models:

models/silver/
- stg_repositories.sql
- stg_commits.sql
- stg_pull_requests.sql
- stg_issues.sql

Examples of transformations:

- extracting **day_of_week** and **hour_of_day** from commit timestamps
- computing repository age
- standardizing issue and PR states
- selecting relevant analytical columns

Tests for the staging layer are defined in:
models/silver/_schema.yml

---

# Gold Layer

The **Gold layer** provides an **analytics-ready star schema**.

## Dimensions
- dim_repository
- dim_date
- dim_contributor

## Fact Table
fact_repo_activity

This table aggregates repository activity metrics:

- number of commits
- number of issues
- number of pull requests
- contributor activity
- temporal information

## Business Model

The final business model computes the **repository ranking**:
scoring_repositories.sql

Each repository receives:

- **score_global** (0–100)
- **repo_rank** (1–10)

Tests ensure:

- ranking consistency
- score completeness
- chronological coherence

Custom tests are located in:
tests/
- assert_ranking_consistency.sql
- assert_chronological_coherence.sql
- assert_scoring_completeness.sql
---

# dbt Incremental Strategies

Several staging models use **incremental materialization** to improve performance.

## stg_commits

**Strategy:** `append`

Git commits are **immutable**: once created, they never change.

Therefore we simply append new commits.

- Incremental column: `author_date`
- Reason: it represents the commit creation date.

---

## stg_issues

**Strategy:** `merge`

Issues can **change state over time** (open → closed).

Using `merge` allows updating existing rows.

- Incremental column: `updated_at`
- Unique key: `issue_number`

Reason: `updated_at` represents the **last modification timestamp**.

---

## stg_pull_requests

**Strategy:** `merge`

Pull requests evolve during their lifecycle:

- open
- merged
- closed

Thus existing rows must be updated.

- Incremental column: `updated_at`
- Unique key: `repo_full_name`

---

## stg_repositories

**Strategy:** `merge`

Repository metadata changes over time:

- stars
- forks
- last update

Using `merge` ensures the table reflects the latest state.

- Incremental column: `updated_at`
- Unique key: `repo_id`

---

# Snapshots

A **dbt snapshot** tracks historical changes to repositories.

Location:
snapshots/snap_repositories.yml

Configuration:

- **source**: `bronze.raw_repositories`
- **unique_key**: `repo_id`
- **strategy**: `timestamp`
- **updated_at**: `updated_at`

This snapshot allows tracking **changes in repository metadata over time**, such as:

- stars
- forks
- repository updates

# Project Structure
````css
  github_analytics/
  │
  ├── dbt_project.yml
  │
  ├── models/
  │ ├── bronze/
  │ │ └── _sources.yml
  │ │
  │ ├── silver/
  │ │ ├── stg_repositories.sql
  │ │ ├── stg_commits.sql
  │ │ ├── stg_pull_requests.sql
  │ │ ├── stg_issues.sql
  │ │ └── _schema.yml
  │ │
  │ ├── gold/
  │ │ ├── dim_repository.sql
  │ │ ├── dim_date.sql
  │ │ ├── dim_contributor.sql
  │ │ ├── fact_repo_activity.sql
  │ │ ├── scoring_repositories.sql
  │ │ └── _schema.yml
  │
  ├── tests/
  │ ├── assert_ranking_consistency.sql
  │ ├── assert_chronological_coherence.sql
  │ └── assert_scoring_completeness.sql
  │
  └── snapshots/
  └── snap_repositories.yml
````
---

# Running the Project

## 1 Install dependencies

```bash
pip install dbt-core dbt-duckdb
````

## 2 Run the Bronze ingestion script
scripts/load_bronze.py

## 3 Run dbt models

```bash
dbt run
````
## 4 Run tests

```bash
dbt test
````
Note: After our incremental implementation, when we runned the tests all passed except for bronze.raw_repositories (full_name repo duplicated) because the load_bronze.py program add again de full_name of the repositories for each new entry.

## 5 Run snapshots

```bash
dbt snapshot
````
Example of command in order to visualize the snapshots:

```bash
 dbt show --inline "select full_name, stargazers_count, dbt_valid_from, dbt_valid_to from main_snapshots.snap_repositories order by full_name, dbt_valid_from"
````




