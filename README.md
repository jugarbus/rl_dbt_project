# rl_dbt_project

# 🏎️ Rocket League Analytics: End-to-End Data Engineering Project

![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&style=flat-square)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Cloud-29B5E8?logo=snowflake&style=flat-square)
![Power BI](https://img.shields.io/badge/Power_BI-Visualization-F2C811?logo=powerbi&style=flat-square)

## 📖 Project Overview

This project implements a complete **ELT (Extract, Load, Transform)** pipeline to analyze professional **Rocket League** esports data.

The goal is to ingest raw match data from Kaggle into **Snowflake**, transform it using **dbt (data build tool)** from a complex normalized structure into an optimized **Star Schema**, and visualize insights in **Power BI**.

### 🎯 Key Objectives
* **Ingestion:** Load raw CSV data into Snowflake (Raw Layer).
* **Transformation:** Clean and normalize data into a Staging Layer (Silver).
* **Modeling:** Create a dimensional model (Gold Layer) optimized for BI performance.
* **Advanced dbt:** Implement **Snapshots** for tracking team changes and **Incremental Models** for high-volume game stats.

---

## 🏗️ Architecture & Data Flow

The project follows the **Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold):

### 1. 🥈 Silver Layer (Staging)
* **Source:** Raw data ingested from Kaggle.
* **Structure:** Highly normalized relational schema (3NF).
* **Content:** Tables such as `games`, `games_players`, `matches`, `events`, `series`, etc. This layer handles data cleaning, casting, and standardizing naming conventions.

### 2. 🥇 Gold Layer (Marts) - *The Star Schema*
* **Purpose:** Serving layer for Power BI.
* **Structure:** Dimensional Modeling (**Star Schema**).
* **Logic:** We denormalize the complex Silver layer into a central Fact table surrounded by Contextual Dimensions.

#### Data Model Diagram (Gold Layer)
The final model consists of the following tables:

* **Fact Table:**
    * `fct_game_player_stats`: The central table containing granular performance metrics per player per match (Goals, Saves, Boost usage, Speed stats, MVP status).

* **Dimension Tables:**
    * `dim_players`: Player attributes (Tag, Name, Country).
    * `dim_teams`: Team details (Name, Region). *Handled via Snapshots.*
    * `dim_games`: Match metadata (Duration, Overtime, Game Number).
    * `dim_matches`: Match context (Format, Rounds).
    * `dim_events`: Tournament info (Event Name, Region, Prize Money).
    * `dim_stages`: Tournament stages (Groups, Playoffs, LAN/Online).
    * `dim_date`: Date dimension for temporal analysis.
    * `dim_maps`, `dim_cars`, `dim_platforms`: Lookup dimensions for filtering.

---

## ⚙️ Key Technical Features

### 1. Incremental Materialization 📈
The `fct_game_player_stats` table contains the bulk of the data (millions of rows representing every player's stats in every game). Rebuilding this table daily is inefficient.

* **Solution:** We use **dbt incremental models**.
* **Logic:** dbt identifies new records based on the `game_date_id` or unique keys and only processes data added since the last run.

```sql
{{
  config(
    materialized='incremental',
    unique_key='game_player_sk',
    on_schema_change='fail'
  )
}}
SELECT ...
```

### 2. Snapshots for SCD Type 2 📸
In esports, teams frequently rebrand or change organizations. A player might play for "Team A" in 2022 and "Team B" in 2023. Overwriting the team name would destroy historical accuracy.

* **Solution:** We apply **dbt Snapshots** to the `teams` source data.
* **Outcome:** This creates a **Slowly Changing Dimension (Type 2)**. We track `dbt_valid_from` and `dbt_valid_to` columns to ensure that when we analyze a match from 2022, the player is associated with their team *at that time*, not their current team.

## 📂 Repository Structure

```bash
rl_dbt_project/
├── analyses/           # Ad-hoc SQL queries and analysis scripts
├── macros/             # Custom Jinja functions and hooks
├── models/             # Transformation logic (SQL files)
│   ├── staging/        # Silver Layer: Views/Tables mapped to source
│   └── marts/          # Gold Layer: Star Schema (Facts & Dimensions)
├── snapshots/          # SCD Type 2 logic (e.g., team history tracking)
├── tests/              # Data integrity and schema tests
├── dbt_project.yml     # Main project configuration file
├── packages.yml        # Project dependencies (e.g., dbt_utils)
└── README.md           # Project documentation

## 📊 Visualization
The data in the Gold Layer is optimized for Power BI. The fct_game_player_stats table serves as the center of the star schema, allowing efficient filtering by Date, Event, or Team dimensions.
