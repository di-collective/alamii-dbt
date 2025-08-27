{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'append_new_columns',
    unique_key = ['backup_date'],
    partition_by = {
      "field": "backup_date",
      "granularity": "day"
    },
    partition_expiration_days = 7,
    tags = ['sales', 'backup']
  )
}}

select 
  current_date("Asia/Jakarta") as backup_date, 
  * except (backup_date),
  safe_cast(null as string) as store_status,
  safe_cast(null as string) as sales_type
from {{ ref('sales_backup_2023_model') }}

union all

select
  current_date("Asia/Jakarta") as backup_date,
  * except (backup_date),
  safe_cast(null as string) as store_status,
  safe_cast(null as string) as sales_type
from {{ ref('sales_backup_2024_direct_model') }}

union all

select 
  current_date("Asia/Jakarta") as backup_date, 
  * except (backup_date),
  safe_cast(null as string) as store_status,
  safe_cast(null as string) as sales_type
from {{ ref('sales_backup_2024_distributor_model') }}

union all

select * from {{ ref('sales_backup_2025_onwards') }}
