{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns',
    unique_key = ['id'],
    tags = ['v2', 'sales', 'clean']
  )
}}

select
  upper(concat(
    format_date("%Y%m%d", po_date),
    substr(regexp_replace(product, "\\s", ""), 1, 5),
    substr(regexp_replace(variant, "\\s", ""), 1, 10),
    substr(regexp_replace(store_name_clean, "\\s", ""), 1, 5),
    substr(regexp_replace(store_name_clean, "\\s", ""), length(regexp_replace(store_name_clean, "\\s", ""))-4),
    substr(regexp_replace(city, "\\s", ""), 1, 5),
    substr(regexp_replace(city, "\\s", ""), length(regexp_replace(city, "\\s", ""))-4)
  )) as id,
  po_date as po_date,
  year_month_po as year_month_po,
  safe_cast(year_po as integer) as year_po,
  safe_cast(month_po as integer) as month_po,
  product as product,
  variant as variant,
  store_name_ori as store_name_ori,
  group_account as group_account,
  type as type,
  sub_type as sub_type,
  channel as channel,
  dist as dist,
  area_simple as area_simple,
  city as city,
  province as province,
  dist_branc as dist_branc,
  dist_kode_outlet as dist_kode_outlet,
  dist_outlet_name as dist_outlet_name,
  safe_cast(regexp_replace(sales_pcs, r'[^0-9.-]', '') as numeric) as sales_pcs,
  safe_cast(regexp_replace(sales_ctn, r'[^0-9.-]', '') as numeric) as sales_ctn,
  safe_cast(regexp_replace(sales_qty3, r'[^0-9.-]', '') as numeric) as sales_qty3,
  safe_cast(price_per_pc_rev as numeric) as price_per_pc_rev,
  safe_cast(regexp_replace(sales_value_gtv, r'[^0-9.-]', '') as numeric) as sales_value_gtv,
  safe_cast(regexp_replace(sales_value_rev, r'[^0-9.-]', '') as numeric) as sales_value_rev,
  safe_cast(regexp_replace(sales_value_rbp, r'[^0-9.-]', '') as numeric) as sales_value_rbp,
  account_manager as account_manager,
  supervisor as supervisor,
  safe_cast(price_per_pc_rbp as numeric) as price_per_pc_rbp,
  store_name_clean as store_name_clean,
  parse_date('%d/%m/%E4Y', replace(delivery_date, '-', '/')) as delivery_date,
  safe_cast(regexp_replace(po_pcs, r'[^0-9.-]', '') as numeric) as po_pcs,
  safe_cast(regexp_replace(po_ctn, r'[^0-9.-]', '') as numeric) as po_ctn,
  safe_cast(regexp_replace(po_value_rbp, r'[^0-9.-]', '') as numeric) as po_value_rbp,
  smd,
  sales_supervisor,
  sub_area
from {{ ref('daily_backup') }}
where ingest_at = current_date("Asia/Jakarta")
