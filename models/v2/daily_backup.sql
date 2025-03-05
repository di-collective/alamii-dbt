{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'append_new_columns',
    partition_by = {
      "field": "ingest_at",
      "granularity": "day"
    },
    partition_expiration_days = 7,
    tags = ['v2', 'sales', 'backup']
  )
}}

select
  current_date("Asia/Jakarta") as ingest_at, -- current time when data is pulled from raw sales
  po_date as po_date,
  safe_cast(year_month_po as string) as year_month_po,
  safe_cast(year_po as string) as year_po,
  safe_cast(month_po as string) as month_po,
  safe_cast(product as string) as product,
  safe_cast(variant as string) as variant,
  safe_cast(store_name_ori as string) as store_name_ori,
  safe_cast(group_account as string) as group_account,
  safe_cast(type as string) as type,
  safe_cast(sub_type as string) as sub_type,
  safe_cast(channel as string) as channel,
  safe_cast(dist as string) as dist,
  safe_cast(area_simple as string) as area_simple,
  safe_cast(city as string) as city,
  safe_cast(province as string) as province,
  safe_cast(dist_branc as string) as dist_branc,
  safe_cast(dist_kode_outlet as string) as dist_kode_outlet,
  safe_cast(dist_outlet_name as string) as dist_outlet_name,
  safe_cast(_sales_pcs_ as string) as sales_pcs,
  safe_cast(_sales_ctn_ as string) as sales_ctn,
  safe_cast(_sales_qty3_ as string) as sales_qty3,
  safe_cast(_price_per_pc_rev_ as integer) as price_per_pc_rev,
  safe_cast(_sales_value_gtv_ as string) as sales_value_gtv,
  safe_cast(_sales_value_rev_ as string) as sales_value_rev,
  safe_cast(_sales_value_rbp_ as string) as sales_value_rbp,
  safe_cast(_account_manager_ as string) as account_manager,
  safe_cast(_supervisor_ as string) as supervisor,
  safe_cast(_price_per_pc_rbp_ as string) as price_per_pc_rbp,
  safe_cast(store_name_clean as string) as store_name_clean,
  format_date('%d-%m-%Y', delivery_date) as delivery_date,
  safe_cast(_po_pcs_ as string) as po_pcs,
  safe_cast(_po_ctn_ as string) as po_ctn,
  safe_cast(_po_value_rbp_ as string) as po_value_rbp,
  safe_cast(SMD as string) as smd,
  safe_cast(sales_supervisor as string) as sales_supervisor,
  safe_cast(sub_area as string) as sub_area
from alamii.sales_production.sales_2024_onwards
