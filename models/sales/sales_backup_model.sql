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

with sales_2022_onwards as (
  select
    current_date("Asia/Jakarta") as backup_date, -- current time when data is pulled from raw sales
    po_date as po_date,
    year_month_po as year_month_po,
    year_po as year_po,
    month_po as month_po,
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
    safe_cast(_sales_pcs_ as string) as sales_pcs,
    safe_cast(_sales_ctn_ as string) as sales_ctn,
    _sales_qty3_ as sales_qty3,
    safe_cast(_price_per_pc_rev_ as integer) as price_per_pc_rev,
    safe_cast(_sales_value_gtv_ as string) as sales_value_gtv,
    safe_cast(_sales_value_rev_ as string) as sales_value_rev,
    safe_cast(_sales_value_rbp_ as string) as sales_value_rbp,
    _account_manager_ as account_manager,
    _supervisor_ as supervisor,
    safe_cast(_price_per_pc_rbp_ as integer) as price_per_pc_rbp,
    store_name_clean as store_name_clean,
    format_date('%d-%m-%Y', delivery_date) as delivery_date,
    safe_cast(_po_pcs_ as string) as po_pcs,
    safe_cast(_po_ctn_ as string) as po_ctn,
    _po_value_rbp_ as po_value_rbp,
    SMD as smd,
    sales_supervisor,
    sub_area
  from alamii.sales_production.raw_sales_v4
  where year_po >= 2022
)

select * from sales_2022_onwards
