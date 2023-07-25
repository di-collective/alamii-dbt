{{ 
  config(
    materialized = 'incremental',
    on_schema_change = 'append_new_columns',
    unique_key = ['backup_date'],
    tags = ['sales', 'backup'],

    -- delete current day data just in case it's already synced
    pre_hook = 'delete from {{this}} where date(backup_date) = date(current_date("Asia/Jakarta"))',
    -- delete backup older than 7 days
    post_hook = 'delete from {{this}} where date(backup_date) <= date_sub(current_date("Asia/Jakarta"), INTERVAL 7 DAY)'
  )
}}

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
  _sales_pcs_ as sales_pcs,
  _sales_ctn_ as sales_ctn,
  _price_per_pc_rev_ as price_per_pc_rev,
  _sales_value_gtv_ as sales_value_gtv,
  _sales_value_rev_ as sales_value_rev,
  _sales_value_rbp_ as sales_value_rbp,
  _account_manager_ as account_manager,
  _supervisor_ as supervisor,
  _price_per_pc_rbp_ as price_per_pc_rbp,
  store_name_clean as store_name_clean
from alamii.sales_production.raw_sales
