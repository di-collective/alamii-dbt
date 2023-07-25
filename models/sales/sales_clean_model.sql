{{ 
  config(
    materialized = 'table',
    full_refres = true,
    tags = ['sales']
  )
}}


select
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
  as area_simple,
  as city,
  as province,
  as dist_branc,
  as dist_kode_outlet,
  as dist_outlet_name,
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
from {{ ref('sales_backup_model') }}
where backup_date = current_date("Asia/Jakarta")
