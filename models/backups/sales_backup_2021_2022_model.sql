
{{ 
  config(
    materialized = 'table',
    full_refresh = true,
    tags = ['sales', 'backup', '2021', '2022']
  )
}}

select
  po_date as po_date,
  year_month_po as year_month_po,
  safe_cast(year_po as integer) as year_po,
  safe_cast(month_po as integer) as month_po,
  regexp_replace(trim(product), r'\s\s+', ' ') as product,
  regexp_replace(trim(variant), r'\s\s+', ' ') as variant,
  regexp_replace(trim(store_name_ori), r'\s\s+', ' ') as store_name_ori,
  regexp_replace(trim(store_name_clean), r'\s\s+', ' ') as store_name_clean,
  regexp_replace(trim(group_account), r'\s\s+', ' ') as group_account,
  regexp_replace(trim(city), r'\s\s+', ' ') as city,
  trim(type) as type,
  trim(sub_type) as sub_type,
  trim(channel) as channel,
  trim(dist) as dist,
  trim(area_simple) as area_simple,
  trim(province) as province,
  trim(dist_branc) as dist_branc,
  trim(dist_kode_outlet) as dist_kode_outlet,
  trim(dist_outlet_name) as dist_outlet_name,
  safe_cast(regexp_replace(_sales_pcs_, r'[^0-9.-]', '') as numeric) as sales_pcs,
  safe_cast(regexp_replace(_sales_ctn_, r'[^0-9.-]', '') as numeric) as sales_ctn,
  safe_cast(0 as numeric) as sales_qty3,
  safe_cast(_price_per_pc_rev_ as numeric) as price_per_pc_rev,
  safe_cast(regexp_replace(_sales_value_gtv_, r'[^0-9.-]', '') as numeric) as sales_value_gtv,
  safe_cast(regexp_replace(_sales_value_rev_, r'[^0-9.-]', '') as numeric) as sales_value_rev,
  safe_cast(regexp_replace(_sales_value_rbp_, r'[^0-9.-]', '') as numeric) as sales_value_rbp,
  trim(_account_manager_) as account_manager,
  trim(_supervisor_) as supervisor,
  safe_cast(_price_per_pc_rbp_ as numeric) as price_per_pc_rbp,
  safe_cast(null as date) as delivery_date,
  safe_cast(0 as numeric) as po_pcs,
  safe_cast(0 as numeric) as po_ctn,
  safe_cast(0 as numeric) as po_value_rbp,
  null as smd,
  null as sales_supervisor,
  null as sub_area
from alamii.sales_production.raw_sales_2021_2022
where po_date is not null
  and product is not null
  and variant is not null
  and city is not null
