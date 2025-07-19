{{ 
  config(
    materialized = 'view',
    tags = ['v2', 'sales', 'clean']
  )
}}

select
  id, po_date, year_month_po, year_po, month_po, product, variant, store_name_ori, group_account, type, sub_type, channel, dist,
  area_simple, city, province, dist_branc, dist_kode_outlet, dist_outlet_name, sales_pcs, sales_ctn, sales_qty3, price_per_pc_rev,
  sales_value_gtv, sales_value_rev, sales_value_rbp, account_manager, supervisor, price_per_pc_rbp, store_name_clean, delivery_date,
  po_pcs, po_ctn, po_value_rbp, smd, sales_supervisor, sub_area
from {{ ref("sales_clean_model") }}
union all
select
  id, po_date, year_month_po, year_po, month_po, product, variant, store_name_ori, group_account, type, sub_type, channel, dist,
  area_simple, city, province, dist_branc, dist_kode_outlet, dist_outlet_name, sales_pcs, sales_ctn, sales_qty3, price_per_pc_rev,
  sales_value_gtv, sales_value_rev, sales_value_rbp, account_manager, supervisor, price_per_pc_rbp, store_name_clean, delivery_date,
  po_pcs, po_ctn, po_value_rbp, smd, sales_supervisor, sub_area
from {{ ref("v2_sales_drive_final") }}