#!/bin/bash

basedir=$1

list=(
# Dimension Tables
"store s_store_sk:ID(STORE)|s_store_id:STRING|s_rec_start_date:DATE|s_rec_end_date:DATE|s_closed_date_sk:ULONG|s_store_name:STRING|s_number_employees:LONG|s_floor_space:LONG|s_hours:STRING|S_manager:STRING|S_market_id:LONG|S_geography_class:STRING|S_market_desc:STRING|s_market_manager:STRING|s_division_id:LONG|s_division_name:STRING|s_company_id:LONG|s_company_name:STRING|s_street_number:STRING|s_street_name:STRING|s_street_type:STRING|is_suite_number:STRING|s_city:STRING|s_county:STRING|s_state:STRING|s_zip:STRING|s_country:STRING|s_gmt_offset:DECIMAL(5,2)|s_tax_percentage:DECIMAL(5,2)"
"call_center cc_call_center_sk:ID(CALL_CENTER)|cc_call_center_id:STRING|cc_rec_start_date:DATE|cc_rec_end_date:DATE|cc_closed_date_sk:ULONG|cc_open_date_sk:ULONG|cc_name:STRING|cc_class:STRING|cc_employees:LONG|cc_sq_ft:LONG|cc_hours:STRING|cc_manager:STRING|cc_mkt_id:LONG|cc_mkt_class:STRING|cc_mkt_desc:STRING|cc_market_manager:STRING|cc_division:LONG|cc_division_name:STRING|cc_company:LONG|cc_company_name:STRING|cc_street_number:STRING|cc_street_name:STRING|cc_street_type:STRING|cc_suite_number:STRING|cc_city:STRING|cc_county:STRING|cc_state:STRING|cc_zip:STRING|cc_country:STRING|cc_gmt_offset:DECIMAL(5,2)|cc_tax_percentage:DECIMAL(5,2)"
"catalog_page cp_catalog_page_sk:ID(CATALOG_PAGE)|cp_catalog_page_id:STRING|cp_start_date_sk:ULONG|cp_end_date_sk:ULONG|cp_department:STRING|cp_catalog_number:LONG|cp_catalog_page_number:LONG|cp_description:STRING|cp_type:STRING"
"web_site web_site_sk:ID(WEB_SITE)|web_site_id:STRING|web_rec_start_date:DATE|web_rec_end_date:DATE|web_name:STRING|web_open_date_sk:ULONG|web_close_date_sk:ULONG|web_class:STRING|web_manager:STRING|web_mkt_id:LONG|web_mkt_class:STRING|web_mkt_desc:STRING|web_market_manager:STRING|web_company_id:LONG|web_company_name:STRING|web_street_number:STRING|web_street_name:STRING|web_street_type:STRING|web_suite_number:STRING|web_city:STRING|web_county:STRING|web_state:STRING|web_zip:STRING|web_country:STRING|web_gmt_offset:DECIMAL(5,2)|web_tax_percentage:DECIMAL(5,2)"
"web_page wp_web_page_sk:ID(WEB_PAGE)|wp_web_page_id:STRING|wp_rec_start_date:DATE|wp_rec_end_date:DATE|wp_creation_date_sk:ULONG|wp_access_date_sk:ULONG|wp_autogen_flag:STRING|wp_customer_sk:ULONG|wp_url:STRING|wp_type:STRING|wp_char_count:LONG|wp_link_count:LONG|wp_image_count:LONG|wp_max_ad_count:LONG"
"warehouse w_warehouse_sk:ID(WAREHOUSE)|w_warehouse_id:STRING|w_warehouse_name:STRING|w_warehouse_sq_ft:LONG|w_street_number:STRING|w_street_name:STRING|w_street_type:STRING|w_suite_number:STRING|w_city:STRING|w_county:STRING|w_state:STRING|w_zip:STRING|w_country:STRING|w_gmt_offset:DECIMAL(5,2)"
"customer c_customer_sk:ID(CUSTOMER)|c_customer_id:STRING|c_current_cdemo_sk:ULONG|c_current_hdemo_sk:ULONG|c_current_addr_sk:ULONG|c_first_shipto_date_sk:ULONG|c_first_sales_date_sk:ULONG|c_salutation:STRING|c_first_name:STRING|c_last_name:STRING|c_preferred_cust_flag:STRING|c_birth_day:LONG|c_birth_month:LONG|c_birth_year:LONG|c_birth_country:STRING|c_login:STRING|c_email_address:STRING|c_last_review_date_sk:ULONG"
"customer_address ca_address_sk:ID(CUSTOMER_ADDRESS)|ca_address_id:STRING|ca_street_number:STRING|ca_street_name:STRING|ca_street_type:STRING|ca_suite_number:STRING|ca_city:STRING|ca_county:STRING|ca_state:STRING|ca_zip:STRING|ca_country:STRING|ca_gmt_offset:DECIMAL(5,2)|ca_location_type:STRING"
"customer_demographics cd_demo_sk:ID(CUSTOMER_DEMOGRAPHICS)|cd_gender:STRING|cd_marital_status:STRING|cd_education_status:STRING|cd_purchase_estimate:LONG|cd_credit_rating:STRING|cd_dep_count:LONG|cd_dep_employed_count:LONG|cd_dep_college_count:LONG"
"date_dim d_date_sk:ID(DATE_DIM)|d_date_id:STRING|d_date:DATE|d_month_seq:LONG|d_week_seq:LONG|d_quarter_seq:LONG|d_year:LONG|d_dow:LONG|d_moy:LONG|d_dom:LONG|d_qoy:LONG|d_fy_year:LONG|d_fy_quarter_seq:LONG|d_fy_week_seq:LONG|d_day_name:STRING|d_quarter_name:STRING|d_holiday:STRING|d_weekend:STRING|d_following_holiday:STRING|d_first_dom:LONG|d_last_dom:LONG|d_same_day_ly:LONG|d_same_day_lq:LONG|d_current_day:STRING|d_current_week:STRING|d_current_month:STRING|d_current_quarter:STRING|d_current_year:STRING"
"household_demographics hd_demo_sk:ID(HOUSEHOLD_DEMOGRAPHICS)|hd_income_band_sk:ULONG|hd_buy_potential:STRING|hd_dep_count:LONG|hd_vehicle_count:LONG"
"item i_item_sk:ID(ITEM)|i_item_id:STRING|i_rec_start_date:DATE|i_rec_end_date:DATE|i_item_desc:STRING|i_current_price:DECIMAL(7,2)|i_wholesale_cost:DECIMAL(7,2)|i_brand_id:LONG|i_brand:STRING|i_class_id:LONG|i_class:STRING|i_category_id:LONG|i_category:STRING|i_manufact_id:LONG|i_manufact:STRING|i_size:STRING|i_formulation:STRING|i_color:STRING|i_units:STRING|i_container:STRING|i_manager_id:LONG|i_product_name:STRING"
"income_band ib_income_band_sk:ID(INCOME_BAND)|ib_lower_bound:LONG|ib_upper_bound:LONG"
"promotion p_promo_sk:ID(PROMOTION)|p_promo_id:STRING|p_start_date_sk:ULONG|p_end_date_sk:ULONG|p_item_sk:ULONG|p_cost:DECIMAL(15,2)|p_response_target:LONG|p_promo_name:STRING|p_channel_dmail:STRING|p_channel_email:STRING|p_channel_catalog:STRING|p_channel_tv:STRING|p_channel_radio:STRING|p_channel_press:STRING|p_channel_event:STRING|p_channel_demo:STRING|p_channel_details:STRING|p_purpose:STRING|p_discount_active:STRING"
"reason r_reason_sk:ID(REASON)|r_reason_id:STRING|r_reason_desc:STRING"
"ship_mode sm_ship_mode_sk:ID(SHIP_MODE)|sm_ship_mode_id:STRING|sm_type:STRING|sm_code:STRING|sm_carrier:STRING|sm_contract:STRING"
"time_dim t_time_sk:ID(TIME_DIM)|t_time_id:STRING|t_time:LONG|t_hour:LONG|t_minute:LONG|t_second:LONG|t_am_pm:STRING|t_shift:STRING|t_sub_shift:STRING|t_meal_time:STRING"
# Fact Tables
"store_sales"
"store_returns"
"catalog_sales"
"catalog_returns"
"web_sales"
"web_returns"
"inventory"
# Relationships
""
""
""
""
""
""
""
""
""
""
""
""
""
"")

for ((i = 0; i < ${#list[@]}; i++)); do
	IFS=' ' read -ra array <<< "${list[$i]}"
	echo ${array[0]} ${array[1]}
	#sed -i '1s/^/'${array[1]}'\n/' "${basedir}/${array[0]}.dat"
done
