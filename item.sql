-- 导出所有产品计费项
with
	item_zone_info as (
		select
			t1.id as item_id
			, t4.id as zone_id
			, t1.code as item_code
			, t1.name as item_name
			, t4.code as zone_code
			, t4.title as zone_name
		from
			ods.ods_billing_pay_dict_items_df t1
			, ods.ods_billing_pay_dict_item_groups_df t2
			, ods.ods_billing_pay_dict_products_df t3
			, ods.ods_billing_pay_dict_zones_df t4
			, ods.ods_billing_pay_dict_zone_item_maps_df t5
		where
			t1.dt = dw_yesterday()
			and t2.dt = dw_yesterday()
			and t3.dt = dw_yesterday()
			and t4.dt = dw_yesterday()
			and t5.dt = dw_yesterday()
			and t1.group_id = t2.id
			and t2.product_id = t3.id
			and t4.id = t5.zone_id
			and t1.id = t5.item_id
			and t3.code = 'kodo'
			-- and t4.code = 5
	),
	t_price_items_ex as (
        select *
        from (
            select
                *,
                from_unixtime_nanos(effect_time*100) as effective_time_humanized,
                from_unixtime_nanos(dead_time*100) as dead_time_humanized,
                row_number() over (
                    partition by
                        price_id, item_id, zone_id, algorithm_id, stair_price_type, is_disabled, unit_rate, "type", should_skip, cumulative_cycle, bill_period_type, is_default_algorithm, currency_type
                    order by
                        effect_time desc
                ) as row_num
            from ods.ods_billing_pay_price_price_items_df
            where dt = dw_yesterday()
                and from_unixtime_nanos(effect_time * 100) <= now()
                and from_unixtime_nanos(dead_time * 100) > now()
        )
        where row_num = 1
	),
	price_info as (
		select
			-- t3.item_id as item_id
			-- , t3.zone_id as zone_id
			t3.item_code as "计费项code"
			--, t3.zone_code as zone_code
			, t3.item_name as "计费项名称"
			, t3.zone_name as "区域"
			, t2."order" as "阶梯顺序"
			, case 
				when t2.quantity >= 1024 and t2.quantity < 1024*1024 and t4.name = 'GB' then t2.quantity/1024
				when t2.quantity >= 1024*1024 and t4.name = 'GB' then t2.quantity/1024/1024
				else t2.quantity
			end as "阶梯范围"
			, case 
				when t2.quantity >= 1024 and t2.quantity < 1024*1024 and t4.name = 'GB' then 'TB'
				when t2.quantity >= 1024*1024 and t4.name = 'GB' then 'PB'
				else t4.name 
			end as "单位"
			, t2.price / 10000.0000 as "价格"
			, t1.currency_type as "币种"
			--, t1.id as "Price_item_id"
		from
			t_price_items_ex t1
			, ods.ods_billing_pay_price_price_item_stairs_df t2
			, item_zone_info t3
			, ods.ods_billing_pay_dict_item_data_type_units_df t4
		where
			t1.dt = dw_yesterday()
			and t2.dt = dw_yesterday()
			and t4.dt = dw_yesterday()
			and t1.price_id = 3
			and t1.id = t2.price_item_id
			and t1.item_id = t3.item_id
			and t1.zone_id = t3.zone_id
			and t4.id = t2.unit_id
			and t1.is_default_algorithm = 1
		
		order by item_code asc, zone_code asc,"order" asc
	)
select * from price_info

