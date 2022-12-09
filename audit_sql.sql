select
	uid as "uid",
	substring(created_time,1,10) as "创建时间"
from
	dwd.dwd_usr_user360_baseinfo_df
where 	
	created_time >= '2020-01-01'
	and 
	dt = dw_yesterday()
	
order by created_time
