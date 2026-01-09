select c_custkey,c_name,c_address,c_phone,n_name
 from dev.dev_schema.customer inner join dev.dev_schema.nation
on c_nationkey=n_nationkey 
where c_nationkey=8;