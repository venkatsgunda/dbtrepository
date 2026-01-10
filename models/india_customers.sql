select c_custkey,c_name,c_address,c_phone,n_name
 from dev.sales.customer inner join dev.sales.nation
on c_nationkey=n_nationkey 
where c_nationkey=8