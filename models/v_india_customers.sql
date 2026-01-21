{{
    config(
        materialized='table',
        transient ='False'
    )
}}

select c_custkey,c_name,c_address,c_phone,n_name
 from {{ source ('s1','customer')}} join {{ source ('s1','nation')}}
on c_nationkey=n_nationkey 
where c_nationkey=8