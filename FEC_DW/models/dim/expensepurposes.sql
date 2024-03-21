with purposes as
(
select distinct 
    purpose as expensepurpose
from		
    {{source('fec','oppexp')}}
where purpose is not null
)
select 
    {{ dbt_utils.generate_surrogate_key(['expensepurpose']) }} as expensepurposekey,
    expensepurpose
    from purposes