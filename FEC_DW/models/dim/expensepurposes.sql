select distinct 
    {{ dbt_utils.generate_surrogate_key(['cm.CMTE_ID']) }} as expensepurposekey,
    purpose as expensepurpose
from		
    {{source('fec','oppexp')}}