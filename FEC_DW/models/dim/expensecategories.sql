select distinct
    {{ dbt_utils.generate_surrogate_key(['ec.ExpenseCategoryCode']) }} as EXPENSECATEGORYKEY,
    ec.ExpenseCategoryCode,
    ec.ExpenseCategoryLabel,
    ec.ExpenseCategoryDesc
from	
    {{source('fec','oppexp')}} s
    INNER JOIN	{{source('fec','expensecategories')}} ec on RTRIM(s.CATEGORY) = ec.ExpenseCategoryCode