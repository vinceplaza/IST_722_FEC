with expensetypes 
as
(
    SELECT	transaction_pgi FROM {{source('fec','oppexp')}} limit 100000
), contribtypes as
(
    SELECT	transaction_pgi FROM {{source('fec','itcont')}} limit 100000
), alltypes as
(
    select transaction_pgi from expensetypes
    UNION
    select transaction_pgi from contribtypes
), electiontypes as
(
    select 
                distinct
                SUBSTRING(RTRIM(o.TRANSACTION_PGI), 1, 1) AS ElectionTypeCode,
                CASE SUBSTRING(RTRIM(o.TRANSACTION_PGI), 1, 1)
                WHEN 'C' THEN 'Convention'
                WHEN 'G' THEN 'General'
                WHEN 'P' THEN 'Primary'
                WHEN 'R' THEN 'Runoff'
                WHEN 'S' THEN 'Special'
                ELSE '{unassigned}' END AS ElectionType 
    FROM		alltypes o
    WHERE		TRY_CAST(SUBSTRING(o.TRANSACTION_PGI, 1, 1) AS NUMERIC) IS NULL
    AND			RTRIM(o.TRANSACTION_PGI) <> ''
)
select     {{ dbt_utils.generate_surrogate_key(['et.ElectionTypeCode']) }} as ElectionTypeKey,
            et.*
from        electiontypes et
-- )
-- select * from electiontypes
-- select  
--     {{ dbt_utils.generate_surrogate_key(['et.ElectionTypeCode']) }} as ElectionTypeKey,
--     electiontypes.*
-- from electiontypes et