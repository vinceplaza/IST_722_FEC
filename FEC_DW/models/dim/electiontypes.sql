with election types as
(
    SELECT		DISTINCT 
                SUBSTRING(RTRIM(o.TRANSACTION_PGI), 1, 1) AS ElectionTypeCode,
                CASE SUBSTRING(RTRIM(o.TRANSACTION_PGI), 1, 1)
                WHEN 'C' THEN 'Convention'
                WHEN 'G' THEN 'General'
                WHEN 'P' THEN 'Primary'
                WHEN 'R' THEN 'Runoff'
                WHEN 'S' THEN 'Special'
                ELSE '{unassigned}' END AS ElectionTypeName 
    FROM		{{source('fec','oppexp')}} o
    WHERE		ISNUMERIC(SUBSTRING(o.TRANSACTION_PGI, 1, 1)) <>1
    AND			RTRIM(o.TRANSACTION_PGI) <> ''
    UNION
    SELECT		DISTINCT 
                SUBSTRING(RTRIM(o.TRANSACTION_PGI), 1, 1) AS ElectionTypeCode,
                CASE SUBSTRING(RTRIM(o.TRANSACTION_PGI), 1, 1)
                WHEN 'C' THEN 'Convention'
                WHEN 'G' THEN 'General'
                WHEN 'P' THEN 'Primary'
                WHEN 'R' THEN 'Runoff'
                WHEN 'S' THEN 'Special'
                ELSE '{unassigned}' END AS ElectionTypeName 
    FROM		{{source('fec','itcont')}} o
    WHERE		ISNUMERIC(SUBSTRING(o.TRANSACTION_PGI, 1, 1)) <>1
    AND			RTRIM(o.TRANSACTION_PGI) <> ''
)

select  
    {{ dbt_utils.generate_surrogate_key(['cm.CMTE_ID']) }} as ElectionTypeKey,
    electiontypes.*
from electiontypes