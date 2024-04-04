with pgis 
as
(
    select  distinct TRANSACTION_PGI from {{source('fec','oppexp')}}
    union
    select  distinct TRANSACTION_PGI from {{source('fec','itcont')}}

), electiontypecodes as
(   
    SELECT	distinct SUBSTRING(RTRIM(TRANSACTION_PGI), 1, 1) as ElectionTypeCode
    FROM    pgis--limit 100000
    WHERE	TRY_CAST(SUBSTRING(TRANSACTION_PGI, 1, 1) AS NUMERIC) IS NULL
            AND	TRANSACTION_PGI <> ''
)
select  {{ dbt_utils.generate_surrogate_key(['et.ElectionTypeCode']) }} as ElectionTypeKey,
        et.ElectionTypeCode,
        CASE et.ElectionTypeCode
        WHEN 'C' THEN 'Convention'
        WHEN 'G' THEN 'General'
        WHEN 'P' THEN 'Primary'
        WHEN 'R' THEN 'Runoff'
        WHEN 'S' THEN 'Special'
        ELSE '{unassigned}' END AS ElectionType 
FROM	electiontypecodes et

