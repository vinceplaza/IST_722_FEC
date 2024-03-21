with expensetypes 
as
(
    SELECT	RPT_TP FROM {{source('fec','oppexp')}} limit 100000
), contribtypes as
(
    SELECT	RPT_TP FROM {{source('fec','itcont')}} limit 100000
), alltypes as
(
    select distinct RPT_TP from expensetypes
    UNION
    select distinct RPT_TP from contribtypes
)
	SELECT		
				{{ dbt_utils.generate_surrogate_key(['s.RPT_TP']) }} as reporttypekey,
				rt.ReportTypeCode, 
				rt.ReportTypeName, 
				rt.ReportTypeDesc
	FROM		alltypes s
	INNER JOIN	{{source('fec','reporttypes')}} rt ON s.RPT_TP = rt.ReportTypeCode
