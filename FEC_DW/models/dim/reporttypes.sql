with expensetypes 
as
(
    select	RPT_TP from {{source('fec','oppexp')}} limit 100000
), contribtypes as
(
    select	RPT_TP from {{source('fec','itcont')}} limit 100000
), alltypes as
(
    select distinct RPT_TP from expensetypes
    union
    select distinct RPT_TP from contribtypes
)
	select		{{ dbt_utils.generate_surrogate_key(['s.RPT_TP']) }} as reporttypekey,
				rt.ReportTypeCode, 
				rt.ReportTypeName, 
				rt.ReportTypeDesc
	from		alltypes s
	inner join	{{source('fec','reporttypes')}} rt ON s.RPT_TP = rt.ReportTypeCode
