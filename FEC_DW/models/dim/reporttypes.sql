with reporttypes as
(
	SELECT		distinct rt.ReportTypeCode, 
				rt.ReportTypeName, 
				rt.ReportTypeDesc
	FROM		{{source('fec','oppexp')}} s1
	INNER JOIN	{{source('fec','reporttypes')}} rt ON s.RPT_TP = tt.ReportTypeCode
	union
	SELECT		distinct tt.ReportTypeCode, 
				tt.ReportTypeName, 
				tt.ReportTypeDesc
	FROM		{{source('fec','itcont')}} s2
	INNER JOIN	{{source('fec','reporttypes')}} tt ON s.RPT_TP = tt.ReportTypeCode        
)
select
	{{ dbt_utils.generate_surrogate_key(['cm.CMTE_ID']) }} as reporttypekey,
	reporttypes.*
from 
	reporttypes