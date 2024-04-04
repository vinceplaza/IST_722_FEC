SELECT		DISTINCT 
            {{ dbt_utils.generate_surrogate_key(['s.AMNDT_IND']) }} as AmendmentTypeKey,
            a.AmendmentTypeCode, 
            a.AmendmentType
FROM		{{source('fec','itcont')}} s
INNER JOIN	{{source('fec','amendmenttypes')}} a on s.AMNDT_IND = a.AmendmentTypeCode