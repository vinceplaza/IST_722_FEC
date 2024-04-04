SELECT		DISTINCT {{ dbt_utils.generate_surrogate_key(['s.ENTITY_TP']) }} as EntityTypeKey,
            tt.EntityTypeCode, 
            tt.EntityType
FROM		{{source('fec','itcont')}} s
INNER JOIN	{{source('fec','entitytypes')}} tt on s.ENTITY_TP = tt.EntityTypeCode
	