with payees as
(
	SELECT		DISTINCT 
				s.NAME as PayeeName, 
				ifnull(s.CITY, 'ABCDEFGHIJK') as PayeeCity,
				ifnull(s.STATE, 'ZZ') as PayeeStateAbbr,
				ifnull(left(s.ZIP_CODE,5),'00000') as PayeeZipCode,
				z.latitude,
				z.longitude

	FROM		{{source('fec','oppexp')}} s
	LEFT join  {{source('fec','zipcodes')}} z on left(s.ZIP_CODE,5) = z.zipcode
	WHERE S.NAME IS NOT NULL
	limit 100000
), payeeswithkeys as
(
	select concat(s.PayeeName , s.PayeeCity, S.PayeeStateAbbr, s.PayeeZipCode) as surrogatekey,
	s.*
	from payees s
)
select
{{ dbt_utils.generate_surrogate_key(['p.surrogatekey']) }} as PayeeKey,
p.*
from payeeswithkeys p