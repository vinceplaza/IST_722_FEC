with payees as
(
	select		distinct 
				s.NAME as PayeeName, 
				ifnull(s.CITY, 'ABCDEFGHIJK') as PayeeCity,
				ifnull(s.STATE, 'ZZ') as PayeeState,
				ifnull(left(s.ZIP_CODE,5),'00000') as PayeeZipCode,
				z.latitude,
				z.longitude
	from		{{source('fec','oppexp')}} s
	left join   {{source('fec','zipcodes')}} z on left(s.ZIP_CODE,5) = z.zipcode
	where 		S.NAME IS NOT NULL
), payeeswithkeys as
(
	select concat(s.PayeeName , s.PayeeCity, S.PayeeState, s.PayeeZipCode) as surrogatekey,
	s.*
	from payees s
)
SELECT	{{ dbt_utils.generate_surrogate_key(['p.surrogatekey']) }} as PayeeKey,
		p.PayeeName,
		p.PayeeCity,
		p.PayeeState,
		p.PayeeZipCode,
		p.latitude,
		p.longitude
from 	payeeswithkeys p