WITH contributors AS
(
    SELECT		DISTINCT 
                IFNULL(s.NAME, '{not provided}') AS ContributorName,
                IFNULL(s.CITY, '{not provided}') AS ContributorCity,
                IFNULL(s.STATE, 'na') AS ContributorState,
                IFNULL(SUBSTRING(s.ZIP_CODE,  1,5), '{na}') AS ContributorZipCode,
                IFNULL(s.EMPLOYER, '{na}')  AS Employer, 
                IFNULL(s.OCCUPATION, '{na}') AS Occupation,
                z.latitude,
				z.longitude
    FROM		{{source('fec','itcont')}} s   
    left join   {{source('fec','zipcodes')}} z on left(s.ZIP_CODE,5) = z.zipcode
), contributorswithkeys as
(
	select concat(s.ContributorName, s.ContributorCity, s.ContributorState, s.ContributorZipCode, s.Employer, s.Occupation) as surrogatekey,
	s.*
	from contributors s
)
select  {{ dbt_utils.generate_surrogate_key(['c.surrogatekey']) }} as ContributorKey,
        c.ContributorName,
        c.ContributorCity,
        c.ContributorState,
        c.ContributorZipCode,
        c.Employer,
        c.Occupation,
        c.latitude,
        c.longitude
from    contributorswithkeys c