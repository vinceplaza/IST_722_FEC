with committees as
(
	select distinct cmte_id from {{source('fec','oppexp')}} 
	union 
	select distinct cmte_id from {{source('fec','itcont')}} 
)
select		distinct 
			{{ dbt_utils.generate_surrogate_key(['cn.CAND_ID']) }} as CandidateKey, 
			cn.CAND_ID as CandidateId,
			cn.CAND_PCC as PrincipalCommitteeId,
			cn.CAND_PTY_AFFILIATION as PartyCode,
			cn.CAND_OFFICE as OfficeCode,
			cn.CAND_ELECTION_YR as ElectionYear,
			cn.CAND_OFFICE_ST as OfficeStateCode,
			cn.CAND_OFFICE_DISTRICT as DistrictNumber,
			cn.CAND_ICI as IncumbentCode,
			cn.CAND_STATUS as StatusCode,			
			cn.CAND_NAME as CandidateName,
			case CAND_OFFICE when 'P' then 'President'
						when 'H' then 'House'
						when 'S' then 'Senate'
						else 'Undefined' end as OfficeName,
			cn.CAND_ST1 as StreetAddress1,
			cn.CAND_ST2 as StreetAddress2,
			cn.CAND_CITY as City,
			cn.CAND_ST as StateCode,
			cn.CAND_ZIP as ZipCode
from		committees s
inner join	{{source('fec','cm')}} cm on s.CMTE_ID = cm.CMTE_ID
inner join	{{source('fec','cn')}} cn on cm.CAND_ID = cn.CAND_ID