select	    {{ dbt_utils.generate_surrogate_key(['i.SUB_ID']) }} as ContributionKey,
            com.CommitteeKey, 
            can.CandidateKey AS CandidateKey, 
            p.ContributorKey,
            et.EntityTypeKey AS EntityTypeKey, 
            rt.ReportTypeKey, 
            elt.ElectionTypeKey AS ElectionTypeKey,
            tt.TransactionTypeKey,
            att.AmendmentTypeKey AS AmendmentTypeKey,
            cast(substring(i.TRANSACTION_DT,5,4) + SUBSTRING(i.TRANSACTION_DT, 1,4) as int) AS ContributionDateKey,
            ifnull(cs.CAND_ELECTION_YR,-1) as ElectionYear,
            i.SUB_ID AS TransactionId,
            i.TRANSACTION_AMT AS ContributionAmountUSD
from		{{source('fec','itcont')}}  i
inner join	DW_DIM.Committees com on i.CMTE_ID = com.CommitteeId
inner join	DW_DIM.Contributors p on IFNULL(i.NAME, '{not provided}') = p.ContributorName 
        and IFNULL(i.CITY, '{not provided}') = p.ContributorCity 
        and IFNULL(i.STATE, 'na') = p.ContributorState 
        and IFNULL(SUBSTRING(i.ZIP_CODE,  1,5), '{na}') = p.ContributorZipCode 
        and IFNULL(i.EMPLOYER, '{na}') = p.Employer 
        and IFNULL(i.OCCUPATION, '{na}') = p.Occupation
inner join	DW_DIM.ReportTypes rt on case i.RPT_TP when '24' then '24H' else i.RPT_TP end = rt.ReportTypeCode
inner join	DW_DIM.TransactionTypes tt on i.TRANSACTION_TP = tt.TransactionTypeCode
left join	DW_DIM.ENTITYTYPES et on i.ENTITY_TP = et.EntityTypeCode
left join	DW_DIM.AmendmentTypes att on i.AMNDT_IND = att.AmendmentTypeCode
left join	DW_DIM.ElectionTypes elt  on SUBSTRING(i.TRANSACTION_PGI,1,1) = elt.ElectionTypeCode
left join	{{source('fec','cm')}}  sm on i.CMTE_ID = sm.CMTE_ID
left join	DW_DIM.Candidates can on com.CandidateId = can.CandidateId
                                    and case when com._ElectionYear is not null 
                                    then com._ElectionYear else can.ElectionYear end = can.ElectionYear
left join	(select distinct CAND_ID, CAND_ELECTION_YR from {{source('fec','cn')}}) cs on can.CandidateId = cs.CAND_ID
--limit 1000