select		distinct 
            {{ dbt_utils.generate_surrogate_key(['cm.CMTE_ID']) }} as CommitteeKey, 
            cm.CMTE_ID as CommitteeId, 
            cm.CAND_ID as CandidateId,
            cm.CMTE_PTY_AFFILIATION as PartyCode,
            cm.CMTE_TP as TypeCode,
            cm.CMTE_DSGN as DesignationCode,
            cm.CMTE_FILING_FREQ as FilingFrequencyCode,
            cm.ORG_TP as OrganizationTypeCode,
            cm.CMTE_NM as CommitteeName,
            cm.TRES_NM as TreasurerName,
            case cm.CMTE_DSGN when 'A' then 'Authorized by a candidate'
                            when 'B' then 'Lobbyist/Registrant PAC'
                            when 'D' then 'Leadership PAC'
                            when 'J' then 'Joint fundraiser'
                            when 'P' then 'Principal campaign committee of a candidate'
                            when 'U' then 'Unautorized'
                            else 'Undefined' end as DesignationDescription,
            case cm.CMTE_FILING_FREQ when 'A' then 'Administratively terminated'
                            when 'D' then 'Debt'
                            when 'M' then 'Monthly filer'
                            when 'Q' then 'Quarterly filer'
                            when 'T' then 'Terminated'
                            when 'W' then 'Waived'
                            else 'Undefined' end as FilingFrequencyLabel,
            case ORG_TP when 'C' then 'Corporation'
                            when 'L' then 'Labor organization'
                            when 'M' then 'Membership organization'
                            when 'T' then 'Trade association'
                            when 'V' then 'Cooperative'
                            when 'W' then 'Corporation without capital stock'
                            else 'Undefined' end as OrganizationTypeLabel,
            cm.CONNECTED_ORG_NM as OrganizationName,
            cm.CMTE_ST1 as StreetAddress1,
            cm.CMTE_ST2 as StreetAddress2,
            cm.CMTE_CITY as City,
            cm.CMTE_ST as StateCode,
            cm.CMTE_ZIP as ZipCode,
            cn.CAND_ELECTION_YR as _ElectionYear				
from		{{source('fec','oppexp')}} s
inner join	{{source('fec','cm')}} cm on s.CMTE_ID = cm.CMTE_ID
left join   {{source('fec','cn')}} cn on cm.CAND_ID = cn.CAND_ID