		select		{{ dbt_utils.generate_surrogate_key(['s.SUB_ID']) }} as ExpenseKey,				
					com.CommitteeKey, 
					can.CandidateKey CandidateKey, 
					pay.PayeeKey,
					rpt.ReportTypeKey ReportTypeKey,
					ect.ElectionTypeKey ElectionTypeKey,
					ec.ExpenseCategoryKey ExpenseCategoryKey,
					et.ExpensePurposeKey ExpensePurposeKey,
					d.DateKey as ExpenseDateKey,
					s.RPT_YR as ReportYear,	
					cs.CAND_ELECTION_YR as ElectionYear,
					s.SUB_ID as TransactionId,									
					s.TRANSACTION_AMT as ExpenseAmountUSD,
					case s.MEMO_CD when 'x' then 1 else 0 end ExcludeFromTotals
		from		{{source('fec','oppexp')}} s 
		inner join	DW_DIM.Committees com on s.CMTE_ID = com.CommitteeId
		inner join	DW_DIM.Payees pay on s.NAME = pay.PayeeName 
					and s.CITY = pay.PayeeCity 
					and s.STATE = pay.PayeeState
					and substring(s.ZIP_CODE, 1, 5) = pay.PayeeZipCode 
		left join	DW_DIM.ReportTypes rpt on s.RPT_TP = rpt.ReportTypeCode
		left join	DW_DIM.ElectionTypes ect on s.TRANSACTION_PGI  = ect.ElectionTypeCode
		left join	DW_DIM.Dates d on TO_DATE(s.TRANSACTION_DT)  = D.Date and s.TRANSACTION_DT > ''
		left join	DW_DIM.Candidates can on com.CandidateId = can.CandidateId
											and case when com._ElectionYear is not null 
											then com._ElectionYear else can.ElectionYear end = can.ElectionYear
		left join	(select distinct CAND_ID, CAND_ELECTION_YR from {{source('fec','cn')}}) cs on can.CandidateId = cs.CAND_ID
		left join	DW_DIM.ExpensePurposes et on s.PURPOSE = et.ExpensePurpose
		left join	DW_DIM.ExpenseCategories ec on s.CATEGORY = ec.ExpenseCategoryCode
		--limit 1000