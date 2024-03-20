		select		com.CommitteeKey, 
					isnull(can.CandidateKey, -1) CandidateKey, 
					pay.PayeeKey,
					isnull(rpt.ReportTypeKey,-1) ReportTypeKey,
					isnull(ect.ElectionTypeKey,-1) ElectionTypeKey,
					isnull(ec.ExpenseCategoryKey, -1) ExpenseCategoryKey,
					isnull(et.ExpensePurposeKey, -1) ExpensePurposeKey,
					ISNULL(d.DateKey, -1) as ExpenseDateKey,
					s.TRANSACTION_AMT as ExpenseAmountUSD,
					isnull(cs.CAND_ELECTION_YR,-1) as ElectionYear,
					s.RPT_YR as ReportYear,
					case s.MEMO_CD when 'x' then 1 else 0 end ExcludeFromTotals,
					@DataYear as _FecDataYear,
					s.SUB_ID as _FecTransactionId
		from		{{source('fec','oppexp')}} s 
		inner join	DW_DIM.Committees com on s.CMTE_ID = com.CommitteeId
		inner join	DW_DIM.Payees pay on s.NAME = pay.PayeeName 
					and s.CITY = pay.PayeeCity 
					and s.STATE = pay.PayeeStateAbbr
					and substring(s.ZIP_CODE, 1, 5) = pay.PayeeZipCode 
		left join	DW_DIM.ReportTypes rpt on s.RPT_TP = rpt.ReportTypeCode
		left join	DW_DIM.ElectionTypes ect on s.TRANSACTION_PGI  = ect.ElectionTypeCode
		left join	DW_DIM.Dates d on cast(s.TRANSACTION_DT AS DATE)  = D.[Date] and s.TRANSACTION_DT > ''
		left join	DW_DIM.Candidates can on com.CandidateId = can.CandidateId
											and case when com._ElectionYear is not null 
											then com._ElectionYear else can.ElectionYear end = can.ElectionYear
		left join	(select distinct CAND_ID, CAND_ELECTION_YR from {{source('fec','cn')}}) cs on can.CandidateId = cs.CAND_ID
		left join	DW_DIM.ExpensePurposes et on s.PURPOSE = et.ExpensePurpose
		left join	DW_DIM.ExpenseCategories ec on s.CATEGORY = ec.ExpenseCategoryCode