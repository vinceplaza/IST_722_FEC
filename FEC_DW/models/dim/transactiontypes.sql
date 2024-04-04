SELECT		DISTINCT 
            {{ dbt_utils.generate_surrogate_key(['s.TRANSACTION_TP']) }} as TransactionTypeKey,
            tt.TransactionTypeCode,
            tt.TransactionTypeDescription AS TransactionTypeDesc
FROM		{{source('fec','itcont')}} s
INNER JOIN	{{source('fec','transactiontypes')}} tt  on s.TRANSACTION_TP = tt.TransactionTypeCode

