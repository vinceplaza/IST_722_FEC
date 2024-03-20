		SELECT		DISTINCT s.[NAME] PayeeName, 
					s.CITY PayeeCity, 
					s.[STATE] PayeeStateAbbr,
					 substring(s.ZIP_CODE,1,5) PayeeZipCode,
                     z.latitude,
                     z.longitude
		FROM		{{source('fec','oppexp')}} s
        LEFT join  {{source('fec','ZIPCODES')}} z on substring(s.ZIP_CODE,1,5) = z.zipcode