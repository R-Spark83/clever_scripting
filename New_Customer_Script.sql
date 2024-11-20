/*
The aim of this exercise is to gather a market segmentation for use in our 
demographic mapping and market research with special constraints outlined this script
rightfully positions each customer into associated buckets based on the selected product/service 
offering for different campaigns spanning from disappearing customers, repeat customer
or new customer.
*/

/*
Gathering insight from repeat customers who, based on specification have closed prior loan agreements
with the business over 5 years ago and have instated either of our loans account offerings
with an active credit accounts on the records for higher purchase (HP) or cashloan (CL) from 
a table spanning the last 5 years spanning 2020-2024
*/
with cte As(
SELECT 'New Repeat Customer' as Source,
NewRpt.custid, NewRpt.MostRecentAccount, Cust_DB1.Gender as Gender, 
    CAST(Cust_DB1.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB1.Is_Cashloan_Acct as Is_CL, Cust_DB1.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB1.Is_Both as Is_Both, Cust_DB1.Birthdate as Birthdate, Cust_DB1.Occupation, Cust_DB1.ItemCategory, Cust_DB1.DivisionName, Cust_DB1.DepartmentName,
    case  when Cust_DB1.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB1.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB1.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when Cust_DB1.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB1.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group, 
    case 
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226', '208', '141', '896') 
    then 'Courts ReadyCash'
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '881', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814', '573', '583') 
    then 'Courts'
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
    then 'Lucky Dollar'
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714', '708', '416', '779') 
    then 'Courts Optical' 
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781', '511', '512')
    then 'RadioShack'
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
    then 'Ashley'
    when Left(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
    then 'Tropigas' 
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800', '450', '917', '102')
    then 'Ecommerce'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
    then 'Telesales'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
    then 'USA'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('817')
    then 'EMMA'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('412', '311', '411', '413', '415')
    then 'Omni'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('935')
    then 'Courts MicroFinance'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('766')
    then 'Outreach Sales'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('792', '612', '881')
    then 'Courts Bargain Center'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('880', '210', '755')
    then 'Sales Financing'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('705', '716', '723', '724', '727', '728', '729', '730', '726')
    then 'BlueStart Kiosk'
    WHEN LEFT(CAST(Cust_DB1.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    END AS Brand,
    case when LOWER(REPLACE(NewRpt.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewRpt.Country end as Country, 
    case when Cust_DB1.Is_Cashloan_Acct = 'Y' and Cust_DB1.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB1.Is_HPorRF_Acct = 'Y' and Cust_DB1.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB1.[Is_Both] = 'Y' Then 'Both' else 'Cash' end as acct_type
    FROM [CODS].[dbo].[Credit.CreditAccountSummaryFact] as NewRpt Inner Join [CODS].[dbo].[RPT_CL_HP_CUSTS_ACCT_SMRY] as Cust_DB1
    ON (NewRpt.MostRecentAccount = Cust_DB1.MostRecentAcctno)
    WHERE (DATEDIFF(year, NewRpt.DateSettled, Cust_DB1.DateAcctOpen)>= 5)
            AND (NewRpt.HPAccountACTIVE > 0 OR NewRpt.CreditAccountACTIVE > 0)
UNION ALL
/*
Gathering intelligences from customers who have never had any prior loan arrangements with the 
business and have taken up a credit account with the business from a table spanning the last
5 years spanning 2020-2024.
*/
SELECT 'New New Customer' as Source, 
    NewNew.custid, NewNew.MostRecentAccount, Cust_DB2.Gender as Gender,
    CAST(Cust_DB2.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB2.Is_Cashloan_Acct as Is_CL, 
    Cust_DB2.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB2.Is_Both as Is_Both, Cust_DB2.Birthdate as Birthdate, Cust_DB2.Occupation, Cust_DB2.ItemCategory, Cust_DB2.DivisionName, Cust_DB2.DepartmentName,
    case  
    when Cust_DB2.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB2.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB2.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when Cust_DB2.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB2.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group,
    case 
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226', '208', '141', '896') 
    then 'Courts ReadyCash'
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '612', '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814') 
    then 'Courts'
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
    then 'Lucky Dollar'
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714', '708', '416', '779') 
        then 'Courts Optical' 
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781', '512', '511')
    then 'RadioShack'
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
    then 'Ashley'
    when Left(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
    then 'Tropigas' 
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800', '450', '917', '102')
    then 'Ecommerce'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
    then 'Telesales'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
    then 'USA'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('817')
    then 'EMMA'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('412', '311', '411', '413', '415')
    then 'Omni'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('935')
    then 'Courts MicroFinance'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('766')
    then 'Outreach Sales'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('792', '612', '881')
    then 'Courts Bargain Center'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('880', '210', '755')
    then 'Sales Financing'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('705', '716', '723', '724', '727', '728', '729', '730', '726')
    then 'BlueStart Kiosk'
    WHEN LEFT(CAST(Cust_DB2.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    END AS Brand,
    case when LOWER(REPLACE(NewNew.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewNew.Country end as Country, 
    case when Cust_DB2.Is_Cashloan_Acct = 'Y' and Cust_DB2.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB2.Is_HPorRF_Acct = 'Y' and Cust_DB2.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB2.Is_Both = 'Y' Then 'Both' else 'Cash' end as acct_type
    FROM [CODS].[dbo].[Credit.CreditAccountSummaryFact] as NewNew 
          INNER JOIN [CODS].[dbo].[RPT_CL_HP_CUSTS_ACCT_SMRY] as Cust_DB2 
              ON NewNew.MostRecentAccount = Cust_DB2.MostRecentAcctno       
    WHERE NewNew.CreditAccountCOUNT = 1
UNION ALL
/*
Gathering insight from repeat customers who, based on specification have closed prior loan agreements
with the business over 5 years ago and have instated either of our loans account offerings
with an active credit accounts on the records for higher purchase (HP) or cashloan (CL) from 
a table spanning the 5 years spanning 2015-2019
*/
SELECT 'New Repeat Customer' as Source,
NewRpt.custid, NewRpt.MostRecentAccount, Cust_DB_ARCH.Gender as Gender, 
    CAST(Cust_DB_ARCH.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB_ARCH.Is_Cashloan_Acct as Is_CL, Cust_DB_ARCH.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB_ARCH.Is_Both as Is_Both, Cust_DB_ARCH.Birthdate as Birthdate, Cust_DB_ARCH.Occupation, Cust_DB_ARCH.ItemCategory, Cust_DB_ARCH.DivisionName, Cust_DB_ARCH.DepartmentName,
    case  when Cust_DB_ARCH.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB_ARCH.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB_ARCH.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when Cust_DB_ARCH.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB_ARCH.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group, 
    case 
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226') 
    then 'Courts ReadyCash'
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '612', '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '881', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814', '573', '583') 
    then 'Courts'
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
    then 'Lucky Dollar'
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714', '416', '779') 
    then 'Courts Optical' 
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781', '511', '512')
    then 'RadioShack'
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
    then 'Ashley'
    when Left(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
    then 'Tropigas' 
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800', '102')
    then 'Ecommerce'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
    then 'Telesales'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
    then 'USA'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('817')
    then 'EMMA'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('412', '311', '411', '413', '415')
    then 'Omni'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('766')
    then 'Outreach Sales'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('792', '612', '881')
    then 'Courts Bargain Center'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('880', '210', '755')
    then 'Sales Financing'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('705', '716', '723', '724', '727', '728', '729', '730', '726', '708')
    then 'BlueStart Kiosk'
    WHEN LEFT(CAST(Cust_DB_ARCH.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    END AS Brand,
    case when LOWER(REPLACE(NewRpt.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewRpt.Country end as Country, 
    case when Cust_DB_ARCH.Is_Cashloan_Acct = 'Y' and Cust_DB_ARCH.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB_ARCH.Is_HPorRF_Acct = 'Y' and Cust_DB_ARCH.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB_ARCH.[Is_Both] = 'Y' Then 'Both' else 'Cash' end as acct_type
    FROM [XXXX].[xxx].[Xxxxxx.CreditAccountSummaryFact] as NewRpt Inner Join [XXXX].[xxx].[RPT_CL_HP_CUSTS_ACCT_SMRY_2015_19] as Cust_DB_ARCH
    ON (NewRpt.MostRecentAccount = Cust_DB_ARCH.MostRecentAcctno)
    WHERE (DATEDIFF(year, NewRpt.DateSettled, Cust_DB_ARCH.DateAcctOpen)>= 5)
            AND (NewRpt.HPAccountACTIVE > 0 OR NewRpt.CreditAccountACTIVE > 0)
UNION ALL
/*
Gathering insight from repeat customers who, based on specification have closed prior loan agreements
with the business over 5 years ago and have instated either of our loans account offerings
with an active credit accounts on the records for higher purchase (HP) or cashloan (CL) from 
a table spanning the 5 years spanning 2010-2014
*/
SELECT 'New Repeat Customer' as Source,
NewRpt.custid, NewRpt.MostRecentAccount, Cust_DB_ARCH1.Gender as Gender, 
    CAST(Cust_DB_ARCH1.DateAcctOpen AS Date) as Open_Date, 
    Cust_DB_ARCH1.Is_Cashloan_Acct as Is_CL, Cust_DB_ARCH1.Is_HPorRF_Acct as Is_HPorRF, 
    Cust_DB_ARCH1.Is_Both as Is_Both, Cust_DB_ARCH1.Birthdate as Birthdate, Cust_DB_ARCH1.Occupation, Cust_DB_ARCH1.ItemCategory, Cust_DB_ARCH1.DivisionName, Cust_DB_ARCH1.DepartmentName,
    case  when Cust_DB_ARCH1.Birthdate between '1997-1-1' and '2012-12-31' Then 'Gen Z'
    when Cust_DB_ARCH1.Birthdate between '1981-1-1' and '1996-12-31' Then 'Millenial'
    when Cust_DB_ARCH1.Birthdate between '1965-1-1' and '1980-12-31' Then 'Gen X'
    when Cust_DB_ARCH1.Birthdate between '1955-1-1' and '1964-12-31' then 'Boomers 2'
    when Cust_DB_ARCH1.Birthdate between '1946-1-1' and '1954-12-31' then 'Boomers 1'
    else 'Post-War' end as Age_Group, 
    case 
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('108', '112', '116', '144', '146', '147', '717', '718', '721', '186', '852', '191', '754', '756', '757', '199', '200', '873',
    '871', '861', '868', '869', '867', '865', '863', '879', '866', '451', '870', '862', '893', '872', '864', '883', '894', '874', '932', '933', '126', '127', '226') 
    then 'Courts ReadyCash'
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('106', '113', '725', '114', '115', '143', '770', '771', '145', '148', '700', '701', '706', '711',
    '702', '719', '720', '190', '850', '858', '854', '857', '740', '743', '744', '745', '747', '749', '750', '960', '753', '720', '721', '722', '207',
    '612', '551', '553', '554', '557', '558', '560', '561', '562', '563', '565', '566', '617', '569', '571', '584', '588', '590', '815', '807', '812',
    '813', '710', '757', '758', '761', '762', '763', '767', '790', '791', '793', '794', '797', '900', '901', '903', '905', '907', '916', '931', '780', '784', '781', 
    '783', '786', '785', '778', '793', '782', '790', '811', '558', '566', '584', '560', '590', '106', '725', '960', '743', '740', '850', '858', '720', '900',
    '901', '903', '905', '907', '916', '936', '128', '129', '782', '600', '601', '602', '603', '604', '605','606', '607', '608', '609', '618', '630', '640', '650',
    '660', '670', '680', '881', '910', '920', '930', '940', '950', '970', '980', '990', '811', '814', '573', '583') 
    then 'Courts'
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('174', '173', '172', '177', '175', '615', '613', '171', '176', '614', '895', '878', '897', '600', '601', '604'
        , '605', '610', '612', '616', '742', '743', '750', '600', '741', '617') 
    then 'Lucky Dollar'
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('107', '111', '142', '714', '187', '192', '193', '194', '758', '204', '351', '341', '342', '346', '344', '203', '343', '201',
        '347', '350', '202', '345', '352', '351', '700', '706', '712', '703', '714', '717', '701', '711', '713', '707', '709', '704', '715', '771', '772', '773', '774', '775'
        ,'776', '777', '778', '919', '920', '930', '128', '129', '130', '717', '919', '920', '702', '714', '416', '779') 
    then 'Courts Optical' 
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('751', '876', '877', '875', '750', '752', '753', '782', '783', '921', '937', '921', '927', '781', '511', '512')
    then 'RadioShack'
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('922', '892', '805', '731')
    then 'Ashley'
    when Left(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('121', '122', '123', '124')
    then 'Tropigas' 
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) IN ('125', '770', '349', '800', '102')
    then 'Ecommerce'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3) in ('710', '790', '178', '816')
    then 'Telesales'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('179', '183')
    then 'USA'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('817')
    then 'EMMA'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('412', '311', '411', '413', '415')
    then 'Omni'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('766')
    then 'Outreach Sales'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('792', '612', '881')
    then 'Courts Bargain Center'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('880', '210', '755')
    then 'Sales Financing'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('705', '716', '723', '724', '727', '728', '729', '730', '726', '708')
    then 'BlueStart Kiosk'
    WHEN LEFT(CAST(Cust_DB_ARCH1.MostRecentAcctno as varchar(max)), 3)  in ('801')
    then 'Customer Care'
    END AS Brand,
    case when LOWER(REPLACE(NewRpt.Country, ' ', '')) in ('antiguaandbarbuda', 'dominica', 
                                  'grenada', 'saintkitts', 'saintlucia', 
                                  'saintvincentandthegrenadines')
    then 'OECS' else NewRpt.Country end as Country, 
    case when Cust_DB_ARCH1.Is_Cashloan_Acct = 'Y' and Cust_DB_ARCH1.Is_Both = 'N' Then 'CashLoan'
    when Cust_DB_ARCH1.Is_HPorRF_Acct = 'Y' and Cust_DB_ARCH1.Is_Both = 'N' Then 'HP or RF'
    When Cust_DB_ARCH1.[Is_Both] = 'Y' Then 'Both' else 'Cash' end as acct_type
    FROM [XXXX].[xxx].[Xxxxx.CreditAccountSummaryFact] as NewRpt Inner Join [XXXX].[xxx].[RPT_CL_HP_CUSTS_ACCT_SMRY_2010_14] as Cust_DB_ARCH1
    ON (NewRpt.MostRecentAccount = Cust_DB_ARCH1.MostRecentAcctno)
    WHERE (DATEDIFF(year, NewRpt.DateSettled, Cust_DB_ARCH1.DateAcctOpen)>= 5)
            AND (NewRpt.HPAccountACTIVE > 0 OR NewRpt.CreditAccountACTIVE > 0)
)
Select * from cte as c1 Where c1.MostRecentAccount NOT IN (SELECT acctno from [XXXX].[xxx].[IFRSFinActiveWriteOffs]
Where transtypecode = 'BDW' or transtypecode = 'BDU')


/*Separate Code for segmentation of total customers over the 10 year period*/
Select subquery.source, COUNT(subquery.custid) as cust_count, subquery.Fiscal, subquery.Country
From(
    SELECT 'New Cash' as source, NewCash.custid, CAST(NewCash.DateAcctOpen AS Date) as Open_Date, NewCash.Country,
    case when NewCash.DateAcctOpen between '2019-04-01' and '2020-03-31  23:59:59' Then 'FY20'
    when NewCash.DateAcctOpen between '2020-04-01' and '2021-03-31 23:59:59' Then 'FY21'
    when NewCash.DateAcctOpen between '2021-04-01' and '2022-03-31 23:59:59' Then 'FY22'
    when NewCash.DateAcctOpen between '2022-04-01' and '2023-03-31 23:59:59' Then 'FY23'
    when NewCash.DateAcctOpen between '2023-04-01' and '2024-03-31 23:59:59' Then 'FY24'
    when NewCash.DateAcctOpen between '2019-01-02' and '2019-03-31 23:59:59' Then 'FY19'
    end as Fiscal
FROM [XXXX].[xxx].[RPT_CASH_CUSTS_ACCT_SMRY] as NewCash) subquery
Group By subquery.source, subquery.Fiscal, subquery.Country
UNION ALL
SELECT subquery1.source, COUNT(subquery1.CustID) as cust_count, subquery1.Fiscal, subquery1.Country
FROM(
    SELECT 'New Credit' as source, NewCredit.CustID, CAST(NewCredit.DateAcctOpen AS Date) as Open_Date, NewCredit.Country,
case when NewCredit.DateAcctOpen between '2019-04-01' and '2020-03-31 23:59:59' Then 'FY20'
    when NewCredit.DateAcctOpen between '2020-04-01' and '2021-03-31 23:59:59' Then 'FY21'
    when NewCredit.DateAcctOpen between '2021-04-01' and '2022-03-31 23:59:59' Then 'FY22'
    when NewCredit.DateAcctOpen between '2022-04-01' and '2023-03-31 23:59:59' Then 'FY23'
    when NewCredit.DateAcctOpen between '2023-04-01' and '2024-03-31 23:59:59' Then 'FY24'
    when NewCredit.DateAcctOpen between '2019-01-02' and '2019-03-31 23:59:59' Then 'FY19'
    end as Fiscal
FROM [XXXX].[xxx].[RPT_CL_HP_CUSTS_ACCT_SMRY] as NewCredit) subquery1
Group By subquery1.source, subquery1.Fiscal, subquery1.Country
UNION ALL
Select subquery2.source, COUNT(subquery2.custid) as cust_count, subquery2.Fiscal, subquery2.Country
From(
    SELECT 'New Cash' as source, NewCash_ARCH.custid, CAST(NewCash_ARCH.DateAcctOpen AS Date) as Open_Date, NewCash_ARCH.Country,
    case when NewCash_ARCH.DateAcctOpen between '2015-04-01' and '2016-03-31 23:59:59' Then 'FY16'
    when NewCash_ARCH.DateAcctOpen between '2016-04-01' and '2017-03-31 23:59:59' Then 'FY17'
    when NewCash_ARCH.DateAcctOpen between '2017-04-01' and '2018-03-31 23:59:59' Then 'FY18'
    when NewCash_ARCH.DateAcctOpen between '2018-04-01' and '2019-03-31 23:59:59' Then 'FY19'
    when NewCash_ARCH.DateAcctOpen between '2019-04-01' and '2019-12-31 23:59:59' Then 'FY20'
    when NewCash_ARCH.DateAcctOpen between '2015-01-02' and '2015-03-31 23:59:59' Then 'FY15'
    end as Fiscal
FROM [XXXX].[xxx].[RPT_CASH_CUSTS_ACCT_SMRY_2015_19] as NewCash_ARCH) subquery2
Group By subquery2.source, subquery2.Fiscal, subquery2.Country
UNION ALL
SELECT subquery3.source, COUNT(subquery3.CustID) as cust_count, subquery3.Fiscal, subquery3.Country
FROM(
    SELECT 'New Credit' as source, NewCredit_ARCH.CustID, CAST(NewCredit_ARCH.DateAcctOpen AS Date) as Open_Date, NewCredit_ARCH.Country,
case when NewCredit_ARCH.DateAcctOpen between '2015-04-01' and '2016-03-31 23:59:59' Then 'FY16'
    when NewCredit_ARCH.DateAcctOpen between '2016-04-01' and '2017-03-31 23:59:59' Then 'FY17'
    when NewCredit_ARCH.DateAcctOpen between '2017-04-01' and '2018-03-31 23:59:59' Then 'FY18'
    when NewCredit_ARCH.DateAcctOpen between '2018-04-01' and '2019-03-31 23:59:59' Then 'FY19'
    when NewCredit_ARCH.DateAcctOpen between '2019-04-01' and '2019-12-31 23:59:59' Then 'FY20'
    when NewCredit_ARCH.DateAcctOpen between '2015-01-02' and '2015-03-31 23:59:59' Then 'FY15'
    end as Fiscal
FROM [XXXX].[xxx].[RPT_CL_HP_CUSTS_ACCT_SMRY_2015_19] as NewCredit_ARCH) subquery3
Where subquery3.Fiscal is NULL
Group By subquery3.source, subquery3.Fiscal, subquery3.Country
UNION ALL
Select subquery4.source, COUNT(subquery4.custid) as cust_count, subquery4.Fiscal, subquery4.Country
From(
    SELECT 'New Cash' as source, NewCash_ARCH1.custid, CAST(NewCash_ARCH1.DateAcctOpen AS Date) as Open_Date, NewCash_ARCH1.Country,
    case when NewCash_ARCH1.DateAcctOpen between '2010-04-01' and '2011-03-31 23:59:59' Then 'FY11'
    when NewCash_ARCH1.DateAcctOpen between '2011-04-01' and '2012-03-31 23:59:59' Then 'FY12'
    when NewCash_ARCH1.DateAcctOpen between '2012-04-01' and '2013-03-31 23:59:59' Then 'FY13'
    when NewCash_ARCH1.DateAcctOpen between '2013-04-01' and '2014-03-31 23:59:59' Then 'FY14'
    when NewCash_ARCH1.DateAcctOpen between '2014-04-01' and '2014-12-31 23:59:59' Then 'FY15'
    when NewCash_ARCH1.DateAcctOpen between '2010-01-02' and '2010-03-31 23:59:59' Then 'FY10'
    end as Fiscal
FROM [XXXX].[xxx].[RPT_CASH_CUSTS_ACCT_SMRY_2010_14] as NewCash_ARCH1) subquery4
Group By subquery4.source, subquery4.Fiscal, subquery4.Country
UNION ALL
SELECT subquery5.source, COUNT(subquery5.CustID) as cust_count, subquery5.Fiscal, subquery5.Country
FROM(
    SELECT 'New Credit' as source, NewCredit_ARCH1.CustID, CAST(NewCredit_ARCH1.DateAcctOpen AS Date) as Open_Date, NewCredit_ARCH1.Country, 
case when NewCredit_ARCH1.DateAcctOpen between '2010-04-01' and '2011-03-31 23:59:59' Then 'FY11'
    when NewCredit_ARCH1.DateAcctOpen between '2011-04-01' and '2012-03-31 23:59:59' Then 'FY12'
    when NewCredit_ARCH1.DateAcctOpen between '2012-04-01' and '2013-03-31 23:59:59' Then 'FY13'
    when NewCredit_ARCH1.DateAcctOpen between '2013-04-01' and '2014-03-31 23:59:59' Then 'FY14'
    when NewCredit_ARCH1.DateAcctOpen between '2014-04-01' and '2014-12-31 23:59:59' Then 'FY15'
    when NewCredit_ARCH1.DateAcctOpen between '2010-01-02' and '2010-03-31 23:59:59' Then 'FY10'
    end as Fiscal
FROM [XXXX].[xxx].[RPT_CL_HP_CUSTS_ACCT_SMRY_2010_14] as NewCredit_ARCH1) subquery5
Group By subquery5.source, subquery5.Fiscal, subquery5.Country

/*DATEDIFF(year, NewRpt.DateSettled, Cust_DB_ARCH.DateAcctOpen) AS DateDelta,*/