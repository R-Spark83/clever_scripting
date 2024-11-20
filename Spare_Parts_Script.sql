/*
Simple script created to track the sale of spare parts on the ShopCourts eCommerce platform.
Making use of the common table expression to drill into a selection while allowing for 
independent use of results from the selection to create special selections from the range of
selections on both credit and cash purchases.
*/

with cte AS(
SELECT Custid, Country, DeliveryDate, Quantity*ItemUnitPrice as Order_Total, Acctno, Itemno2, Item, Quantity
FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY]
Where ItemNo2 Like '%SP' AND LEFT(Acctno, 3) = '800' AND DeliveryDate >= '2023-02-01'
UNION ALL
SELECT Custid, Country, DeliveryDate, Quantity*ItemUnitPrice as Order_Total, Acctno, Itemno2, Item, Quantity
FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY]
Where ItemNo2 Like '%SP' AND LEFT(Acctno, 3) = '349' AND DeliveryDate >= '2023-02-01'
UNION ALL
SELECT Custid, Country, DeliveryDate, Quantity*ItemUnitPrice as Order_Total, Acctno, Itemno2, Item, Quantity
FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY]
Where ItemNo2 Like '%SP' AND LEFT(Acctno, 3) = '125' AND DeliveryDate >= '2022-10-01')
SELECT c1.Custid, c1.Country, c1.DeliveryDate, c2.price*c2.delqty as Order_Total, c1.Acctno, c2.itemno, c2.ItemDesc1, c2.delqty
From cte as c1 JOIN FT_LINEITEM_EXTRACT as c2 ON c1.Acctno = c2.acctno
Where ItemDesc1 is not NULL and itemno Like '%SP' and delqty > 0
Order by DeliveryDate ASC



SELECT Custid, Country, DeliveryDate, AgreementTotal_USD, Acctno, Itemno2, BranchName
FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY]
Where ItemNo2 Like '%SP' AND LEFT(Acctno, 3) in ('800', '349', '125', '191') AND DeliveryDate >= '2022-08-01';

SELECT Country, ISO, DateAcctOpen, DeliveryDate, FirstName, LastName
FROM [CODS].[dbo].[RPT_CASH_CUSTS_ACCT_SMRY]
Where DateAccOpenMth in ('Oct-23', 'Nov-23', 'Dec-23') AND Left(Acctno,3) IN ('800', '349', '770', '917', '125', '102', '141', '703', '184', '191', '203') AND BranchName Like 'Shop%'

