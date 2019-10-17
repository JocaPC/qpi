USE WideWorldImporters;
GO

EXEC('SELECT SupplierID, DeliveryMethodID, ContactPersonID
FROM Purchasing.PurchaseOrders po
		JOIN Purchasing.PurchaseOrderLines pol
			ON po.PurchaseOrderID = pol.PurchaseOrderID
WHERE SupplierID = 2
AND DeliveryMethodID = 9 
AND ContactPersonID = 2')
go 10