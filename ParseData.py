import pandas as pd
from pymongo import MongoClient

client = MongoClient()

# Connect with the portnumber and host
client = MongoClient(
    "mongodb://fds-invoices-prd-db:CoQhwInsexWBkcMME2xXDykMRBf8uMy3lzu4Lb5miWA1MVRN3MC5fbnNIuBFKuhj1iEfwn9b6rZSK2dwOQCAqQ%3D%3D@fds-invoices-prd-db.mongo.cosmos.azure.com:10255/test?appName=%40fds-invoices-prd-db%40&maxIdleTimeMS=120000&replicaSet=globaldb&retrywrites=false&ssl=true")
mydatabase = client["Invoice-db"]
data = mydatabase["invoiceLineDetail"]

vendorNbr = []
productType = []
productId = []
priceIdCode = []
classOfTradePrices = []
unitPrice = []
invoiceId = []
invoiceDate = []
invoicedItmDescr = []

for i in data.find({}, {
    '_id': 1,
    'invoicedProducts': 1,
    'vendorNbr': 1,
    'classOfTradePrices': 1,
    'invoiceLineStatus': 1,
    'innerPackQty': 1,
    'invoicedItmDescr': 1
}).limit(500000):
    invoicepro = i.get("invoicedProducts")
    clasTrade = i.get("classOfTradePrices")
    print(i)

    try:
        for product in invoicepro:
            pType = invoicepro.get(product)["productIdTypeCd"]
            if pType == "UP":
                invoiceId.append(i.get("_id").get("invoiceId"))
                vendorNbr.append(i.get("vendorNbr"))
                invoicedItmDescr.append(i.get("invoicedItmDescr"))
                productId.append(invoicepro.get(product)["productId"].strip())

                defaultUnitPrice = 0
                if clasTrade is None:
                    unitPrice.append(defaultUnitPrice)
                else:
                    for cls in clasTrade:
                        priceCode = clasTrade.get(cls)["priceMultCode"]
                        if priceCode == "CSD":
                            try:
                                defaultUnitPrice = round(clasTrade.get(cls)["unitPrice"] / i.get("innerPackQty"), 2)
                                break
                            except:
                                defaultUnitPrice = 0
                    unitPrice.append(defaultUnitPrice)

                invoiceLineStatus = i.get("invoiceLineStatus")
                for invoiceLineStat in invoiceLineStatus:
                    status = invoiceLineStatus.get(invoiceLineStat)["invcStatusTypCd"]
                    if invoiceLineStatus.get(invoiceLineStat)["invcStatusTypCd"] == 8:
                        invoiceDate.append(invoiceLineStatus.get(invoiceLineStat)["invLnStatDate"])

    except:
        pass

print("vendorNbr.lenght", len(vendorNbr))
print("productId.lenght", len(productId))
print("unitPrice.lenght", len(unitPrice))
print("invoiceDate.lenght", len(invoiceDate))
print("invoicedItmDescr.lenght", len(invoiceDate))


data = {
    "productId": productId,
    "productName": invoicedItmDescr,
    "vendorNbr": vendorNbr,
    "unitPrice": unitPrice,
    "invoiceId": invoiceId,
    "invoiceDate": invoiceDate
}

dataframe = pd.DataFrame(data)
dataframe['invoiceId'] = dataframe['invoiceId'].astype(int)
dataframe['vendorNbr'] = dataframe['vendorNbr'].astype(int)

dataframe.to_csv("anamolise.csv", index=False)
