import pandas as pd
import matplotlib.pyplot as plt
import requests
import urllib3
from bs4 import BeautifulSoup
from xml.etree.ElementTree import fromstring, ElementTree
import numpy as np
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
df = pd.read_csv("../anamolise.csv")
grouped = df.groupby(['productId'])

vendorMap = {}

def get_vendor_name(vendorNbr):
    vendorNameList = []
    for vendorNumber in vendorNbr.tolist():
        if (vendorNumber in vendorMap):
            vendorNameList.append(vendorMap[vendorNumber])
        else:
            url = "https://api.wal-mart.com/si/tesmdm/mdmsupplier/v1/suppliers/US/{0}?client_id=56bec9a9-7afe-49c9-9020-6bc167559576"
            url_format = url.format(str(vendorNumber)[:-3])
            vendor_response = requests.request("GET", url_format, verify=False)
            soup = BeautifulSoup(vendor_response.content, "lxml-xml")
            vendorName = parse_xml(str(soup))
            vendorMap[vendorNumber] = vendorName
            vendorNameList.append(vendorName)
    return vendorNameList


def parse_xml(xml_string):
    xtree = ElementTree(fromstring(xml_string))
    for node in xtree.iter(tag=
                           '{http://www.xmlns.walmartstores.com/SupplyChain/SourcingManagement/datatypes/GetSupplierInfo/1.0/}supplier'):
        for supplierName in node.iter(tag=
                                      "{http://www.xmlns.walmartstores.com/SupplyChain/SourcingManagement/datatypes/GetSupplierInfo/1.0/}legalName"):
            for name in supplierName.iter(tag=
                                          "{http://www.xmlns.walmartstores.com/SupplyChain/SourcingManagement/datatypes/SupplierDetails/1.3/}textValue"):
                return name.text


for productId, data in grouped:
    if len(data.vendorNbr.unique().tolist()) > 1 and len(data.unitPrice.unique().tolist()) > 1:
        processed_df = data.sort_values(by='unitPrice').drop_duplicates(['vendorNbr', 'unitPrice']).drop(['productId'], axis=1)

        minPrice = min(processed_df['unitPrice'])
        maxPrice = max(processed_df['unitPrice'])
        if minPrice > 10 and maxPrice - minPrice > 1:
            productId = f'{productId:013d}'

            # get product name
            print("-" * 100)
            graphTitle = "ProductId: " + productId

            if processed_df['productName'].any():
                productName = re.sub(' +', ' ', processed_df['productName'].tolist()[0])
                graphTitle = graphTitle + ", ProductName: " + productName

            # get vendor name
            print("graphTitle:: ", graphTitle)
            processed_df['negotiationMargin'] = (processed_df['unitPrice'] - minPrice)
            processed_df['minPrice'] = minPrice
            processed_df['negotiation %'] = round(
                ((processed_df['unitPrice'] - minPrice) / processed_df['unitPrice']) * 100, 2)
            processed_df['vendorName'] = get_vendor_name(processed_df['vendorNbr'])
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                print(processed_df)

            def addlabels(x, y, s):
                for i in range(len(x)):
                    plt.text(i, y[i], str(s[i]) + "%", ha='center', va='top')

            processed_df.plot(x='vendorNbr', y=['minPrice', 'negotiationMargin'], kind='bar', stacked=True, width = .4, figsize=(10,10))
            addlabels(processed_df['vendorName'].tolist(), processed_df['minPrice'].tolist(), processed_df['negotiation %'].tolist())
            plt.title(graphTitle)
            plt.xticks(rotation=0)
            plt.legend(loc='upper left')
            plt.savefig("Product: "+productId)
            plt.show()
