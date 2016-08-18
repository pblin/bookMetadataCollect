#!/usr/bin/env python
import requests
import json
import csv
#import codecs
import io

imagesDir = 'c:\externalBooInfo'
bookInfoUrl = 'http://xisbn.worldcat.org/webservices/xid/isbn/'
metadata = '?method=getMetadata&format=json&fl=*'
edition = '?method=getEditions&format=json&fl=*'

def writeHeaders (jsonData, f): 
     csvWriter = csv.writer (f, delimiter='\t')
     headerList = jsonData['list'][0].keys()
     headerList.insert (0, 'requestISBN')
     print headerList
     csvWriter.writerow (headerList)
     
     
def writeData (jsonData, isbn, f):
     csvWriter = csv.writer (f, delimiter='\t')
     for listItems in jsonData['list']:
 	outputList = [isbn];
	for key in listItems.keys():
	   if key != "oclcnum":
	        oValue = listItems[key]
	   	if type(oValue) in (tuple, list):
	             outputList.append(json.dumps(oValue[0]).replace('"','').strip().encode('utf8'))
	        else:
	             outputList.append (json.dumps(oValue).replace('"','').strip().encode('utf8'))
	csvWriter.writerow(outputList)
try:
    f = open('isbnList.txt','r')
    bookInfoMeta = io.open('bookMetaInfo.csv', 'wb+')
    bookInfoEdition = io.open('bookEdition.csv', 'wb+')
except IOError, (ErrorNumber, ErrorMessage):
    print ErrorMessage
        

for isbn in f:
	#metadata 
	#print isbn
	isbnNoLine = isbn.rstrip('\r\n')
	metaRequestUrl = bookInfoUrl + isbnNoLine + metadata;
	print metaRequestUrl
	metaData = requests.get(metaRequestUrl)
	metajsonObj = metaData.json()
	if metajsonObj['stat'] == "ok":
		#print json.dumps(metajsonObj)
		
		writeHeaders (metajsonObj,bookInfoMeta)
		writeData (metajsonObj,isbnNoLine, bookInfoMeta)

	#edition
	editionReqestUrl = bookInfoUrl + isbnNoLine + edition;
	print editionReqestUrl
	editionData = requests.get(editionReqestUrl)
	editionjsonObj = editionData.json()

	if editionjsonObj ['stat'] == "ok":
		
		writeHeaders (editionjsonObj,bookInfoEdition)
		writeData (editionjsonObj,isbnNoLine, bookInfoEdition)

	
	



