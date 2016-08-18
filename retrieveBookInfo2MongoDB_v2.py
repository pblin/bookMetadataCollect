# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 12:50:00 2015

@author: bernardlin
"""

from pymongo import MongoClient
import requests
import time
import json

from lxml import etree as ET
#import xml.etree.ElementTree as ET

import xml2json

#worldcat.org
bookInfoUrl = 'http://xisbn.worldcat.org/webservices/xid/isbn/'
metadata = '?method=getMetadata&format=json&fl=*'
edition = '?method=getEditions&format=json&fl=*'

#Google
API_KEY="AIzaSyBDQfj38nvFlHBKcsUgaoVw7AsUcMjP0dQ"
googleBookApiSearch="https://www.googleapis.com/books/v1/volumes?key="+API_KEY
#client = MongoClient('mongodb://poc.scholastic-labs.io:27017/')
client = MongoClient('mongodb://localhost:27017/')

#Library of Congress
locInfoUrl="http://lx2.loc.gov:210/lcdb?version=1.1&operation=searchRetrieve&maximumRecords=1&query=bath.isbn="

#MongoDB client
db = client['externalBooksDB']
count = 0;

def writeWorldCatData (jsonData, isbn, coll):
     for listItems in jsonData['list']:
      post = { "isbn13": isbn, "data": listItems }
      coll.insert(post)

def getFromWorldCat (isbn, coll):
    metaRequestUrl = bookInfoUrl + isbnNoLine + metadata
    print metaRequestUrl
    metaData = requests.get(metaRequestUrl)
    metajsonObj = metaData.json()
      
    if metajsonObj['stat'] == "ok":
        #print json.dumps(metajsonObj)
        writeWorldCatData (metajsonObj,isbnNoLine, coll)

    #edition
	editionReqestUrl = bookInfoUrl + isbnNoLine + edition;
    print editionReqestUrl
    editionData = requests.get(editionReqestUrl)
    editionjsonObj = editionData.json()

    if editionjsonObj ['stat'] == "ok":
       writeWorldCatData (editionjsonObj,isbnNoLine, coll)

def getFromGoogleBookApi (isbn,coll):
   query = googleBookApiSearch + "&q={'ISBN_13':" + "'" + isbn + "'" + "}"
  
   #limit result to 1
   query += "&maxResults=1"
   print query
   bookSearch=requests.get(query)
   bookSearchJSON=bookSearch.json()
   #print bookSearchJSON
   if bookSearchJSON['totalItems'] >= 1:
       post = {"isbn13": isbn, "data":bookSearchJSON['items'][0] }
       print post
       coll.insert(post)

def getFromLoC (isbn, coll):
    locQuery = locInfoUrl + isbn
    locData = requests.get(locQuery)
    ET.register_namespace("zs", "http://www.loc.gov/zing/srw/")
    #ET.register_namespace ("", "http://www.loc.gov/MARC21/slim")
    root = ET.fromstring(locData.text)

    #print ET.tostring(root)
    numElement = root.find("zs:numberOfRecords", namespaces={"zs":"http://www.loc.gov/zing/srw/"})
    print numElement.text

    if numElement.text != '0':
        elem = root.find(".//ns0:record", namespaces={"ns0":"http://www.loc.gov/MARC21/slim"})
        newDoc = ET.XML(ET.tostring(elem, method="xml"))

        xslt = ET.parse("./MARC21slim2RDFDC.xsl")
        transform = ET.XSLT(xslt)
        dcDom = transform(newDoc)
        xmlstring = ET.tostring(dcDom, method="xml")
        options = optparse.Values({"pretty": False})
        json_string = xml2json.xml2json(xmlstring,options,0)
        print json_string

try:
    f = open('isbnList.txt','r')

except IOError, (ErrorNumber, ErrorMessage):
    print ErrorMessage
        

for isbn in f:
     
     if count > 500:
         count = 0
         print "sleep for a minutes..."
         time.sleep(60)
         
     isbnNoLine = isbn.rstrip('\r\n')
     worldCatColl = db['worldCat']
     #getFromWorldCat (isbnNoLine, worldCatColl)
     googleColl = db['google']
     #getFromGoogleBookApi(isbnNoLine,googleColl)
     locColl = db['loc']
     getFromLoC(isbn, locColl)
     count +=1
 
print "the end..."	