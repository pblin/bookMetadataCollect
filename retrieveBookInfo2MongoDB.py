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
#import xml.etree.ElementTree as eET
import xml2json
import optparse
import sys
import threading
from StringIO import StringIO
import string

#worldcat.org
bookInfoUrl = 'http://xisbn.worldcat.org/webservices/xid/isbn/'

#Google
#API_KEY="AIzaSyBDQfj38nvFlHBKcsUgaoVw7AsUcMjP0dQ"
google_API_KEY="AIzaSyD7SATp9HAWqo8g1iUuOcBpivtPrRish34"
googleBookApiSearch="https://www.googleapis.com/books/v1/volumes?key="+google_API_KEY

#client = MongoClient('mongodb://poc.scholastic-labs.io:27017/')
client = MongoClient('mongodb://localhost:27017/')

#Library of Congress
locInfoUrl="http://lx2.loc.gov:210/lcdb?version=1.1&operation=searchRetrieve&maximumRecords=1&query=bath.isbn="

#Goodreads (Amazon)
goodread_API_KEY="tWVyinRKhmfoLQVTi8Mtw"
goodreadsUrl="https://www.goodreads.com/book/isbn?key="+goodread_API_KEY+"&format=xml&isbn="

#Ingram
ingramUserID="sCH0last1C_test"
ingramUrl="https://idswebtest.ingramcontent.com/cws/contentws.aspx?UserName="+ingramUserID
ingramUrl+="&queryType=5&startRecord=1&endRecord=25&sortField=DE&liveUpdate=N&dataRequest=BIB,BIO,AWD,RQ,IMG&isbn="

#MongoDB client
db = client['externalBooksDB']
count = 0;

#Writing worldCat data to MongoDB collection
def writeWorldCatData(jsonData,isbn,coll):
     for listItems in jsonData['list']:
      post = { "isbn13": isbn, "data": listItems }
      coll.insert(post)

#Retrieve data from WorldCat
def getFromWorldCat(isbn,coll,r):
    metaRequestUrl=bookInfoUrl+isbn+'?method=getMetadata&format=json&fl=*'
    print metaRequestUrl
    metaData = requests.get(metaRequestUrl)
    if metaData.status_code == requests.codes.ok:
        metajsonObj = metaData.json()

        if metajsonObj['stat'] == "ok":
            #print json.dumps(metajsonObj)
            r.append("worldCatMeta")
            writeWorldCatData (metajsonObj,isbn,coll)
    else:
        print "worldcat_meta:" + " HTTP ERROR:" + str(metaData.status_code)

    #edition
    editionReqestUrl=bookInfoUrl+isbn +'?method=getEditions&format=json&fl=*'
    print editionReqestUrl
    editionData = requests.get(editionReqestUrl)
    if editionData.status_code == requests.codes.ok:
        editionjsonObj = editionData.json()

        if editionjsonObj ['stat'] == "ok":
           r.append("worldCatEdition")
           writeWorldCatData (editionjsonObj,isbn,coll)
    else:
        print "wordcat_edition" + " HTTP ERROR:" + str(editionData.status_code)

#Retrive data from Google Book API
def getFromGoogleBookApi(isbn,coll,r):
   query = googleBookApiSearch + "&q={'ISBN_13':" + "'" + isbn + "'" + "}"
  
   #limit result to 1
   query += "&maxResults=1"
   print query
   bookSearch=requests.get(query)
   if bookSearch.status_code == requests.codes.ok:
       bookSearchJSON=bookSearch.json()
       #print bookSearchJSON
       if bookSearchJSON['totalItems'] >= 1:
           post = {"isbn13": isbn, "data":bookSearchJSON['items'][0] }
           r.append("google")
           #print post
           coll.insert(post)
   else:
       print "google" + " HTTP ERROR:" + str(bookSearch.status_code)

#Retrieve data from Library of Congress
def getFromLoC(isbn,coll,r):
    locQuery = locInfoUrl + isbn

    try:
       locData = requests.get(locQuery)
    except requests.exceptions.RequestException as e:
        print e
        return

    if locData.status_code == requests.codes.ok:
        ET.register_namespace("zs","http://www.loc.gov/zing/srw/")
        ET.register_namespace("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        ET.register_namespace("dc","http://purl.org/dc/elements/1.1/")
        print locData.content
        root = ET.fromstring(locData.content)
        #print ET.tostring(root)
        print locQuery
        diagElem = root.find(".//diag:message", namespaces={"diag":"http://www.loc.gov/zing/srw/diagnostic/"})
        if diagElem is not None:
            print diagElem.text
            return

        numElement = root.find(".//zs:numberOfRecords", namespaces={"zs":"http://www.loc.gov/zing/srw/"})
        print numElement.text

        if numElement is not None and numElement.text != '0':
            elem = root.find(".//ns0:record", namespaces={"ns0":"http://www.loc.gov/MARC21/slim"})
            newDoc = ET.XML(ET.tostring(elem,method="xml"))

            xslt = ET.parse("./MARC21slim2RDFDC.xsl")
            transform = ET.XSLT(xslt)
            dcDom = transform(newDoc)

            xmlstring = ET.tostring(dcDom, method="xml", pretty_print="false")
            #print xmlstring
            """ Covert to json """
            options = optparse.Values({"pretty":False })
            json_string = xml2json.xml2json(xmlstring,options,1)
            #print json_string

            post = {"isbn13":isbn, "data":json_string, "xmlData":xmlstring}
            coll.insert(post)
            r.append("loc")
    else:
        print "loc" + " HTTP ERROR:" + str(locData.status_code)

#Retrieve data from Goodreads (Amazon)
def getFromGoodreads(isbn,coll,r):
    reqUrl = goodreadsUrl + isbn
    print reqUrl
    bookData = requests.get(reqUrl)
    if bookData.status_code == requests.codes.ok:
        if "not found" not in bookData.text:
            #print bookData.text
            parser = ET.XMLParser(strip_cdata=True,recover=True)
            #print bookData.content
            root = ET.XML(bookData.content,parser)
            elem = root.find("book")
            bookDoc = ET.XML(ET.tostring(elem,method="xml"))
            xmlstring = ET.tostring(bookDoc,method="xml",pretty_print="false")
            options = optparse.Values({"pretty": False})
            json_string = xml2json.xml2json(xmlstring,options,0)
            #print json_string
            post = {"isbn13": isbn, "data":json_string, "xmlData":xmlstring}
            r.append("goodreads")
            coll.insert(post)
    else:
        print "goodreads" + " HTTP ERROR:" + str(bookData.status_code)

#Retrieve data from Ingram WebServices
def getFromIngram(isbn,coll,r):
    reqUrl=ingramUrl+isbn
    print reqUrl
    bookData = requests.get(reqUrl)
    if bookData.status_code == requests.codes.ok:
        #print bookData.content
        root = ET.fromstring(bookData.content)
        elem = root.find(".//MatchingRecs")
        #print elem.text
        if elem is not None and elem.text != '0':
            elem = root.find("Book")
            print elem.text
            bookDoc = ET.XML(ET.tostring(elem,method="xml"))
            xmlstring = ET.tostring(bookDoc,method="xml",pretty_print="false")
            options = optparse.Values({"pretty": False})
            json_string = xml2json.xml2json(xmlstring,options,0)
            #print json_string
            post = {"isbn13": isbn, "data":json_string, "xmlData":xmlstring}
            r.append("ingram")
            coll.insert(post)
    else:
        print "ingram" + " HTTP ERROR:" + str(bookData.status_code)

try:
    isbnFile = open('isbnList.txt','r')

except IOError, (ErrorNumber, ErrorMessage):
    print ErrorMessage
        
isbnStatus = db['isbnStatus']
iterations = 1

worldCatColl = db['worldCat']
googleColl = db['google']
goodreadsColl = db['goodreads']
ingramColl=db['ingram']
locColl = db['loc']

for isbn in isbnFile:

     #strip off line
     isbnIn = isbn.rstrip('\r\n')

     #strip off non printable characters

     isbnClean = filter(lambda x: x in string.printable, isbnIn)
     rec = {"isbn13":isbnClean}

     #print rec
     found = isbnStatus.find_one(rec)
     
     #skip isbn's that are loaded 
     if found :
         continue

     print isbnClean
     #exceed daily limit
     if iterations > 1000:
         print "rate limit reached."
         break

     #sleep 30 seconds for every 20 requests (throttle down)
     if count > 20:
         count = 0
         print "sleep for 30 seconds..."
         time.sleep(30)

     sources = []
     threadList = []

     t = threading.Thread(target=getFromWorldCat, args=(isbnClean,worldCatColl,sources))
     threadList.append(t)

     t = threading.Thread(target=getFromGoogleBookApi, args=(isbnClean,googleColl,sources))
     threadList.append(t)


     t = threading.Thread(target=getFromGoodreads, args=(isbnClean,goodreadsColl,sources))
     threadList.append(t)

     t = threading.Thread(target=getFromIngram, args=(isbnClean,ingramColl,sources))
     threadList.append(t)

     t = threading.Thread(target=getFromLoC, args=(isbnClean,locColl,sources))
     threadList.append(t)


     #request threads
     for t in threadList:
         t.start()

     #join all threads
     for t in threadList:
         t.join()


     count +=1
     iterations += 1

     #log request
     rec['sources'] = sources
     isbnStatus.insert(rec)

#ensure MongoDB data collections
worldCatColl.ensure_index("isbn13")
locColl.ensure_index("isbn13")
googleColl.ensure_index("isbn13")
goodreadsColl.ensure_index("isbn13")
goodreadsColl.ensure_index("ingram")

isbnFile.close()

print "end..."
