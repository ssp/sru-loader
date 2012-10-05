#!/usr/bin/env python
#coding=utf-8
"""
	Script for downloading search results from an SRU interface.

	2012 Sven-S. Porst, SUB Göttingen <porst@sub.uni-goettingen.de>
"""
import sys
import os
import argparse
import urllib
from lxml import etree as ET
import simplejson


def main ():
	global config

	loadXSLs()

	SRUBaseURL = config.url + '?' \
								+ 'operation=searchRetrieve' \
								+ '&' + 'version=1.1' \
								+ '&' + 'recordPacking=xml' \
								+ '&' + 'recordSchema=' + urllib.quote(config.schema) \
								+ '&' + 'maximumRecords=' + str(config.chunksize) \
								+ '&' + 'query=' + urllib.quote(config.query)


	recordCount = 1
	done = False


	while not done:
		firstRecord = recordCount
		SRUURL = SRUBaseURL + '&' + 'startRecord=' + str(recordCount)
		print SRUURL
		SRUResponse = urllib.urlopen(SRUURL).read()

		XML = ET.fromstring(SRUResponse)
		records = XML.findall('.//{http://www.loc.gov/zing/srw/}recordData/*')

		collectedRecords = {}
		for record in records:
			ID = recordID(record, recordCount)

			""" Transform record. """
			for XSL in config.XSLs:
				record = XSL(record).getroot()

			if record is None:
				print u"Record transformation failed for ID »" + ID + u"«"
				print ET.tostring(record)
			else:
				storeRecordWithID(record, ID, collectedRecords)

			recordCount += 1

		storeBatches(collectedRecords, firstRecord)

		done = (len(records) == 0)



""" Store record. """
def storeRecordWithID (record, ID, collectedRecords):
	global config

	sys.stdout.write("ID: " + str(ID) + u"… ")

	collectedRecords[ID] = record

	""" Write XML file for record. """
	if 'xml' in config.format:
		filePath = pathForID(ID, 'xml')
		XMLFile = open(filePath, 'w')
		XMLFile.write(ET.tostring(record))
		XMLFile.close()
		sys.stdout.write(' ./' + filePath)

	""" Convert to JSON and write file. """
	if 'json' in config.format:
		JSONInternal = elem_to_internal(record, strip=1)
		if len(JSONInternal) == 1:
			JSONInternal = JSONInternal.values()[0]
		JSONInternal['_id'] = ID
		filePath = pathForID(ID, 'json')
		JSONFile = open (filePath, "w")
		JSONFile.write(simplejson.dumps(JSONInternal))
		JSONFile.close()
		sys.stdout.write(' ./' + filePath)

	""" If no format is given, print the record. """
	if len(config.format) == 0:
		print ET.tostring(record)

	print ""




""" Create path for record ID. """
def pathForID (ID, format):
	global config
	folderName = format

	for level in range(config.folderdepth):
		if len(ID) >= level * 2:
			firstChar = - (level + 1) * 2
			lastChar = - level * 2
			if lastChar == 0:
				lastChar = None
			subfolderName = ID[firstChar:lastChar]
			folderName += '/' + subfolderName

	if not os.path.exists(folderName):
		os.makedirs(folderName)

	path = folderName + '/' + ID + '.' + format

	return path




""" Store batches of records. """
def storeBatches (collectedRecords, firstRecord):
	global config

	if len(collectedRecords) > 0:
		if 'xml-batch' in config.format:
			XMLContainer = ET.XML('<records/>')
			for (ID, record) in collectedRecords.iteritems():
				XMLContainer.append(record)
			filePath = pathForBatch(firstRecord, 'xml')
			XMLFile = open(filePath, 'w')
			XMLFile.write(ET.tostring(XMLContainer))
			XMLFile.close()
			print u"XML-Batch: " + str(len(collectedRecords)) + u" records to »" + filePath + u"«"


		if 'json-batch' in config.format or 'couchdb-batch' in config.format:
			JSONContainer = []
			for (ID, record) in collectedRecords.iteritems():
				JSONInternal = elem_to_internal(record, strip=1)
				if len(JSONInternal) == 1:
					JSONInternal = JSONInternal.values()[0]
				JSONInternal['_id'] = ID
				JSONContainer += [JSONInternal]

			if 'json-batch' in config.format:
				filePath = pathForBatch(firstRecord, 'json')
				JSONFile = open (filePath, "w")
				JSONFile.write(simplejson.dumps(JSONContainer))
				JSONFile.close()
				print u"JSON-Batch: " + str(len(collectedRecords)) + u" records to »" + filePath + u"«"

			if 'couchdb-batch' in config.format:
				filePath = pathForBatch(firstRecord, 'couch.json')
				JSONContainer = {'docs': JSONContainer}
				JSONFile = open (filePath, "w")
				JSONFile.write(simplejson.dumps(JSONContainer))
				JSONFile.close()
				print u"CouchDB JSON-Batch: " + str(len(collectedRecords)) + u" records to »" + filePath + u"«"
def pathForBatch (firstRecord, format):
	folderName = format + '-batch'
	if not os.path.exists(folderName):
		os.makedirs(folderName)
	path = folderName + '/' + ('%05d' % firstRecord) + '.' + format
	return path




""" Determine the record’s ID use the record count if we cannot find one. """
def recordID (record, recordCount):
	global config

	ID = recordCount
	for IDPath in config.idxpath:
		IDs = record.findall(IDPath)
		if len(IDs) > 0:
			ID = IDs[0].text
			break

	return ID



""" Parse arguments from the command line. """
def parseArguments ():
	parser = argparse.ArgumentParser(description='Download SRU results.')
	parser.add_argument('--url', help='URL of SRU interface', required=True)
	parser.add_argument('--chunksize', help='number of records per request', default=100, type=int)
	parser.add_argument('--schema', help='SRU record schema to request', required=True)
	outputformats = ['xml', 'xml-batch', 'json', 'json-batch', 'couchdb-batch']
	parser.add_argument('--format', help='output format', choices=outputformats, action='append', default=[])
	parser.add_argument('--xsl', help='path to XSL file applied to each record', action='append', default=[])
	parser.add_argument('--folderdepth', help='hierarchical folder levels to use for single record storage', default=1, type=int)
	defaultIDPaths = ['./{http://www.loc.gov/MARC21/slim}controlfield[@tag="001"]', \
						'./controlfield[@tag="001"]', \
						'./{http://www.indexdata.com/turbomarc}c001', \
						'./{info:srw/schema/5/picaXML-v1.0}datafield[@tag="003@"]/{info:srw/schema/5/picaXML-v1.0}subfield[@code="0"]']
	parser.add_argument('--idxpath', help='XPath to get the record ID from, for use as the file name, the record number is used if it is blank', action='append', default=defaultIDPaths)
	parser.add_argument('query', help='CQL query')

	return parser.parse_args()



""" Load stylesheets and place them into config. """
def loadXSLs ():
	config.XSLs = []

	for XSLPath in config.xsl:
		try:
			xslXML = ET.parse(XSLPath)
			XSL = ET.XSLT(xslXML)
			config.XSLs += [XSL]
		except:
			sys.stderr.write(u"Could not read XSLT at »" + XSLPath + u"«, ignoring it.\n")



"""
	stolen from xml2json
	https://github.com/mutaku/xml2json
	added simplistic stripping of namespace from the tag names
"""
def elem_to_internal(elem, strip=1):

	"""Convert an Element into an internal dictionary (not JSON!)."""

	d = {}
	for key, value in elem.attrib.items():
		d['@'+key] = value

	# loop over subelements to merge them
	for subelem in elem:
		v = elem_to_internal(subelem, strip=strip)
		tag = subelem.tag
		tagWithoutNamespace = tag .rpartition('}')[2]
		value = v[tag]
		try:
			# add to existing list for this tag
			d[tagWithoutNamespace].append(value)
		except AttributeError:
			# turn existing entry into a list
			d[tagWithoutNamespace] = [d[tagWithoutNamespace], value]
		except KeyError:
			# add a new non-list entry
			d[tagWithoutNamespace] = value
	text = elem.text
	tail = elem.tail
	if strip:
		# ignore leading and trailing whitespace
		if text: text = text.strip()
		if tail: tail = tail.strip()

	if tail:
		d['#tail'] = tail

	if d:
		# use #text element if other attributes exist
		if text: d["#text"] = text
	else:
		# text is the value if no attributes
		d = text or None
	return {elem.tag: d}






config = parseArguments()
main()
