#!/usr/bin/python3
# -*- coding: utf-8 -*-



import urllib.parse
import urllib.request
import json
import os
from datetime import datetime
import time
import sys
import codecs
import re
import lxml.etree as ET 
from lxml import etree
import pycountry


from atom.main import data_manager, g
from atom.helpers.helper import stringOps
so=stringOps()


class eadxml(object):
	#data_directory='data/'
	ns=""
	dm=data_manager.DataManager()
	ead_data = []
	sourcefile=""
	ns=""
	source_str=""
	root={}
	namespaces={}
	tree={}
	tree=object()
	meta={"repository":"","levelOfDescription":"Fonds", "legacyId":"","title":"","parentId":""}
	
	
	def __init__(self, sourcefile, source_str):
		
		self.sourcefile=sourcefile
		self.source_str=source_str	
		if os.path.isfile(self.sourcefile):
			try:
			#while True:
				self.tree = etree.parse(self.sourcefile)

				self.root = self.tree.getroot()
				print("File ", sourcefile, " found ")		
				self.namespaces=dict([node for _,node in ET.iterparse(sourcefile, events=['start-ns'])])
				if "" in self.namespaces:
					self.ns="{"+self.namespaces['']+"}"
				else:
					self.ns=""

			#if True:
			except:
				print(" Couldn't open source File ", self.sourcefile)
				

		
		
		
	"""			
	def __init__2(self, sourcefile, source_str):

		self.sourcefile=sourcefile
		self.source_str=source_str
		if os.path.isfile(self.sourcefile):
			try:
				self.tree = ET.iterparse(sourcefile, events=('end', ))

				self.root = etree.parse(sourcefile)
				print("File ", sourcefile, " found ")
			except:
				print("Couldn't open source File ", self.sourcefile)

			self.namespaces=dict([node for _,node in ET.iterparse(sourcefile, events=['start-ns'])])
			if "" in self.namespaces:
				self.ns="{"+self.namespaces['']+"}"
			else:
				self.ns=""
		else:
			print("Couldn't open source File ")
	
	"""
	#def export (self,counter, from_term):
	def export(self,**kwargs):
		yield self.transform()

		
	"""
	def build_era_facet1(self,era):
		# spliting the era into seperate years
		#obsolete ?
		era_list=era.split('/')

		if len(era_list)==1:
			return self.left(era_list[0],4)
			
		else:
			years=[]
			for x in range(int(self.left(era_list[0],4)),int(self.left(era_list[1],4))+1):
				years.append(str(x))
				#print years
			return '*['+ '##'.join(years) + ']*'
	
	"""

	def left(s, amount):
		return s[:amount]




	"""	

	def export2(self,filepath,source_str):
		# exportes a dict list
		l=self.transform(source_str)
		return l
	"""

	def transform(self):
		for child in self.root:
			#print(child.tag)
			if child.tag==self.ns+"eadheader":
				for head in child:
					if head.tag==self.ns+"eadid":
						self.meta["legacyId"]=head.text
						self.meta['descriptionIdentifier']=head.text
			if child.tag==self.ns+"archdesc":
				for sub_child in child:
					if sub_child.tag==self.ns+"bioghist":
						self.meta['archivalHistory']=self.cleanHtml(str(etree.tostring(sub_child,encoding='unicode')))
					if sub_child.tag==self.ns+"scopecontent":
						self.meta['scopeAndContent']=self.cleanHtml(str(etree.tostring(sub_child,encoding='unicode')))
					if sub_child.tag==self.ns+"prefercite":
						self.meta['prefercite']=sub_child.text
					if sub_child.tag==self.ns+"did":
						for did in sub_child:
							if did.tag==self.ns+'unittitle':
								self.meta['title']=did.text
							if did.tag==self.ns+"unitid":
								if 'call number' in did.attrib:
									self.meta['identifier']=did.text
								elif 'type' in did.attrib:
									if did.attrib['type']=='call number':
										self.meta['identifier']=did.text
								else:
									self.meta['identifier']=did.text
							if did.tag==self.ns+"unitdate":
								self.meta['eventDates']=did.text
								self.meta['eventStartDates']= self.dm.build_eventDates(self.meta['eventDates'])[0]
								self.meta['eventEndDates']=self.dm.build_eventDates(self.meta['eventDates'])[1]
							if did.tag==self.ns+"repository":
								
								for rep in did:
									if rep.tag==self.ns+"corpname":
										
										if rep.tag==self.ns+"corpname":
											self.meta['repository']=rep.text
										if rep.tag==self.ns+"extref":
											sel.meta['findingAids'] = rep.attrib['{http://www.w3.org/1999/xlink}href']
							if did.tag==self.ns+"abstract":
								if 'scopeAndContent' in self.meta:
									self.meta['scopeAnContent']+=did.text
								else:
									self.meta['scopeAnContent']=did.text
							if did.tag==self.ns+'odd':
								for p in did:
									if p.tag=='p':
										self.meta['title']+= " - " + p.text
				
					if sub_child.tag==self.ns+"dsc":
						self.meta['arrangement']= self.meta['title']
						self.get_c(sub_child,None,None,self.meta['legacyId'], self.meta['repository'],self.meta['title'])
						#print(json.dumps(self.ead_data, indent=2))		
		if not('culture' in self.meta) :
			self.meta['culture']=g.CULTURE
		
		self.ead_data.append(self.meta.copy())	
		#self.ead_data=self.ead_data[::-1]
		#print(json.dumps(self.ead_data, indent=2))	
		#self.ead_data[len(self.ead_data)-1].update(self.ead_data[0])
		#self.ead_data.pop(0)x
		"""
		if self.ead_data[1]['parentId']=="":
			self.ead_data[1]['parentId']=self.ead_data[0]['parentId']
			self.ead_data.pop(0)
		"""
		self.ead_data.insert(0, self.ead_data.pop(len(self.ead_data)-1))
		i=0
		empty=None
		for i,e in enumerate(self.ead_data):
			if e['legacyId']==e['parentId']:
				e['parentId']=""
			test_str=""
			for k,v in e.items():
				if k in ['legacyId','repository','levelOfDescription']:
					continue
				test_str+=str(v)
			if test_str.strip(" ")=="":
				empty=i
		if empty  is None:
			pass
		else:
			self.ead_data.pop(empty)

			
			
		#print("-----------------")
		#print(json.dumps(self.ead_data, indent=2))	
		return self.ead_data
	
	
	def get_c(self,elem,levelOfDescription,l_id,p_id,p_arrangement,p_title):
		
		d={"title":"","scopeAndContent":"",'eventActors':''}
		
		if levelOfDescription:
			d['levelOfDescription']=levelOfDescription
		if l_id:
			d['legacyId']=l_id
		else: 
			d['legacyId']=""
		if p_id:
			d['parentId']=p_id
		else:
			d['parentId']=""
			
		for child in elem: 
			
			if child.tag==self.ns+'did':
				
				for did in child:
					if did.tag==self.ns+'unittitle':
						d['title']=did.text
						if p_arrangement=="":
							d['arrangement']=d['title']
						else:
							if d['title']!="":
								d['arrangement']=p_arrangement + " >> "+ p_title
							else:
								d['arrangement']=p_arrangement
						
					if did.tag==self.ns+'unitid':
						
						if 'type' in did.attrib:
							if did.attrib['type']=="call number":
								d['identifier']=did.text
							if did.attrib['type']=='file reference' :
								d['alternativeIdentifiers'] = did.text
								d['alternativeIdentifierLabels']='Former call number'
						else:
							d['identifier']=did.text
					if did.tag==self.ns+'origination':
						if 'eventActors' not in d:
							d['eventActors']=did.text + "|"
						else:
							#d['eventActors']='|'.join(d['eventActors'].split['|'] + [did.text] )
							d['eventActors']+=did.text + "|"
					if did.tag==self.ns+"unitdate":
						d['eventDates']=did.text
						d['eventStartDates']= self.dm.build_eventDates(d['eventDates'])[0]
						d['eventEndDates']=self.dm.build_eventDates(d['eventDates'])[1]
					if did.tag==self.ns+"physdesc":
						for phd in did:
							if phd.tag==self.ns+"genreform":
								d['physicalCharacteristics']=phd.text	
							if phd.tag==self.ns+"extent":
								d["extendAndMedium"]=phd.text
					if did.tag==self.ns+"langmaterial":
						for lmt in did:
							if lmt.tag==self.ns+"language":
								d['language']=self.dm.iso639_3to2(lmt.attrib['langcode'])
								d['culture']=d['language']
								d['languageOfDescription']=d['language']
								d['script']=lmt.attrib['scriptcode']
								d['languageNote']=lmt.text
								
								
					if did.tag==self.ns+"abstract":
						if 'scopeAndContent' in d:
							d['scopeAndContent']+=did.text
						else:
							d['scopeAndContent']=did.text
					if did.tag==self.ns+'odd':
						for p in did:
							if p.tag=='p':
								d['title']+= " - " + p.text

			if child.tag==self.ns+"scopecontent":
				#d['scopeAndContent']=" ".join([x.text for x in child ])
				d['scopeAndContent']+=self.cleanHtml(str(etree.tostring(child,encoding='unicode'))) +" \n"		
			if child.tag==self.ns+"relatedmaterial":
				d['relatedUnitsOfDescription']=self.cleanHtml(str(etree.tostring(child,encoding='unicode')))
				
			if child.tag==self.ns+'otherfindaid':
				success=False
				for p in child:
					if p.tag==self.ns+'p':
						success=True
						for extref in p:
							if extref.tag==self.ns+"extref":
								d['findingAids'] = extref.attrib['{http://www.w3.org/1999/xlink}href']
				if not success:
					for extref in child:
							if extref.tag==self.ns+"extref":
								d['findingAids'] = extref.attrib['{http://www.w3.org/1999/xlink}href']
				
								
			if child.tag==self.ns+"c":

				if 'level' in child.attrib:
					levelOfDescription=child.attrib['level']
				if 'id' in child.attrib:
					l_id=child.attrib['id']
					
				if d['legacyId']=="":
					self.get_c(child,levelOfDescription,l_id,d['parentId'],p_arrangement,d['title'])	
				else:
					self.get_c(child,levelOfDescription,l_id,d['legacyId'],d['arrangement'],d['title'])	
				
		d['repository']=self.meta['repository']
		if not('culture' in d):
			d['culture']=g.CULTURE
		if d['legacyId']!="":
			if not d['title']:
				d['title']=""
			d['eventActors']=d['eventActors'].strip('|')
			self.ead_data.append(d.copy())
			
			
							

	def cleanHtml(self,text):
		linefeeds=["</p>","<lb/>","<br/>","<br />","</adressline>","</head>"]
		text=so.replaceChars(text,linefeeds," \n")
		text=so.stripHtml(text," ")
		text=text.replace("  "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace("  "," ")
		#text=text.replace("\n\n","\n").replace("\n\n","\n").replace("\n\n","\n").replace("\n\n","\n")
		text=so.dedupSpaces(text)
		text=so.replaceChars(text,["\n\n","\n \n"],"\n")
		text=text.strip("\n")
		return text

