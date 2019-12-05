#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  location.py
#  
#  Copyright 2018 FH Potsdam FB Informationswissenschaften PR Kolonialismus <kol@fhp-kol-1>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  
import os
import urllib
import json
import sys
import time
import re
import pprint
import xmltodict
from lxml import html
from atom.main import data_manager, g
from atom.config import api_keys
from atom.helpers.helper import fileOps, listOps, stringOps, osOps, funcOps
fo = fileOps()

class Kalliope(object):
	
	base_url="http://kalliope-verbund.info/sru"
	dm=data_manager.DataManager()
	BLOCKED_FILE="../data/blocked.txt"       # list of items which should not be retrieved
	BLOCKED=[]
	AUTHORITIES=[]
	AUTHORITIES_FILE="../data/authorities.json"
	
	
	IGNORE_INSTITUTIONS=[]
	
	
	def __init__(self):
		pass
		
	def __del__(self):
		pass


	def _open_blocked(self):
		if os.path.isfile(self.BLOCKED_FILE) :
			self.BLOCKED=[]
			with open(self.BLOCKED_FILE, 'r') as file:
				self.BLOCKED = file.read().split('\n')
			file.close()
			
	def _store_blocked(self):
		with open(self.BLOCKED_FILE, 'w') as file:
			file.write("\n".join(self.BLOCKED))
		file.close()
		
		
		


	def export(self, **kwargs):
			"""
			retrieves data from the German www.deutsche-digitale-bibliothek.de.
			Sends them back to DataManager in batches of {counter} size
			"""
			counter=kwargs['counter']
			from_term=kwargs['from_term']
			export_item_family=[]
			export_list=[]
			self.open_authorities()
			self._open_blocked()
			print(kwargs)
			for search_term in 	self.dm.search_term_generator(**kwargs):
				print(search_term)
				#self._open_blocked()
				for match in self._search_generator(search_term[0]):
					print(len(export_item_family), len(export_list),"<<<<<<<<<<<<<<<< LEN\n")
					print(len(self.dm.LEGACY_IDS),"<<<<<<<<<<<<<<<< LEGACY_IDS\n")
					##pprint.pprint(match)
					d=self._get_content(match)
					
					if d:
						#print(self.dm.LEGACY_IDS)
						if self.dm.predict_item(d['title']+" " + d['scopeAndContent'] + " "+  d['nameAccessPoints']+ " " + d['arrangement']):
							if d['legacyId'] not in self.dm.LEGACY_IDS and d['legacyId'] not in self.BLOCKED and not self.dm.is_in_atom(d['legacyId']):
								self.dm._add_to_legacy_ids(d['legacyId'])
								export_item_family.append(d)
								print(len(export_item_family),"länge")
								if d['parentId']!="":
									for parent in self._parent_generator(d['parentId']):
										#print("we have a parent")
										export_item_family.append(parent)
							else:
								print(d['legacyId'], " skipped")
						else:
							print ("prediction failed")

					
					if len(export_item_family)>0:
						export_item_family=[x for x in export_item_family if x is not None]
						#pprint.pprint(export_item_family)
						#export_item_family.sort(key=operator.itemgetter('apd_level_of_description'))					
						export_list.extend(export_item_family)
						export_item_family=[]			

				
				if len(export_list)>counter:
						self.dm.store_out_files()
						self.store_authorities()
						self._store_blocked()
						print("Len of export list: ", len(export_list))
						yield export_list.copy()
						export_list=[]
			yield export_list.copy()
			
	def _parent_generator(self,parentId):
		r=self.get(parentId,"id")
		#print(r)
		if r[1] not in ["0",""]:
			#pprint.pprint(r)
			if isinstance(r[0],list):   #some records of Kalliope are duplicates
				match=r[0][0]['srw:recordData']['mods']
			else:
				match=r[0]['srw:recordData']['mods']
			print("PARENT")
			#pprint.pprint(match)
			#print('-----')
			d=self._get_content(match)
			if d:
				if d['legacyId'] not in self.dm.LEGACY_IDS and d['legacyId'] not in self.BLOCKED and not self.dm.is_in_atom(d['legacyId']):
					self.dm._add_to_legacy_ids(d['legacyId'])
					yield(d)
					if d['parentId']!="":
						yield from self._parent_generator(d['parentId'])
				else:
					print("Parent already registred")
			
	
	def _get_content(self,match):
		d={}
		pprint.pprint(match)
		#match=match['srw:recordData']['mods']
		for fieldname in g.ARCHIVAL_DESCRIPTIONS_FIELDS:
			d[fieldname]=""
		d['culture']=g.CULTURE
		d['levelOfDescription']="Gliederung"
		
		d['legacyId']=match['identifier']['#text'][match['identifier']['#text'].rfind("/")+1:]
		d['descriptionIdentifier']=d['legacyId']
		if 'relatedItem' in match:
			d['parentId']=match['relatedItem']['identifier'][0]['#text']
			d['parentId']=d['parentId'][d['parentId'].rfind("/")+1:]
			d['arrangement']=match['relatedItem']['titleInfo']['title']
		d['title']=match['titleInfo']['title']
		if isinstance(match['typeOfResource'],dict):
			d['physicalCharacteristics']=match['typeOfResource']['#text']
		else:
			d['physicalCharacteristics']=match['typeOfResource']
		if '@manuscript' in match['typeOfResource']:
			if match['typeOfResource']['@manuscript']=="yes":
				d['physicalCharacteristics']+="|Manuscript"
				d['levelOfDescription']="Objekt"
		if '@collection' in match['typeOfResource']:
			if match['typeOfResource']['@collection']=="yes":
				d['physicalCharacteristics']+="|Collection"
				d['levelOfDescription']="Sammlung"
		if 	'languageOfCataloging' in match['recordInfo']:
			if isinstance(match['recordInfo']['languageOfCataloging'],dict):
				language= match['recordInfo']['languageOfCataloging']['languageTerm']
			else:
				language= match['recordInfo']['languageOfCataloging']
			for lang_term in language:
				if '@type' in lang_term:
					d['languageNote']=lang_term['#text']
				if	'@authority' in lang_term:
					if lang_term['#text'] in g.LANGUAGES:
						d['language']=g.LANGUAGES[lang_term['#text']]
						d['culture']=d['language']
		if 'abstract' in match:
			if isinstance(match['abstract'],str):
				d['scopeAndContent']=match['abstract']
			else:
				d['scopeAndContent']=match['abstract']['#text']
		if 'physicalDescription' in match:
			d['extentAndmedium']=match['physicalDescription']['extent']
		if 'originInfo' in match:
			if 'place' in match['originInfo']:
				d['placeAccessPoints']=match['originInfo']['place']['placeTerm']['#text']
			if 'dateCreated' in match['originInfo']:
				if match['originInfo']['dateCreated']:
					if isinstance(match['originInfo']['dateCreated'],dict):
						d['eventDates']=match['originInfo']['dateCreated']['#text']
					else:
						d['eventDates']=match['originInfo']['dateCreated']
					(d['eventStartDates'],d['eventEndDates'])=self.dm.build_eventDates(d['eventDates'])
					if not self.dm._is_in_time(d['eventStartDates'],d['eventEndDates']):
						self._add_blocked(d['legacyId'])
						print("added to blocked")
						return False
		
		if 'recordContentSource' in match['recordInfo']:
			if isinstance(match['recordInfo']['recordContentSource'],dict):
				d['repository']=match['recordInfo']['recordContentSource']['#text']
				
			else:
				d['repository']=match['recordInfo']['recordContentSource']
			d['repository']=re.sub(r'<|>','',d['repository'])				
		else:
			print("record don't have a recordContentSource")
		if 'location' in match:
			if isinstance(match['location'],list):
				loc_list=match['location']
			else:
				loc_list=[match['location']]
			#print(loc_list)
			for loc in loc_list:
				#print(loc)
				if 'shelfLocator' in loc:
					#print(loc)
					d['physicalObjectLocation']=loc['shelfLocator']
				if 'url' in loc:
					if '#text' in loc['url']:
						d['findingAids']=loc['url']['#text']
					else:
						d['findingAids']=loc['url']
		else:
			print("record don't have a location tag?")
				
		if 'name' in match:
			if isinstance (match['name'],list):
				name_list=match['name']
			else:
				name_list=[match['name']]
			#print("we have names")
			name_arr=[]
			for n in name_list:
				name_arr.append(self.name_clean(n['namePart']))
				self._add_authority(n)
			print(name_arr)
			if name_arr:
				d['nameAccessPoints']="|".join(name_arr)
			
		
		#print(d,"here is d")
		return d
				
	
	def name_clean(self,name_str):
		if isinstance(name_str,str):
			name_str=re.sub(r'<|>','',name_str)	
			name_str=re.sub(r'\([^\)]*\)','',name_str).strip(" ")
			arr=name_str.split(",")
			if len(arr)==2:
				name_str=arr[1]+ " " + arr[0]
			return name_str
		else:
			return ""


	def _add_blocked(self,legacyId):
		if legacyId not in self.BLOCKED:
			self.BLOCKED.append(legacyId)

	def _add_authority(self, d):
		#print("adding to autorithy",d)
		if '@valueURI' in d:
			d['value']=d['@valueURI']
		elif 'id' in d:
			d['value']=d['id']
		else:
			d['value']=""
		#print(d)
		if not next((x for x in self.AUTHORITIES if x['value']==d['value']),False):
			self.AUTHORITIES.append(d)

		#print(self.AUTHORITIES,"???")
		return
		
	def store_authorities(self):
		with open(self.AUTHORITIES_FILE, 'w') as file:
			file.write(json.dumps(self.AUTHORITIES, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		
		
	def open_authorities(self):
		if len(self.AUTHORITIES) ==0:
			if os.path.isfile(self.AUTHORITIES_FILE):
				with open(self.AUTHORITIES_FILE, 'r') as file:
					self.AUTHORITIES = json.load(file)
				file.close()
		

	def _search_generator(self,search_term):
		"""
		lookup for search results in DDB using the search terms from DataManager

		Parameter:
		search_term : a tupel ({search_term String},{search_term WD_Instance})
		"""
		search_log=""
		print("entrering search_generator with ", search_term)
		try:
		#

			(r,number_of_results)=self.get(search_term)
			log=str(number_of_results)+"\t"+search_term+"\tKAL\t"+time.strftime("%Y-%m-%d %H:%M:%S")+"\n"+search_log
			print (log)#
			self.dm.SEARCH_LOG=log+self.dm.SEARCH_LOG
			#print(r)
			for e in r:
				yield e['srw:recordData']['mods']
			
		except Exception as e: 
		
			#self.store_id_list()
			print("Error:", sys.exc_info()[0])
			print(e)	

	def get(self, search_term, method="fulltext"):
		try:
			#print(search_term)
			data={}
			data['query']='"'+search_term+'"'
			data['version']="1.2"
			data['recordSchema']='mods'
			if method== "fulltext":
				data['operation']="searchRetrieve"
			if method=="id":
				data['operation']="searchRetrieve"
				data['query']="ead.id="+search_term
				
			url_values = urllib.parse.urlencode(data)
			full_url=self.base_url+"?"+url_values
			
			
			print(full_url)


			r= urllib.request.urlopen(full_url).read().decode("utf-8")
			if r:
				d = json.loads(json.dumps(xmltodict.parse(r)))
			
				return (d['srw:searchRetrieveResponse']['srw:records']['srw:record'],d['srw:searchRetrieveResponse']['srw:numberOfRecords'])
			else:
				return ([],"0")

		except Exception as e:
			self.dm.store_out_files()
			print('Error! Code: {c}, Message, {m}'.format(c = type(e).__name__, m = str(e)))
			return ("","")
