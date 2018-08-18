#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  ddb.py
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

from atom.main import data_manager, g
from atom.config import api_keys


class Ddb(object):
	
	oauth_consumer_key=api_keys.API_KEYS["DDB"]
	base_url='https://api.deutsche-digitale-bibliothek.de/'
	id_list_file='ddb/id_list.json'
	id_all_file='ddb/id_all.json'
	result_file='ddb/results.json'
	id_list=[]
	id_all=[]
	results=[]
	key_path__value_list={}
	key__value_list_1={}
	key__value_list_0={}
	level_of_description={}
	languages={}
	count=0
	dm=data_manager.DataManager()
	ID_INDEX_FILE="ddb/id_index.json"
	ID_INDEX=[]
	BLOCKED_FILE="ddb/blocked.txt"       # list of items which should ne be retrieved
	BLOCKED=[]
	
	LABELS=	{
			'identifier':'descriptionIdentifier',
			'title':'title',
			'name':'repository',
			'rights':'reproductionConditions',
			'origin':'findingAids',
			'metadata-rights':'archivistNote',
			'levelOfDescription':'apd_level_of_description',
			'md_format':'md_format',
			'flex_arch_004':'identifier',
			'flex_arch_025':'identifier',
			'flex_arch_031':'identifier',
			'flex_arch_023':'title',
			'flex_arch_030':'title',
			'flex_arch_011':'extentAndMedium',
			'flex_arch_012':'extentAndMedium',
			'flex_arch_014':'repository',
			'flex_arch_013':'archivalHistory',
			'flex_arch_006':'acquisition',
			'flex_arch_010':'scopeAndContent',
			'flex_arch_024':'scopeAndContent',
			'flex_arch_032':'scopeAndContent',
			'flex_arch_001':'arrangement',
			'flex_arch_029':'arrangement',
			'flex_arch_017':'languageNote',
			'flex_arch_015':'physicalCharacteristics',
			'flex_arch_016':'physicalCharacteristics',
			'flex_arch_002':'findingAids',
			'flex_arch_028':'relatedUnitsOfDescription',
			'flex_arch_022':'publicationNote',
			'flex_arch_009':'digitalObjectURI',
			'flex_arch_018':'generalNote',
			'flex_arch_019':'generalNote',
			'flex_arch_020':'generalNote', 
			'flex_arch_021':'generalNote',
			'flex_arch_034':'subjectAccessPoints',
			'flex_arch_033':'placeAccessPoints',
			'flex_arch_027':'nameAccessPoints',
			'flex_arch_005':'alternativeIdentifiers',
			'flex_arch_008':'eventDates',
			'flex_arch_026':'eventDates'

		}
	LEVELS_OF_DESCRIPTIONS={
			"htype_001":"Section",
			"htype_002":"Appendix",
			"htype_003":"Contained work",
			"htype_004":"Annotation",
			"htype_005":"Address",
			"htype_006":"Article",
			"htype_007":"Volume",
			"htype_008":"Additional",
			"htype_009":"Intro",
			"htype_010":"Entry",
			"htype_011":"Fascicle",
			"htype_012":"Fragment",
			"htype_013":"Manuscript",
			"htype_014":"Issue",
			"htype_015":"Illustration",
			"htype_016":"Index",
			"htype_017":"Table of contents",
			"htype_018":"Chapter",
			"htype_019":"Map",
			"htype_020":"Multivolume work",
			"htype_021":"Monograph",
			"htype_022":"Music",
			"htype_023":"Serial",
			"htype_024":"Privilege",
			"htype_025":"Review",
			"htype_026":"Text",
			"htype_027":"Verse",
			"htype_028":"Preface",
			"htype_029":"Dedication",
			"htype_030":"Fonds",
			"htype_031":"Class",
			"htype_032":"Series",
			"htype_033":"Subseries",
			"htype_034":"File",
			"htype_035":"Item",
			"htype_036":"Collection",
			"Tektonik_collection":"Tektonik_collection",
			"repository":"Collection",
			"institution":"Institution"
			
			}
	LANGUAGES={
			"deutsch":"de",
			"englisch":"en",
			"französisch":"fr",
			"spanisch":"es",
			"portugiesisch":"pt",
			"russisch":"ru",
			"japanisch":"jp",
			"chinesisch":"zh",
			"italienisch":"it",
			"dänisch":"dk",
			"schwedisch":"sw",
			"niederländisch":"nl",
			"polnisch":"pl",
			"arabisch":"ar",
			"turkisch":"tk"
		
		}	
	
	REQUESTED=[]
	CURRENT_ITEMS=[]

	
	def __init__(self):
		
		if self.count==0:
			"""
			if os.path.isfile(API_KEY_FILE):
				with open(API_KEY_FILE, 'r') as file:
					keys = json.load(file)
				file.close()
			self.oauth_consumer_key=keys['DDB']
			"""
			if os.path.isfile(self.ID_INDEX_FILE):
				with open(self.ID_INDEX_FILE, 'r') as file:
					self.ID_INDEX = json.load(file)
				file.close()
			#self.oauth_consumer_key=keys['DDB']			
			
			
			#self.read_id_list()
			#self.create_key_lists()

			
	
			
		self.count +=1



	def _open_blocked(self):
		
		if os.path.isfile(self.BLOCKED_FILE) :
			self.BLOCKED=[]
			with open(self.BLOCKED_FILE, 'r') as file:
				self.BLOCKED = file.read().split('\n')
			file.close()	

	def _get_level_of_description(self,htype):
		if htype in self.LEVELS_OF_DESCRIPTIONS:
			return self.LEVELS_OF_DESCRIPTIONS[htype]
		else:
			return htype

	def _get_description_identifier_from_legacy_id(self,legacy_id):
		"""
		retrieves the descriptionIdentifier from an class generated index. 
		It should help to prevent queries for item which are already stored in AtoM
		"""
		if legacy_id in self.ID_INDEX:
			return self.ID_INDEX[legacy_id]
		else:
			return None

	def _write_to_id_index(self,legacy_id,description_identifier):
		if legacy_id not in self.ID_INDEX:
			self.ID_INDEX[legacy_id]=description_identifier
			
	def _store_id_index(self):
		with open(self.ID_INDEX_FILE, 'w') as file:
			file.write(json.dumps(self.ID_INDEX, sort_keys=True, indent=2, ensure_ascii=False))

		file.close()	
		

	def read_id_list(self):
		#populates the id_list from file
		if os.path.getsize(self.id_list_file) > 1:
			with open(self.id_list_file, 'r') as file:
				self.id_list = json.load(file)
			file.close()
			with open(self.id_all_file, 'r') as file:
				self.id_all = json.load(file)
			file.close()
			with open(self.result_file, 'r') as file:
				self.results = json.load(file)
			file.close()
	
	def store_id_list(self):
		#stores the id_list to file
		with open(self.id_list_file, 'w') as file:
			file.write(json.dumps(self.id_list, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		with open(self.id_all_file, 'w') as file:
			file.write(json.dumps(self.id_all, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		with open(self.result_file, 'w') as file:
			file.write(json.dumps(self.results, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()		
		print ('stored')
		print ("id_list: " + str(len(self.id_list)))
		print ("id_all : " + str(len(self.id_all)))		


	
	def load_json(self,rfile):
		with open(rfile, 'r') as rf:
			l= json.load(rf)
		rf.close()
		return l
		

	def store_json(self,l,wfile):
		#stores the id_list to file
		with open(wfile, 'w') as wf:
			wf.write(json.dumps(l, sort_keys=True, indent=4, ensure_ascii=False))
		wf.close()
	
	def OLDexport(self):
		"""
		retrieves data from the German www.deutsche-digitale-bibliothek.de.
		Sends them back to DataManager in batches of {counter} size
		"""
		d=[]
		for e in self.id_list:
			e['legacyId']=e['_original_id']
			if 'nameAccessPoints' in e:
				if e['nameAccessPoints']:
					if type(e['nameAccessPoints']) is not str:
						e['nameAccessPoints']=self.flat(e['nameAccessPoints'])
			if 'placeAccessPoints' in e:
				if e['placeAccessPoints']:
					if type(e['placeAccessPoints']) is not str:
						e['placeAccessPoints']=self.flat(e['placeAccessPoints'])			
			
			e['_status']='confirmed'  # Bei Zeiten ändern
			e['_original_from']='DDB'
			e['culture']=CULTURE
			self.repair_repository_entry(e)
			if e['levelOfDescription']=='institution':
				e['levelOfDescription']='Collection'
			
			if e['_status']=='confirmed':
				d.append(e)
				e['_status']='exported'
		#pause=input('pause')	
		self.store_id_list()
		return d.copy()
	
	def export(self, counter, from_term):
		"""
		retrieves data from the German www.deutsche-digitale-bibliothek.de.
		Sends them back to DataManager in batches of {counter} size
		"""
		
		export_list=[]
		
		for search_term in 	self.dm.search_term_generator(from_term):
			self._open_blocked()
			for match in self._search_generator(search_term):
				print("------------------------------------------")
				print(str(len(export_list)))
				print ("Current search term : ", search_term)
				print("------------------------------------------")
				export_item_family=[]
				
				#print (match)
				if 'id' in match:

					if not self.dm.is_in_atom(match['id']):  # data still unknown in AtoM
						print(match)
						item_raw=self.get(match['id'],'items','view','json')
						print(item_raw)
						if self._is_archival_description(item_raw):
							item_dict=self._get_content(item_raw)
							
							self._write_to_id_index(match['id'],item_dict['descriptionIdentifier'])
							if 'eventStartDates' in item_dict:
								start=item_dict['eventStartDates']
							else:
								start=""
							if 'eventEndDates' in item_dict:
								end=item_dict['eventEndDates']
							else:
								end=""									
							if self.dm._is_in_time(start,end):
								#pprint.pprint(item_raw)
								#pprint.pprint(item_dict)
								if self.dm.predict_item(self._get_content_str(item_dict)):
									item_dict['legacyId']=match['id']
									if 'apd_level_of_description' in item_dict:
										item_dict['levelOfDescription']=self.dm._get_level_of_description(item_dict['apd_level_of_description'])
									item_dict['culture']=CULTURE
									item_dict['language']=CULTURE
									if (match['id'] not in self.CURRENT_ITEMS and match['id'] not in self.dm.LEGACY_IDS 
										and match['id'] not in self.dm.TMP_OUT and match['id'] not in self.dm.DEF_OUT):
										export_item_family.append(item_dict.copy())
										self.CURRENT_ITEMS.append(match['id'])

										print("there is an item ",item_dict)
										for d in self._parents_generator(item_dict['legacyId'],item_dict['repository']):
											if d:
												if isinstance(d,str):
													export_item_family[len(export_item_family)-1]['parentId']=d  #connecting the tree
												else:
													
													if d['legacyId']  not in self.CURRENT_ITEMS:
														export_item_family.append(d)
														self.CURRENT_ITEMS.append(d['legacyId'])
													
													print("there is a parent ",d)
											else:
												continue   #broken tree, we can't use this for AtoM
										for d in self._children_generator(item_dict['legacyId'],item_dict['repository']):
											if d['legacyId'] not in self.CURRENT_ITEMS:
												export_item_family.append(d)
												self.CURRENT_ITEMS.append(d['legacyId'])

											print("there is a child ",d)
								else:
									print("prediction failed")
									self.dm.add_tmp_out(match['id'])
							else:
								print (match['id'], " is not in time")
								self.dm.add_def_out(match['id'])
						else:
							print (match['id'], " is not an archival description")
							self.dm.add_def_out(match['id'])
					else:
						print (match['id'], " is already in AtoM")
						self.dm.add_tmp_out(match['id'])


				if len(export_item_family)>0:
					export_item_family=[x for x in export_item_family if x is not None]
					pprint.pprint(export_item_family)
					#export_item_family.sort(key=operator.itemgetter('apd_level_of_description'))					
					export_list.extend(export_item_family)			

			
			if len(export_list)>counter:
					self.dm.store_out_files()
					yield export_list.copy()
					export_list=[]
		yield export_list.copy()
	

			
				
				
	def OLD_load_tree(self,legacy_id):
		"""
		returns a list of all parent id
		
		Parameter:
		legacyId: String DDB's lagacyId of an item
		"""
		parents=[]
		tmp=self.get(legacy_id,'items','parents')
		if 'hierarchy' in tmp:
			for e in tmp['hierarchy']:
				if 'parent' in e:
					parents.append(e['parent'])
		return parents.copy()
			
		#pprint.pprint(tree[len(tree)-1])

	def _get_content_str(self,item_dict):
		"""
		retrieves the information from all titles, scopenAndContent and date fields, helpful for predicting
		"""
		content_str=""
		if 'arrangement' in item_dict:
			content_str+=item_dict['arrangement']+"; "
		if 'title' in item_dict:
			content_str+=item_dict['title']+"; "
		if 'scopeAndContent' in item_dict:
			content_str+=item_dict['scopeAndContent']+"; "
		#print ("Content String" , content_str)
		return content_str
			
	

	def parent_control(self):
		l=[]
		for e in self.id_list:
			problem=False
			if not 'arrangement' in e:
				arrangement=''
			else:
				arrangement=e['arrangement']
			
			if not 'parentId' in e:
				problem=True
				parentId=""
			elif e['parentId']=="":
				problem=True
				parentId=""
			else:
				parentId=e['parentId']
				for ee in self.id_list:
					problem=True
					if ee['legacyId']==e['parentId']:
						problem=False
						break
			if problem:
				l.append(e)
				print("%s|%s|%s|%s|%s|%s" % (e['legacyId'],parentId,e['levelOfDescription'],e['repository'],e['title'],arrangement))	
		
		for e in l:
			pass
	
	def OLDcheck_orphans(self):
		i=len(self.id_list)
		dm=atom.data_manager()
		l=dm.lookup_orphans('archival_description','DDB')
		for hit in l:
			e=hit['_source']
			self.analyze_results(e['legacyId'], False, True, True)
		self.store_id_list()
		return len(self.id_list)-i

	def flat(self,l):	
		str_l=""
		if type(l) is dict:
			if '$' in l:
				a=l['$'].split(";")
				str_l = a[0] 
			return str_l
			
		
		for e in l:
			if type(e) is dict:
				if '$' in e:
					a=e['$'].split(";")
					str_l += a[0] + "|"
		if len(str_l)>0:
			#print(str_l[:-1])
			return str_l[:-1]
		else:
			return None


	def OLDsearch_items(self,query_list):
		try:
		#while True:
			
			for q in query_list:
				number_of_docs=0
				number_of_results=1
				while number_of_results> number_of_docs:
					d=[]
					r=self.get('','search','','json',q,number_of_docs)
					self.results.append(r)

					pprint.pprint(r)
					print (str(len(r['results'][0]['docs'])) + '<<<<<<<   Results with ' + q) 
					print (str(number_of_docs) + '======= Docs retrieved')
					#self.id_list=r
					number_of_results=int(r['numberOfResults'])
					number_of_docs+=int(r['results'][0]['numberOfDocs'])				
					for doc in r['results'][0]['docs']:
						if not self.in_id_list(doc['id']):
							self.analyze_results(doc['id'], True)
							self.id_all.append(doc['id'])
					
							self.store_id_list()
			return
		except Exception as e: 
		#else:
			self.store_id_list()
			print("Error:", sys.exc_info()[0])
			print(e)
			
	def _search_generator(self,search_term):
		"""
		lookup for search results in DDB using the search terms from DataManager

		Parameter:
		search_term : a tupel ({search_term String},{search_term WD_Instance})
		"""
		try:
		#
			number_of_docs=0
			number_of_results=1
			results=[]
			search_log=""
			while number_of_results> number_of_docs:
			
				d=[]
				r=self.get('','search','','json','"'+search_term[0]+'"',number_of_docs)
				print (results)
				results.extend(r['results'][0]['docs'])
				number_of_results=int(r['numberOfResults'])
				number_of_docs+=int(r['results'][0]['numberOfDocs'])
				
				
				MAX_RETRIEVAL=200
				if number_of_results>MAX_RETRIEVAL and search_term[1]!=WD_COLONY:
					log=str(number_of_results)+"\t"+search_term[0]+"\tDDB\t"+time.strftime("%Y-%m-%d %H:%M:%S")+"[aborted]\n"+search_log
					self.dm.SEARCH_LOG=log+self.dm.SEARCH_LOG	
					return []
				
				#pprint.pprint(r)
				#print (str(len(r['results'][0]['docs'])) + '<<<<<<<   Results with ' + search_term) 
				#print (str(number_of_docs) + '======= Docs retrieved')
				#self.id_list=r
				
				#for doc in r['results'][0]['docs']:
					#if not self.in_id_list(doc['id']):
						#self.analyze_results(doc['id'], True)
						#self.id_all.append(doc['id'])
				
						#self.store_id_list()
			log=str(number_of_results)+"\t"+search_term[0]+"\tDDB\t"+time.strftime("%Y-%m-%d %H:%M:%S")+"\n"+search_log
			print (log)
			self.dm.SEARCH_LOG=log+self.dm.SEARCH_LOG	
			#print (len(results), results)	
			for result in results:
				yield result
		except Exception as e: 
		#
			#self.store_id_list()
			print("Error:", sys.exc_info()[0])
			print(e)	
			
	def OLDanalyze_results(self,item, with_children=True, with_parents=True, review=False):
		result=(self.get(item,'items','view','json'))
		
		#print(result)
		element=self.in_id_list(item)
		if review or not element :
			result=self.get_content(result,item,with_children,with_parents, review)
			#if type(result) is dict:        # we have a valid record !
			if 'legacyId' in result:
				if not review:
					self.id_list.append(result)
				else:
					if 'legacyId' in result:

						print('remove element ' + element['legacyId'] )
						self.id_list.remove(element)
						print('add element ' + result['legacyId'] )
						self.id_list.append(result)
					
						
				
		else:
			print (item + " already in id_list")

		print('----------> ' + str(len(self.id_list)))		
	

	def update_item(self,item):
		pass
	
	def count_repositories(self):
		
		l=[]
		d={}

		for e in self.id_list:
			exists=False
			for ee in l:
				if e['repository'] ==ee['repository']:
					exists=True
					if 'aquisition' in e:
						if e['aquisition'] not in ee['aquisition']:
							ee['aquisition'].append(e['aquisition'])
					if 'arrangement' in e:
						arr=e['arrangement'][0:e['arrangement'].find(">>")-1]
						if arr not in ee['arrangement']:
							#print(l)
							ee['arrangement'].append(arr)
			if not exists:
				d['repository']=e['repository']
				d['arrangement']=[]
				d['aquisition']=[]
				if 'aquisition' in e:
					d['aquisition'].append(e['aquisition'])
				if 'arrangement' in e:
					d['arrangement']= e['arrangement'][0:e['arrangement'].find(">>")-1]
					
				l.append(d.copy())
				d.clear()
		pprint.pprint(l)

	
	def in_id_listOLD(self,item):
		if item in self.id_all:
			return True
		else:
			print(str(len(self.id_all)) +"<<--- Len id_all")
		#print(self.id_list)
		if len(self.id_list)>1:
			for e in self.id_list:
				if e['legacyId']==item:
					return True
		return False
	
	def in_id_list(self,item):
		in_all=False
		element=False
		if item in self.id_all:
			in_all=True
		
		element=next((x for x in self.id_list if x['legacyId']==item),None)
		if element:
			return element
		else:
			return in_all
		

	def get(self,item_id, method='items', subtype='aip', accept='json', query='', offset=0 ):
		try:
			
			data={}
			data['oauth_consumer_key']= self.oauth_consumer_key
			service_url=method + '/' + item_id + '/' +subtype +'?'
			if method=="search":
				service_url=method + "?"
				data['query']=query+" AND (begin_time:[684832 TO 701267] OR end_time:[684832 TO 701267]) AND sector:sec_01"
				data['sort']='ALPHA_ASC'
				data['row']=1000
				data['offset']=offset
				#data['facet']="sector_fct"
				#data['sector_fct']="sec_01"
				
				
			url_values = urllib.parse.urlencode(data)
			full_url=self.base_url+service_url+url_values
			
			
			print(full_url)
			if accept=='json':
				
				r= urllib.request.urlopen(full_url, timeout=30).read().decode("utf-8")
				if item_id+"|"+method not in self.REQUESTED:
					self.REQUESTED.append(item_id+"|"+method)
				return json.loads(r)
			if accept=="xml":
				r= urllib.request.urlopen(full_url).read().decode("utf-8")
				#d = xmltodict.parse(r)
				return r
			else:
				return []
		except Exception as e:
			self.dm.store_out_files()
			print('Error! Code: {c}, Message, {m}'.format(c = type(e).__name__, m = str(e)))
			return []

			
	

	
	
	def get_level_of_description(self,htype):
		if htype in self.level_of_description:
			return self.level_of_description[htype]
		else:
			return htype
	
	def _get_iso_639_1(self,lang):
		lang=lang.lower()
		if lang in self.languages:
			return self.languages[lang]
	
	def _is_archival_description(self,item_dict):
		"""
		checks if the item is an archival description
		"""
		str_item_dict=pprint.pformat(item_dict)
		if str_item_dict.find("flex_arch")==-1:
			return False
		#print("we have an archival description")
		return True
		
	def _get_content(self,result, item="", with_children=False, with_parents=False, review=False):
		"""
		retrieves recursivly the content of an item and store them into a AtoM ready dict
		
		Parameters:
		result: the raw DDB dict
		item, with_children, with_parents, review: depreciated
		
		"""
		d={}
		for e in self._get_key_value(result, ""):
			d={**e,**d}
		
		if 'languageNote' in d:
			d['language']=self._get_iso_639_1(d['languageNote'])
			d['languageOfDescription']=self._get_iso_639_1(d['languageNote'])
			d['scriptOfDescription']="Latn"
		if 'eventDates' in d:
			d['eventStartDates']=self.dm.build_eventDates(d['eventDates'])[0]
			d['eventEndDates']=self.dm.build_eventDates(d['eventDates'])[1]
		else:
			d['eventStartDates']=""
		
		return d.copy()
	
	def _get_label(self,label):
		if label in self.LABELS:
			return self.LABELS[label]
		else:
			return label

	def OLD_get_key_value(self,j, k_label=""):
		#print (type(j))
		if isinstance(j,list):
			for e in j:
				if isinstance(e,dict):
					if '@id' in e: 
						k_label=e['@id']
					for k,v in e.items():
						if isinstance(v,list) or isinstance(v,dict):
							yield from self._get_key_value(v, k_label)
						else:
							if k=="value":
								yield {self._get_label(k_label):v}
				elif isinstance(e,list):
					yield from self._get_key_value(v, k_label)
				else:
					yield {self._get_label(k_label):e}
								
		elif isinstance(j,dict):
			if '@id' in j: 
				k_label=j['@id']
			for k,v in j.items():	
				#print (" ", type(v))
				if isinstance(v,list) or isinstance(v,dict):
					
					yield from self._get_key_value(v,k_label)
				else:
					if k=="value":
						yield {self._get_label(k_label):v}
					else:
						yield {self._get_label(k):v}
		else:
			
			yield {k_label:j}
			#yield from self._get_key_value(j,k_label)

	def _get_key_value(self,j, k_label=""):
		#print (type(j))
		if isinstance(j,list):
			if all(isinstance(x, str) for x in j):
				yield {self._get_label(k_label):"|".join(j)}
			else:
				for e in j:
					if isinstance(e,dict):
						if '@id' in e: 
							k_label=e['@id']
						for k,v in e.items():
							if isinstance(v,list) or isinstance(v,dict):
								yield from self._get_key_value(v, k_label)
							else:
								if k=="value":
									yield {self._get_label(k_label):v}
					elif isinstance(e,list):
						if all(isinstance(x, str) for x in e):
							yield {self._get_label(k_label):"|".join(e)}
						else:
							yield from self._get_key_value(v, k_label)
					else:
						yield {self._get_label(k_label):e}
								
		elif isinstance(j,dict):
			if '@id' in j: 
				k_label=j['@id']
			for k,v in j.items():	
				#print (" ", type(v))
				if isinstance(v,list):
					if all(isinstance(x, str) for x in v):
						yield {self._get_label(k_label):"|".join(v)}
					else:				
					
						yield from self._get_key_value(v,k_label)
				elif isinstance(v,dict):
					yield from self._get_key_value(v,k_label)
				else:
					if k=="value":
						yield {self._get_label(k_label):v}
					else:
						yield {self._get_label(k):v}
		else:
			print("simple value")
			yield {k_label:j}
			yield from _get_key_value(j,k_label)


	def OLD_get_content(self,result, item, with_children, with_parents, review=False):
		""" Analyzes the view item"""
		#dm=atom.data_manager()
		#Let's check if it's a archival description which contains at least some archival vocabulary
		str_result=pprint.pformat(result)
		#print (str_result)
		if str_result.find("flex_arch")==-1:
			print ("Not archival description")
			return None
		d={}
		print (result)
		for k,v in self.key_path__value_list.items():
			d[k]=self.path_get(result,v)
		
		for e in result['item']['fields'][0]['field']:
			if e['@id'] in self.key__value_list_0.keys():
				if self.key__value_list_0[e['@id']] in d:
					d[self.key__value_list_0[e['@id']]]+="; " + e['value']
				else:
					d[self.key__value_list_0[e['@id']]]=e['value']

		for e in result['item']['fields'][1]['field']:
			if e['@id'] in self.key__value_list_1.keys():
				if self.key__value_list_1[e['@id']] in d:
					d[self.key__value_list_1[e['@id']]]+="; " + e['value']
				else:
					d[self.key__value_list_1[e['@id']]]=e['value']
		if 'identifier' in result['item']:
			d['descriptionIdentifier']=result['item']['identifier']
		else:
			d['descriptionIdentifier']=""
		if 'label' in result['item']:
			d['title']=result['item']['label']
		else:
			d['title']=""
		d['culture']=CULTURE
		d['_status']='candidate'
		d['legacyId']=item
		# Some archives also add the creator to the repository entry, seperated by semicolon
		self.repair_repository_entry(d)
			
			
		if 'levelOfDescription' in d:
			d['levelOfDescription']=dm.get_level_of_description(d['levelOfDescription'])	
		d['_original_id']=item


		if 'languageNote' in d:
			d['language']=self.get_iso_639_1(d['languageNote'])
		pprint.pprint(d)
		if 'eventDates' in d:
			d['eventStartDates']=dm.build_eventDates(d['eventDates'])[0]
			d['eventEndDates']=dm.build_eventDates(d['eventDates'])[1]
		else:
			d['eventStartDates']=""
		if with_parents:
			d['parentId']=self.get_parents(d['_original_id'], d['repository'], review)
			
		if with_children:
			self.get_children(d['_original_id'], d['repository'])
		if d['eventStartDates']<'1946':
			return d.copy()
		else:
			return 'Not old enough'
	
	
	def get_parents(self,item, repository, review=False):
		d={}
		result=self.get(item,'items','parents')
		if 'hierarchy' in result:
			for i in range(1,len(result['hierarchy'])-1):
				#print(result['hierarchy'][i])
				if result['hierarchy'][i]['type'] in ('htype_034', 'htype_030'):
					self.analyze_results(result['hierarchy'][i]['id'],False, True, review)
				else:
					d['legacyId']=result['hierarchy'][i]['id']
					d['parentId']=result['hierarchy'][i]['parent']
					d['title']=result['hierarchy'][i]['label']
					d['levelOfDescription']=dm.get_level_of_description(result['hierarchy'][i]['type'])
					d['culture']=CULTURE
					d['language']=CULTURE
					d['_original_id']=d['legacyId']
					d['repository']=repository
					d['_status']='candidate'
					if not self.in_id_list(result['hierarchy'][i]['id']):
							
							self.id_list.append(d.copy())
							
					d.clear()
				
				
			if len(result['hierarchy'])>1:	
				return result['hierarchy'][1]['id']
			else:
				return ""
				
				
	def complete_class(self, l, repository):
		d={}
		new_list=[]
		for e in l:
			print(e)
			d['legacyId']=e['id']
			d['parentId']=e['parent']
			d['title']=e['label']
			d['levelOfDescription']=self.get_level_of_description(e['type'])
			d['culture']=CULTURE
			d['language']=CULTURE
			d['_original_id']=d['legacyId']
			d['repository']=repository
			d['_status']='candidate'	
			d['original_from']='DDB'
			new_list.append(d.copy())
			if not self.in_id_list(d):
				self.id_list.append(d.copy())
			d.clear()
		return new_list.copy()
		
	def _parents_generator(self,item,repository):
		print ("entering the parents generator")
		if item+"|parents" in self.REQUESTED:
			return
		else:
			pass
			#print(self.REQUESTED,"\n--->",item+"|parents")
		parents=self.get(item,'items','parents')
		item_dict={}
		if 'hierarchy' in parents:
			for match in parents['hierarchy']:
				item_dict.clear()
				if match['id']==item :
					yield match['parent']
				if match['id'] in self.CURRENT_ITEMS:
					continue
				if match['id'] in self.dm.LEGACY_IDS :
					continue
				if match['id'] in self.dm.TMP_OUT or match['id'] in self.dm.DEF_OUT:
					continue
				if match['id'] in self.BLOCKED:
					for rematch in parents['hierarchy']:
						self.dm.add_def_out(rematch['id'])
						if rematch['id']==match['id']:
							break
					yield None 
				if not match['parent']:
					if match['type']not in ['htype_030','htype_036','repository', 'Tektonik_collection', 'institution']:
						for rematch in parents['hierarchy']:
							self.dm.add_tmp_out(rematch['id'])
							if rematch['id']==match['id']:
								break						
						yield None    # broken tree
				if not self.dm.is_in_atom(match['id']):
					if match['leaf'] or (not match['leaf'] and not match['aggregationEntity']):   #those should not return a 404
						if match['id']+"|view" in self.REQUESTED :
							print (match['id'], " already requested" )
							continue
						item_raw=self.get(match['id'],'items','view','json')
						if item_raw:
							item_dict=self._get_content(item_raw)
							if 'descriptionIdentifer' in item_dict:
								self._write_to_id_index(parent,item_dict['descriptionIdentifier'])
					item_dict['title']=match['label']
					item_dict['legacyId']=match['id']
					item_dict['culture']=CULTURE
					item_dict['language']=CULTURE
					item_dict['levelOfDescription']=self.dm._get_level_of_description(match['type'])
					item_dict['repository']=repository
					if match['parent']:
						item_dict['parentId']=match['parent']
					else:
						item_dict['parentId']=""
					yield item_dict.copy()
				else:
					print("Parent already in AtoM")
					
		else:
			print ("no hierarchy in parents")
		
			
					
								
		
		

	def _children_generator(self,item, repository):
		"""
		Generates item_dicts which are representing valid children of item
		"""
		print ("entering children generator")
		children=self.get(item,'items','children')
		item_dict={}
		if children:
			if len(children['hierarchy']) >0:
				item_dict.clear()
				for match in children:
					#print (self.CURRENT_ITEMS, "CURRENT")
					if isinstance(match,dict):
						if 'id' in match:
							if match['id'] in self.CURRENT_ITEMS:
								continue
							if match['id'] in self.dm.LEGACY_IDS:
								continue
					if 'id' in match:
						if not self.dm.is_in_atom(match['id']):  # data still unknown in AtoM
							if match['leaf'] or (not match['leaf'] and not match['aggregationEntity']):   #those should not return a 404
								if match['id']+"|view" in self.REQUESTED:
									print (match['id'], " already requested" )
									continue
								item_raw=self.get(match['id'],'items','view','json')
								item_dict=self._get_content(item_raw)
								self._write_to_id_index(match['id'],item_dict['descriptionIdentifier'])
								if 'eventStartDates' in item_dict:
									start=item_dict['eventStartDates']
								else:
									start=""
								if 'eventEndDates' in item_dict:
									end=item_dict['eventEndDates']
								else:
									end=""									
								if self.dm._is_in_time(start,end):
									if self.dm.predict_item(self._get_content_str(item_dict)):
										print ("we have a child ",item)
										item_dict['legacyId']=match['id']
										item_dict['title']=match['label']
										item_dict['culture']=CULTURE
										item_dict['language']=CULTURE
										item_dict['levelOfDescription']=self.dm._get_level_of_description(match['type'])
										item_dict['repository']=repository
										item_dict['parentId']=item
										print("we just have ",item, " as parenId to ", match['id'])
										yield from self._children_generator(match['id'],repository)
									else:
										print ('prediction failed ', self._get_content_str(item_dict))
								else:
									print ("not in time " , item_dict['eventDates'])
							item_dict['legacyId']=match['id']
							item_dict['title']=match['label']
							item_dict['culture']=CULTURE
							item_dict['language']=CULTURE
							item_dict['levelOfDescription']=self.dm._get_level_of_description(match['type'])
							item_dict['repository']=repository
							item_dict['parentId']=item
							yield item_dict.copy()
							
			else:
				print ("no children in item ")
	
	
	def get_children(self,item, repository):
		result=self.get(item,'items','children')
		d={}
		if 'hierarchy' in result:
			for i in range(0,len(result['hierarchy'])-1):
				#print(result['hierarchy'][i])
				if result['hierarchy'][i]['type'] in ('htype_034', 'htype_030'):
					self.analyze_results(result['hierarchy'][i]['id'],True,False)
				else:
					d['legacyId']=result['hierarchy'][i]['id']
					d['parentId']=result['hierarchy'][i]['parent']
					d['title']=result['hierarchy'][i]['label']
					d['levelOfDescription']=self.get_level_of_description(result['hierarchy'][i]['type'])
					d['culture']=CULTURE
					d['language']=CULTURE
					d['_original_id']=d['legacyId']
					d['repository']=repository
					e['_status']='candidate'
					if not self.in_id_list(result['hierarchy'][i]['id']):
							
							self.id_list.append(d.copy())
					self.get_children(d['legacyId'],repository)
							
					d.clear()
				return

	def path_get(self,dictionary, path):
		for item in path.split("/"):
			if item.isdigit():
				item=int(item)
			if item in dictionary:
				dictionary = dictionary[item]
			else:
				return None
		return dictionary		
	
	
	def repair_repository_entry(self,d):
		if 'repository' in d:
			repository=d['repository'].split(';')
			d['repository']=repository[0]
			if len(repository)>1:
				repository[1].replace('Autor/Fotograf:','')
				repository[1].strip(" ")
				if 'eventActors' in d:
					d['eventActors']+='|'+repository[1]
				else:
					d['eventActors']=repository[1]
		return d
		

	def recursive_items(self,dictionary):
		for key, value in dictionary.items():
			if type(value) is dict:
				yield (key, value)
				yield from self.recursive_items(value)
			else:
				if type(value) is list:
					for i in value:
						if type(i) is dict:
							yield from self.recursive_items(i)
						elif type(i) is list:
							for j in i:
								if type(j) is dict:
									yield from self.recursive_items(i)
				yield (key, value)
	
	def get_institutions(self, local=True):
		inst_list=[]
		inst_list=self.load_json('tmp/inst.json')
		l=[]
		try:
			if local:
				for i in inst_list:
					e=self.get(i['id'])
					l.append(e)
					print(i)
					print(e)
					self.store_json(l,'tmp/inst_detail.json')
			else:
				l=self.load_json('tmp/inst_detail2.json')
			
			lp=[]
			
			for e in l:
				d={}
				d['name']=e['provider-info']['provider-name']
				d['uri']=e['provider-info']['provider-uri']
				d['email']=e['provider-info']['provider-email']
				d['id']=e['indexing-profile']['item-id']
				d['parent_id']=e['provider-info']['provider-parent-id']
				d['isil']=e['provider-info']['provider-isil']

				d['plz']=e['view']['cortex-institution'] ['locations']['location']['address']['postalCode']
				d['ort']=e['view']['cortex-institution'] ['locations']['location']['address']['city']
				d['land']=e['view']['cortex-institution'] ['locations']['location']['address']['country']	
				d['sector']=e['view']['cortex-institution']['sector']
				d['sector_name']=self.get_sector(d['sector'])
				
				lp.append(d)

			print(lp)	
			self.store_csv(lp,'tmp/inst.csv')
		except:
			self.store_csv(lp,'tmp/inst.csv')
		return l


	def get_sector(self,sector):
		d={'sec_01':'Archiv', 'sec_02':'Bibliothek', 'sec_03': 'Denkmalpflege', 'sec_04':'Forschung', 'sec_05':'Mediathek', 'sec_06':'Museum','sec_07':'Sonstige'}
		return d[sector]

	def store_csv(self,l,wfile):
		with open(wfile,'w') as wf:
			fieldnames = l[0].keys()			
			for e in l:
					for ee in e.keys():
						if not ee in fieldnames:
							fieldnames.extend(ee)
					

			writer = csv.DictWriter(wf, fieldnames=fieldnames,extrasaction='ignore', delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for e in l:
				writer.writerow(e)
		wf.close()

	def repair(self,l,repository,filename):
		directory='tmp/export/archival_description/'
		record_type='archival_description'
		new=self.complete_class(l,repository)
		dm=atom.data_manager()
		dm.write_to_index(new, record_type,'DDB')
		liste= fo.load_data(directory + filename)
		liste.extend(new)
		dm.write_csv(liste,directory + filename, record_type)
		dm.sort_file(directory+filename,record_type)
