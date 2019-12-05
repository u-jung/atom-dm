#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  nad.py
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
import pprint
from lxml import html
from atom.main import data_manager, g
from atom.config import api_keys
import re


class Nad(object):
	"""
	Imports records from The National Archives Discovery API 
	(https://discovery.nationalarchives.gov.uk/API/sandbox/index)
	"""
	
	
	base_url='https://discovery.nationalarchives.gov.uk/API/'
	id_list_file='../data/nad_id_list.json'
	id_all_file='../data/nad_id_all.json'
	result_file='../data/nad_results.json'
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
	ID_INDEX_FILE="../data/nad_id_index.json"
	ID_INDEX={}
	BLOCKED_FILE="../data/nad_blocked.txt"       # list of items which should ne be retrieved
	BLOCKED=[]
	
	REQUESTED=[]
	CURRENT_ITEMS=[]

	SEARCH_MAPPINGS=(
		('description',"scopeAndContent"),
		('title','title'),
		('numEndDate','eventEndDates'),
		('numStartDate','eventStartDates'),
		('coveringDates','eventDates'),
		('catalogueLevel', 'levelOfDescription'),
		('heldBy',"repository"),
		('id','legacyId'),
		('reference','identifier')
		)
		
	DETAIL_MAPPINGS=(
		("accessRegulation","accessConditions"),
		("copiesInformation","reproductionConditions"),
		('creatorName','eventActors'),
		('custodialHistory','archivalHistory'),
		("language","languageNote"),
		('parentId','parentId'),
		('locationOfOriginals','locationOfOriginals'),
		('physicalDescriptionExtent','extentAndMedium'),
		("physicalDescriptionForm","physicalCharacteristics"),
		('catalogueLevel', 'levelOfDescription')

	)


	#LoDen, NAD cataloguelevel
	LEVELS={
		"1":"Record Group", 	
		"0":"Plan of record groups",
		"4":"Class" ,
		"2":"Fonds",
		"6":"File",
		"11":"Item",
		"3":"Subfonds",
		"7":"Item",
		"5":"Series",
		"9":"File",
		"10":"Item",
	
	}
	
	def __init__(self):
		
		if self.count==0:
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

	def get_level_of_description(self,catalogueLevel):
		if catalogueLevel=="":
			catalogueLevel="11"
		enLevel=self.LEVELS[catalogueLevel]
		e=next((x for x in g.A_LOD if enLevel.lower()==x['en'].lower()),False)
		
		if e:
		
			return e[g.CULTURE]
		else:
			return ""

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
			e['culture']=g.CULTURE
			self.repair_repository_entry(e)
			if e['levelOfDescription']=='institution':
				e['levelOfDescription']='Collection'
			
			if e['_status']=='confirmed':
				d.append(e)
				e['_status']='exported'
		#pause=input('pause')	
		self.store_id_list()
		return d.copy()
	
	def get_item_by_id(self,ddb_id):
		export_list=[]
		export_item_family=[]
		if not self.dm.is_in_atom(ddb_id):  # data still unknown in AtoM
			item_raw=self.get(ddb_id,'items','view','json')
			item_dict=self._get_content(item_raw)
			print("item_dict", item_dict)
			if not ('eventStartDates' in item_dict and 'eventEndDates' in item_dict):
				item_dict['eventDates']=self._map('eventDates',item_dict,True,"str",True)
				item_dict['eventStartDates']=self.dm.build_eventDates(item_dict['eventDates'])[0]
				item_dict['eventEndDates']=self.dm.build_eventDates(item_dict['eventDates'])[1]
				#print('->->---------------\n',item_dict['eventDates'],item_dict['eventStartDates'],item_dict['eventEndDates'])
			item_dict['legacyId']=ddb_id
			if 'apd_level_of_description' in item_dict:
				item_dict['levelOfDescription']=self.dm._get_level_of_description(item_dict['apd_level_of_description'])
			for field in self.FIELD_MAPPING.keys():
				item_dict[field]=self._map(field,item_dict,True,"str",True)
				item_dict=self._set_pipes(item_dict,["genreAccessPoints","placeAccessPoints","nameAccessPoints","subjectAccessPoints"])
				item_dict=self._dedup(item_dict)
				item_dict['culture']=g.CULTURE
				item_dict['language']=g.CULTURE
			if 'eventDates' in item_dict:
				item_dict['eventStartDates']=self.dm.build_eventDates(item_dict['eventDates'])[0]
				item_dict['eventEndDates']=self.dm.build_eventDates(item_dict['eventDates'])[1]
			if not("levelOfDescription" in item_dict):
				if 'identifer' in item_dict or 'extentAndMedium' in item_dict or 'physicalCharacteristics' in item_dict:
					item_dict['levelOfDescription']="File"
				else:
					item_dict['levelOfDescription']="Class"
			export_item_family.append(item_dict.copy())
			#self.CURRENT_ITEMS.append(match['id'])
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
			print (ddb_id, " is already in AtoM")
			self.dm.add_tmp_out(ddb_id)		
		if len(export_item_family)>0:
			export_item_family=[x for x in export_item_family if x is not None]
			pprint.pprint(export_item_family)
			#export_item_family.sort(key=operator.itemgetter('apd_level_of_description'))					
			export_list.extend(export_item_family)	
		return export_list.copy()
	
	def export(self,**kwargs):
		"""
		retrieves data from The National Archives Discovery Portal.
		Sends them back to DataManager in batches of {counter} size
		"""
		export_list=[]
		counter=kwargs['counter']
		#kwargs={'from_term':kwargs['from_term'], 'predefined':kwargs['predefined']}
		if 'facet' in kwargs and 'facet_value' in kwargs:
			facet= kwargs['facet'] + "=" + kwargs['facet_value']
		else:
			facet=""
		kwargs['messages']=False
		
		if 'id' in kwargs:
			yield self.get_item_by_id(kwargs['id'])
			return
	
		for search_term in 	self.dm.search_term_generator(**kwargs):
			self._open_blocked()
			for match in self._search_generator(search_term, facet, kwargs['timespan']):
				print("------------------------------------------")
				print(str(len(export_list)))
				print ("Current search term : ", search_term)
				print("------------------------------------------")
				export_item_family=[]
				print(json.dumps(match,indent=2))

				#print (match)
				if 'id' in match:

					if not self.dm.is_in_atom(match['id']):  # data still unknown in AtoM
						print(match)
						item_dict=self._get_content(match,{},False)
						item_raw=self.get(match['id'],'detail')
						if 'id' in item_raw:   # check for archival description if no facet where given else let pass everything
							item_dict=self._get_content(item_raw,item_dict,True)
							#print("item_dict",item_dict)
							if not ('eventStartDates' in item_dict and 'eventEndDates' in item_dict) and 'eventDates' in item_dict:
								item_dict['eventStartDates']=self.dm.build_eventDates(item_dict['eventDates'])[0]
								item_dict['eventEndDates']=self.dm.build_eventDates(item_dict['eventDates'])[1]
							item_dict['levelOfDescription']=self.get_level_of_description(str(item_dict['levelOfDescription']))
							item_dict['findingAids']="https://discovery.nationalarchives.gov.uk/details/r/"+item_dict['legacyId']	
							item_dict=self._set_pipes(item_dict,["genreAccessPoints","placeAccessPoints","nameAccessPoints","subjectAccessPoints"])
							item_dict['culture']=g.CULTURE
							item_dict['language']="en"
							context_raw=self.get(match['id'],'context')
							
							for e in context_raw:
								d={
									'legacyId':e['id'],
									'parentId':e['parentId'],
									'title':e['title'],
									'identifier':e['citableReference'],
									'levelOfDescription':self.get_level_of_description(str(e['catalogueLevel'])),
									'repository':item_dict['repository'],
									'culture':g.CULTURE,
									'language':"en",
									'languageOfDescription':"en"
								}
								if isinstance(d['title'],str):
									d['title']=re.sub('<[^>]*>', '', d['title'])
								else:
									d['title']=""
								
								if d['title']=="":
									if not e['scopeContent']['description'] is None:
										d['title']=re.sub('<[^>]*>', '', e['scopeContent']['description'])
								if d['legacyId']==item_dict['legacyId']:
									d['levelOfDescription']=self.get_level_of_description(str(e['catalogueLevel']))
									export_list.append(item_dict.copy())
								else:
									if d['legacyId'] not in self.CURRENT_ITEMS:
										export_list.append(d.copy())
										self.CURRENT_ITEMS.append( d['legacyId'])
					else:
						print (match['id'], " is already in AtoM")
						self.dm.add_tmp_out(match['id'])

				"""if len(export_list)>10:
					print(json.dumps(export_list,indent=2))
					a=input("STOP")"""

			if len(export_list)>counter:
					
					self.dm.store_out_files()
					yield self.add_country_code(export_list)
					export_list=[]
		yield self.add_country_code(export_list.copy())
	
	def add_country_code(self,l):
		for e in l:
			if 'parentId' in e:
				if not e['parentId'] is None:
					e['parentId']="GB-"+e['parentId']
				else:
					e['parentId']=""
			if 'legacyId' in e:
				e['legacyId']="GB-"+e['legacyId']			
		return l.copy()

	def _set_pipes(self,d,fields):
		for field in fields:
			if field in d:
				d[field]="|".join(d[field].split(";"))
		return d.copy()
	
	def _dedup(self,d):
		for k,v in d.items():
			if not v is None:
				tmp=v.split("|")
				tmp=list(set(tmp))
				d[k]="|".join(tmp)
		return d.copy()
	



	def _get_content_str(self,item_dict):
		"""
		retrieves the information from all titles, scopenAndContent and date fields, helpful for predicting
		"""
		return self._map('arrangement',item_dict,True) + "; " + self._map('title',item_dict,True) + "; " + self._map('scopeAndContent',item_dict,True) 
		
			
	def _search_generator(self,search_term, facet,timespan):
		"""
		lookup for search results in NAD using the search terms from DataManager

		Parameter:
		search_term : a tupel ({search_term String},{search_term WD_Instance})
		"""
		
		try:
		#if 1==1:
			number_of_docs=0
			number_of_results=1
			results=[]
			search_log=""
			while number_of_results> number_of_docs:
			
				d=[]
				r=self.get(search_term[0],"search")
				#print ("r:",r)
				if 'records' in r:
					results.extend(r['records'])
					number_of_results=0
					number_of_docs+=int(r['count'])
				else:
					
					return []
				
				MAX_RETRIEVAL=200
				if number_of_results>MAX_RETRIEVAL and search_term[1]!=g.WD_COLONY:
					log=str(number_of_results)+"\t"+search_term[0]+"\tNAD\t"+time.strftime("%Y-%m-%d %H:%M:%S")+"[aborted]\n"+search_log
					self.dm.SEARCH_LOG=log+self.dm.SEARCH_LOG	
					return []

			log=str(number_of_results)+"\t"+search_term[0]+"\tNAD\t"+time.strftime("%Y-%m-%d %H:%M:%S")+"\n"+search_log
			print (log)
			self.dm.SEARCH_LOG=log+self.dm.SEARCH_LOG	
			#print (len(results), results)	
			for result in results:
				yield result
		except Exception as e: 
		#if 0==1:
			#self.store_id_list()
			print("We have an error on Nad._search_generator:", sys.exc_info()[0])
			print(e)	
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			

	
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
		

	#def get(self,item_id, method='items', subtype='aip', accept='json', query='', offset=0 , facet="", timespan=True):
	def get(self, search_term, method='search'):
		data={}
		headers = {}		
		headers['Accept'] = 'application/json'
		if method=='search':
			url=self.base_url+"search/records?"
			data['sps.dateFrom']=g.START_TIME_ISO
			#data['sps.dateTo']=g.END_TIME_ISO
			data['sps.dateTo']="1920-01-01"
			data['sps.searchQuery']='"'+search_term+'"'
			data['sps.sortByOption']='DATE_ASCENDING'
			data['sps.resultsPageSize']=1000
		elif method=="detail":
			url=self.base_url+"records/details/"+search_term
		elif method=="context":
			url=self.base_url+"records/context/"+search_term
		data = urllib.parse.urlencode(data)
		#data = data.encode('ascii') # data should be bytes
		#req = urllib.request.Request(url, data, headers)
		full_url=url+data
		print(url)
		r= urllib.request.urlopen(full_url).read().decode("utf-8")
		#print(r)

		if r:
			r=json.loads(r)
			if r:
				return r
			else:
				return []
		else:
			return []

	
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
		
	def _get_content(self,result,d,detail=True):
		"""
		iterates trough the content of an item and store them into a AtoM ready dict
		
		Parameters:
		result: the raw NAD dict
		d: the dict to fill
		detail: True if the Details Page should be analyzed
		
		"""
		if not detail:
			for f in self.SEARCH_MAPPINGS:
				if f[0] in result:
					d[f[1]]=result[f[0]]
					if f[0]=="heldBy":
						d[f[1]]=d[f[1]][0]
					if f[0] in ("numEndDate","numStartDate"):
						datum=str(d[f[1]])
						d[f[1]]=datum[0:4]+"-"+datum[4:6]+"-"+datum[6:8]
				 
			
			d['culture']=g.CULTURE
			if 'languageNote' in d:
				d['language']=self._get_iso_639_1(d['languageNote'])
			d['languageOfDescription']="en"
			d['scriptOfDescription']="Latn"
			if 'eventDates' in d:
				d['eventStartDates']=self.dm.build_eventDates(d['eventDates'])[0]
				d['eventEndDates']=self.dm.build_eventDates(d['eventDates'])[1]
			else:
				d['eventStartDates']=""
		else:
			for f in self.DETAIL_MAPPINGS:
				if f[0] in result:
					if not result[f[0]] is None:
						d[f[1]]=result[f[0]]
		
		for k,v in d.items():
			if not isinstance(d[k],str):
				d[k]=""
		return d.copy()
	
	def _get_label(self,label):
		if label in self.LABELS:
			return self.LABELS[label]
		else:
			return label


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





	def complete_class(self, l, repository):
		d={}
		new_list=[]
		for e in l:
			print(e)
			d['legacyId']=e['id']
			d['parentId']=e['parent']
			d['title']=e['label']
			d['levelOfDescription']=self.get_level_of_description(e['type'])
			d['culture']=g.CULTURE
			d['language']=g.CULTURE
			d['_original_id']=d['legacyId']
			d['repository']=repository
			d['_status']='candidate'	
			d['original_from']='DDB'
			new_list.append(d.copy())
			if not self.in_id_list(d):
				self.id_list.append(d.copy())
			d.clear()
		return new_list.copy()
		

	
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
