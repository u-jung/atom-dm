#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  data_manager.py
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

import urllib.parse
import urllib.request
import urllib
from elasticsearch import Elasticsearch
import json
import csv
import os
import MySQLdb
import re
import time
import pathlib
import pprint
import requests

from atom.config import mysql_opts
from atom.main import g
from atom.helpers.helper import fileOps, listOps, stringOps, osOps
fo=fileOps()
lo=listOps()
so=stringOps()
oo=osOps()

class DataManager(object):	
	"""
	Core class of atom.py.
	
	Does manage:
	- The import from external source into the candidates index of elastic search.
	- Evaluation of imported records and relevance check.
	- Import of confirmed records into AtoM database
	- Some management tasks of AtoM
		
	"""
	# the standard index for intermediary storage 
	indexstore='candidates'
	
	# search results from elasticsearch should be retrieved on one single chunk
	max_results=200000
	
	# This is just for debuging
	#log = logging.getLogger(__name__)
	
	# This is the list of Fonds which are fully integrated. Items of this fonds can be skipped during import
	jump_fonds=['BArch R 1001','BArch R 1002','BArch R 1003', 'BArch R 8023', 'BArch R 8024','BArch R 1003','BArch R 8124', 'BArch RW 51', 'BArch RH 88', 'BArch RW 51']
	
	# These are the locations of the helper files
	KEYWORDS_FILE = "atom/data/keywords.json"      # A list of keywords (with scores) to look for in external databases
	"""
	List of dicts with keys:
	item : Wikidata object number Qxxxx
	label_de: The German label
	description: The German Description
	label_clean: The German label without stopwords
	instances: [list of values of P31]
	search_terms: [list of search terms to use for queries]
	timestamp : a timestamp of last modifying
	frequency : a integer indicating the  the number of parallel occurances inside Wikidata labels (for P31>Q5 [human] also the number of people with the same family name)  
	"""
	KILLERS_FILE = "atom/data/killers.txt" # A list of words, which presence indicates "out of theme" (here: German Colonialism) 
	ENTITIES_FILE = "atom/data/entities.txt"		# A list of Wikidata items already used for keywords.json
	STOPWORDS_FILE ="atom/data/stopwords.txt"	 # A list of stopwords
	FREQUENT_NAMES_FILE="atom/data/frequent_names.txt" 
	NAME_SUFFIXES_FILE = "atom/data/name_suffixes.json"   # A list of typical name suffixes to adress people
	SEARCH_HISTORY_PATH = "search_history/"   # A list of searches which has been already carried out (a combination of search term, date and external database)
	SEARCH_HISTORY_FILE_SUFFIX="_search_history.csv"
	TMP_RESULTS_PATH = "tmp_results/"      # The path to store temporaly results
	#MYSQL_OPT_FILE="/home/kol/mysql_opts.json"   # a file with a single json string 		{"host": "localhost","user": "my username","pass": "my password","db":  "atom"}
	CURRENT_DESCRIPTION_IDENTIFIERS_FILE="tmp_results/current_description_identifiers.txt" # a list of description identifier which already exist in tmp_results/*.csv but not yet imported
	SEARCH_LOG_FILE="log/adm_search.log" # The storage for the search log
	TMP_OUT_FILE="search_history/tmp_out.txt"     #id of items which are temporary out of interest 
	DEF_OUT_FILE="search_history/def_out.txt" #id of items which are definitly out of interest
	INSTITUTIONS_FILE="atom/data/institutions.json"
	NEW_RELATION_FILE="tmp_results/new_authority_relations.json"
	
	COUNTER=0
	
	LANGUAGES={
		"ger":"de",
		"eng":"en",
		"fre":"fr",
		"spa":"es",
		"prt":"pt",
		"rus":"ru",
		"jpn":"jp",
		"zho":"zh",
		"ita":"it",
		"dan":"dk",
		"swe":"sw",
		"nld":"nl",
		"pol":"pl",
		"ara":"ar",
		"tur":"tk"
	
	}
	
	#URL
	BASE_URL="http://vm.atom/"
	
	#LISTS
	KEYWORDS=[]
	STOPWORDS=[]
	FREQUENT_NAMES=[]
	NAME_SUFFIXES=[]
	KILLERS=[]
	INSTITUTIONS=[]
	CURRENT_DESCRIPTION_IDENTIFIERS=[]
	SEARCH_LOG=""
	LEGACY_IDS=[]    #the legacy_id of active lists or from the stored csv 
	TMP_OUT=[]
	DEF_OUT=[]
	COORPORATE_DIFFENCES=["zu", "von", "ag", "a.g.", "g.m.b.h", "gmbh", "d.k.g", "dkg", "ohg",
						  "mbh", "m.b.h", "kg","k.g."]   # self.STOPPWORDS MINUS COORPORATE_DIFFERENCES  GIVE STOPPWORDS for persons
						  
						  
	ARCHIVAL_DESCRIPTIONS_FIELDS = ['out',
							'keywordInfo',
							'legacyId',
							'parentId',
							'title',
							'levelOfDescription',
							'repository',
							'scopeAndContent',
							'subjectAccessPoints',
							'placeAccessPoints',
							'nameAccessPoints',
							'genreAccessPoints',
							'eventDates',
							'eventTypes',
							'eventStartDates',
							'eventEndDates',
							'eventDescriptions',
							'qubitParentSlug',
							'identifier',
							'accessionNumber',
							'extentAndMedium',
							'archivalHistory',
							'acquisition',
							'appraisal',
							'accruals',
							'arrangement',
							'accessConditions',
							'reproductionConditions',
							'language',
							'script',
							'languageNote',
							'physicalCharacteristics',
							'findingAids',
							'locationOfOriginals',
							'locationOfCopies',
							'relatedUnitsOfDescription',
							'publicationNote',
							'digitalObjectURI',
							'generalNote',
							'descriptionIdentifier',
							'institutionIdentifier',
							'rules',
							'descriptionStatus',
							'levelOfDetail',
							'revisionHistory',
							'languageOfDescription',
							'scriptOfDescription',
							'sources',
							'archivistNote',
							'publicationStatus',
							'physicalObjectName',
							'physicalObjectLocation',
							'physicalObjectType',
							'alternativeIdentifiers',
							'alternativeIdentifierLabels',
							'eventActors',
							'eventActorHistories',
							'eventPlaces',
							'culture',
							'status']
	
	#Endpoints
	WD_API_ENDPOINT = "https://www.wikidata.org/w/api.php"
	WD_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
	
	MYSQL_CONNECTION = None
	
	# Limits
	LIMIT_FROM = 1850   # only files with eventdates from LIMIT_FROM to LIMIT_TO should be retrieved
	LIMIT_TO =1946
	
	
	LEVELS_OF_DESCRIPTION={
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
		"htype_037":"?",
		"htype_038":"?",
		"htype_039":"?",
		"Tektonik_collection":"Tektonik_collection",
		"repository":"Collection",
		"institution":"Institution"
		
		}
	# Level of Description used by AtoM	
	ATOM_LOD_FONDS=227 
	ATOM_LOD_SUBFONDS = 228 
	ATOM_LOD_COLLECTION = 229 
	ATOM_LOD_SERIES =  230
	ATOM_LOD_SUBSERIES = 231
	ATOM_LOD_FILE = 232 
	ATOM_LOD_ITEM = 233 
	ATOM_LOD_PART = 290

	# manage data extraction and import from various sources 
	def __init__(self, debug=False):
		self.es= Elasticsearch()
		self._open_keywords()
		if not debug:
			log=None
		
		
		"""
		if os.path.isfile(self.MYSQL_OPT_FILE):
			with open(self.MYSQL_OPT_FILE, 'r') as file:
				mysql_opts = json.load(file)
			file.close()
		else:
			print("not found")
		"""
		self.MYSQL_CONNECTION = MySQLdb.connect(mysql_opts.mysql_opts['host'], mysql_opts.mysql_opts['user'], mysql_opts.mysql_opts['pass'], mysql_opts.mysql_opts['db'],use_unicode=True, charset="utf8") 

	
	def __del__(self):
		#self._store_objects()
		#self.store_out_files()
		pass


	def _open_current_description_identifiers(self):
		self.CURRENT_DESCRIPTION_IDENTIFIERS=fo.load_once(self.CURRENT_DESCRIPTION_IDENTIFIERS,self.CURRENT_DESCRIPTION_IDENTIFIERS_FILE)

	
	def _open_keywords(self):

		self.KEYWORDS=fo.load_once(self.KEYWORDS,self.KEYWORDS_FILE)



	def _open_institutions(self):
		self.INSTITUTIONS=fo.load_once(self.INSTITUTIONS,self.INSTITUTIONS_FILE)
		
	def _open_entities(self):
		self.ENTITIES=fo.load_once(self.ENTITIES,self.ENTITIES_FILE)		

	def _open_names_suffixes(self):
		self.NAME_SUFFIXES=fo.load_once(self.NAME_SUFFIXES,self.NAME_SUFFIXES_FILE)
	
	def _open_killers(self):
		self.KILLERS=fo.load_once(self.KILLERS,self.KILLERS_FILE)

	def _open_frequent_names(self):
		self.FREQUENT_NAMES=fo.load_once(self.FREQUENT_NAMES,self.FREQUENT_NAMES_FILE)

	def _open_out_files(self):
		self.TMP_OUT=fo.load_once(self.TMP_OUT,self.TMP_OUT_FILE)
		self.DEF_OUT=fo.load_once(self.DEF_OUT,self.DEF_OUT_FILE)


	def store_out_files(self):
		ok=False
		if fo.save_data(self.TMP_OUT, self.TMP_OUT_FILE):
			ok=True
		if fo.save_data(self.DEF_OUT, self.DEF_OUT_FILE):
			ok=True
		return ok


	def add_tmp_out(self,item):
		"""
		adds an id to the list of temprary outs
		"""
		if item not in self.TMP_OUT:
			self.TMP_OUT.append(item)
			
	def add_def_out(self,item):
		"""
		adds an id to the list of definitiv outs
		"""
		if item not in self.DEF_OUT:
			self.DEF_OUT.append(item)	
			
	def create_search_term_list(self, wdid=False):
		f=open("keywords/search_terms.txt","w")
		for term in self.search_term_generator():
			if wdid:
				f.write(term[0] + "\t" + term[1] +"\n")
			else:
				f.write(term[0]+"\n")
			print(term[0])
		f.close()
			
			
	def join_csv(self, add_keyword=True):
		"""
		joins all existing import.csv files inside self.TMP_RESULTS_PATH into one.
		Eliminates duplicates and reorders the items. The new file is stored in the root of self.TMP_RESULTS_PATH
		"""
		path = self.TMP_RESULTS_PATH
		l=[]
		for root in os.walk(path):
			if root[0]==self.TMP_RESULTS_PATH:
				continue
			if  os.path.isfile(root[0]+"/import.csv") :
				l.extend(fo.load_data(root[0]+"/import.csv"))
		print(len(l))
		l=self._remove_out_of_time(l)
		l=self._remove_duplicates(l,'legacyId')

		l=self.hierarchy_sort(l)[0]
		oo.remove_all(self.TMP_RESULTS_PATH)
		
		self.write_csv(l,self.TMP_RESULTS_PATH+"import.csv","archival_description")
		if add_keyword:		
			l=self._add_keyword_info(l)
		
		self.write_csv(l,self.TMP_RESULTS_PATH+"import.csv","archival_description")
	
	def _remove_out_of_time(self,l):
		"""
		removes "File" items from list if there are out of time
		"""
		new=[]
		for e in l:
			
			if self._is_in_time(e['eventStartDates'],e['eventEndDates']):
				new.append(e)
			else:
				if e['levelOfDescription'] not in ['File']:
					new.append(e)
		return new.copy()

	def reduce_csv(self,x=False, add_keyword=True):
		"""
		iterates once again the csv after manual inspection
		eleminates the marked items (and all children) abnd writes them into the self.DEF_OUT
		"""
		shutil.copy2(self.TMP_RESULTS_PATH+"import.csv",self.TMP_RESULTS_PATH+"import_old.csv")
		l=fo.load_file(self.TMP_RESULTS_PATH+"import.csv")
		new=[]
		out=[]
		incertain=[]
		
		i=0
		while i<len(l):
			print(i,len(l))
			e=l[i]
			
			if 'eventDates' in e:
				(e['eventStartDates'],e['eventEndDates'])=self.build_eventDates(e['eventDates'])
			if x:
				l=self._find_dead_ends(l)
				if not self._is_in_time(e['eventStartDates'],e['eventEndDates']):
					if e['levelOfDescription']=='File':
						e['out']="x"
						l=self._write_to_children(l,e['parentId'],'out',"x")	
						self.add_def_out(e['legacyId'])
				for killer in self.KILLERS:
					if e['title'].lower().find(killer)>-1 or e['scopeAndContent'].lower().find(killer)>-1:
						e['out']="x"
						l=self._write_to_children(l,e['parentId'],'out',"x")	
						self.add_def_out(e['legacyId'])		
						
				if e['out'] not in ["","."]:
					#l=self._write_to_parents(l,e['parentId'],'out',".")	
					l=self._write_to_children(l,e['legacyId'],'out',".")
					self.add_def_out(e['legacyId'])	
			if e['out']==".":
				#print(type(l),e['legacyId'],e['parentId'])
				l=self._write_to_parents(l,e['parentId'],'out',".")	
				l=self._write_to_children(l,e['legacyId'],'out',".")
			i+=1	
	
		for e in l:
			if e['out']=='.':
				new.append(e)
	
		self.write_csv(new,self.TMP_RESULTS_PATH+"import.csv","archival_description")
		self.write_csv(l,self.TMP_RESULTS_PATH+"import_old_modified.csv","archival_description")
		self._post_import(l,"",True,add_keyword)
	
	def _find_dead_ends(self,l):
		for e in l:
			if e['levelOfDescription']!="File" and e['out']=='' :
				if not next((x for x in l if x['parentId']== e['legacyId']),False):
					e['out']="-"
					l=self._write_to_parents(l,e['legacyId'],'out','-')
				
		return l.copy()
		
	
	
	def _write_to_children(self,l,legacyId,field,value):
		print(type(l),legacyId)
		for item in (x for x in l if x["parentId"]==legacyId):
			if item:
				item[field]=value
				l=self._write_to_children(l,item['legacyId'],field,value)
		return l.copy()
	
	
	def _write_to_parents(self,l,legacyId,field,value):
		"""
		writes a value into field of all parents of legacyId
		"""
		item=next((x for x in l if x["legacyId"]==legacyId),False)
		if item:
			#print("parent found",item['legacyId'])
			if item[field]!="":
				if value == ".":
					#print("recursive")
					l=self._write_to_parents(l,item['parentId'],field,value)
				else:
					#print ("return",len(l))
					return l.copy()
			else:
				#print("recursive 2")
				item[field]=value
				l=self._write_to_parents(l,item['parentId'],field,value)

		else:
			#print ("return because no parent",len(l))
			return l.copy()
		return l.copy()
			
	def get_from_atom(self, item):
		"""
		retrieves all information concerning a specific information_object from atom using AtoM's id
		returns a dict representing an csv entry
		Parameters:
		item : the AtoM id of the informtion_object
		"""
		d={}
		fields_str="extent_and_medium,archival_history,acquisition,scope_and_content,appraisal,accruals,arrangement,access_conditions,reproduction_conditions,physical_characteristics,finding_aids,location_of_originals,location_of_copies, related_units_of_description,rules,sources,revision_history,culture,identifier,level_of_description_id, repository_id ,parent_id ,description_identifier,title,institution_responsible_identifier"		
		fields=fields_str.split(",")					
		"""
		#'legacyId',
		#'parentId',
		#'title',
		##'levelOfDescription',
		##'repository',
		#'scopeAndContent',
		#'subjectAccessPoints',
		#'placeAccessPoints',
		#'nameAccessPoints',
		#'genreAccessPoints',
		#'eventDates',
		#'eventTypes',
		#'eventStartDates',
		#'eventEndDates',
		#'eventDescriptions',
		#'qubitParentSlug',
		#'identifier',
		'accessionNumber',
		#'extentAndMedium',
		#'archivalHistory',
		#'acquisition',
		#'appraisal',
		#'accruals',
		#'arrangement',
		#'accessConditions',
		#'reproductionConditions',
		#'language',
		#'script',
		#'languageNote',
		#'physicalCharacteristics',
		#'findingAids',
		#'locationOfOriginals',
		#'locationOfCopies',
		#'relatedUnitsOfDescription',
		#'publicationNote',
		'digitalObjectURI',
		#'generalNote',
		#'descriptionIdentifier',
		#'institutionIdentifier',
		#'rules',
		'descriptionStatus',
		'levelOfDetail',
		#'revisionHistory',
		#'languageOfDescription',
		#'scriptOfDescription',
		#'sources',
		#'archivistNote',
		#'publicationStatus',
		'physicalObjectName',
		'physicalObjectLocation',
		'physicalObjectType',
		#'alternativeIdentifiers',
		#'alternativeIdentifierLabels',
		'eventActors',
		'eventActorHistories',
		'eventPlaces',
		#'culture',
		'status']
		"""

		query="select "+fields_str+" from information_object i join information_object_i18n ii on i.id=ii.id where i.id="+item+";"
		r=self.get_mysql(query,True)
		if r:
			i=0
			for e in r:
				d[re.sub(r'_([a-z])',lambda m: m.group(1).upper(),fields[i])]=e
				i+=1
		print(d)
		if 'levelOfDescription' in d:
			query="select name from term_i18n where id="+str(d['levelOfDescriptionId'])+";"
			r=self.get_mysql(query,True)
			if r:
				d['levelOfDescription']=r[0]
		
		if 'repositoryId' in d:
			if d['repositoryId']:
				query="select respository_id from actor_i18n where id="+e['repositoryId'] +";"
				r=self.get_mysql(query,True)
				d['repository']=r[0]
		
		d['institutionIdentifier']=d['institutionResponsibleIdentifier']
		
		query ="select source_id from keymap where target_id="+item+";"	
		r=self.get_mysql(query,True)
		if r:
			d['legacyId']=r[0]
		
		if d['parentId']:
			query=" select slug from slug where object_id="+d['parentId']+";"
			r=self.get_mysql(query,True)
			if r:
				d['qubitParentSlug']=r[0]
			
			query ="select source_id from keymap where target_id="+d['parentId']+";"	
			r=self.get_mysql(query,True)
			if r:
				d['parentId']=r[0]
		
		query ="select content from note_i18n ni join note n on ni.id=n.id where object_id="+item+" and type_id=124;"
		r=self.get_mysql(query,False)
		if r:
			value=[]
			for e in r:
				value.append(e[0])
			d['archivistNote']="|".join(value)

		query ="select content from note_i18n ni join note n on ni.id=n.id where object_id="+item+" and type_id=174;"
		r=self.get_mysql(query,False)
		if r:
			value=[]
			for e in r:
				value.append(e[0])
			d['languageNote']="|".join(value)

		query ="select content from note_i18n ni join note n on ni.id=n.id where object_id="+item+" and type_id=125;"
		r=self.get_mysql(query,False)
		if r:
			value=[]
			for e in r:
				value.append(e[0])
			d['generalNote']="|".join(value)
	
		query ="select content from note_i18n ni join note n on ni.id=n.id where object_id="+item+" and type_id=120;"
		r=self.get_mysql(query,False)
		if r:
			value=[]
			for e in r:
				value.append(e[0])
			d['publicationNote']="|".join(value)
		
		query='select name from term_i18n t join status s on t.id=s.status_id where s.object_id='+item+' and t.culture="de";'
		r=self.get_mysql(query,True)
		if r:
			d['publicationStatus']=r[0]
		
		
		query='select scope,name, value from property p join property_i18n pi on p.id=pi.id where object_id='+item+';'
		r=self.get_mysql(query,False)
		alternativeIdentifiers=[]
		alternativeIdentifierLabels=[]
		language=[]
		script=[]
		languageOfDescription=[]
		scriptOfDescription=[]
		if r:
			for line in r:
				if line[0]:
					if line[0]=="alternativeIdentifiers":
						alternativeIdentifiers.append(line[2])
						alternativeIdentifierLabels.append(line[1])
				else:
					if line[1]==("language"):
						m= re.search('"([^"]*)"',line[2])
						if m:
							language.append(m.group(1))
					if line[1]==('script'):
						m= re.search('"([^"]*)"',line[2])
						if m:
							script.append(m.group(1))
					if line[1]==("languageOfDescription"):
						m= re.search('"([^"]*)"',line[2])
						if m:
							languageOfDescription.append(m.group(1))
					if line[1]==('scriptOfDescription'):
						m= re.search('"([^"]*)"',line[2])
						if m:
							scriptOfDescription.append(m.group(1))							
			d['languageOfDescription']="|".join(languageOfDescription)
			d['scriptOfDescription']="|".join(scriptOfDescription)
			d['alternativeIdentifiers']="|".join(alternativeIdentifiers)
			d['alternativeIdentifierLabels']="|".join(alternativeIdentifierLabels)
			d['language']="|".join(language)
			d['script']="|".join(script)		
		
		query='select tai.name, ti.name from object_term_relation o join term t on o.term_id=t.id join term_i18n ti on t.id=ti.id join taxonomy_i18n tai on t.taxonomy_id=tai.id where o.object_id=153735 and tai.culture="en";'
		r=self.get_mysql(query,False)
		if r:
			subjectAccessPoints=[]
			placeAccessPoints=[]
			genreAccessPoints=[]
			for line in r:
				if line[0]=='Places':
					placeAccessPoints.append(line[1])
				if line[0]=='Subjects':
					subjectAccessPoints.append(line[1])
				if line[0]=='Genre':
					genreAccessPoints.append(line[1])		
			d['subjectAccessPoints']="|".join(subjectAccessPoints)	
			d['placeAccessPoints']="|".join(placeAccessPoints)	
			d['genreAccessPoints']="|".join(genreAccessPoints)	
		
		query='select  authorized_form_of_name from relation r join actor_i18n ai on r.object_id=ai.id where type_id=161 and r.subject_id='+item+';'
		r=self.get_mysql(query,False)
		if r:
			nameAccessPoints=[]
			for line in r:
				nameAccessPoints.append(line[0])
			d['nameAccessPoints']="|".join(nameAccessPoints)
		
		
		query='select  date,start_date,end_date, description from event e join event_i18n ei on e.id=ei.id where e.type_id=111 and e.object_id='+item+' ;'
		r=self.get_mysql(query,True)
		if r:
			if r[3]:
				d['eventDescriptions']=r[3]
			if r[0]:
				d['eventDates']=r[0]
			if r[1]:
				d['eventStartDates']=r[1]
			if r[2]:
				d['eventEndDates']=r[2]		
			query='select name from term_i18n where id=111 and culture="'+d['culture']+'"';
			rr=r=self.get_mysql(query,True)
			if rr:
				d['eventTypes']=rr[0]	
				
		print(d)
		



				
			
		#| alternate_title | edition | extent_and_medium | archival_history | acquisition | scope_and_content | appraisal | accruals | arrangement | access_conditions | reproduction_conditions | physical_characteristics | finding_aids | location_of_originals | location_of_copies | related_units_of_description | institution_responsible_identifier | rules | sources | revision_history | id     | culture |



			
			
	def _has_child_file(self,l,legyacId):
		"""
		"""
		pass

	def _add_keyword_info(self,l):
		i=0
		
		for e in l:
			if "keywordInfo" not in e:
				e['keywordInfo']=self.predict_item(e['title'],e['scopeAndContent'],False,False,True)
			else:
				if e['keywordInfo']=="":
					e['keywordInfo']=self.predict_item(e['title'],e['scopeAndContent'],False,False,True)
			print (i , " of ", len(l))
			i+=1
		return l.copy()
	
	def _get_entities(self,from_file=False):
		"""
		retrieves all related wikidata items from wikidata
		"""
		entities=[]
		if from_file:
			if os.path.getsize(self.ENTITIES_FILE) > 1:
				with open(self.ENTITIES_FILE, 'r') as file:
					entities = file.read().split('\n')
					return entities
				file.close()
		else:
			query='SELECT DISTINCT ?item \
				where \
				{\
				{optional{?item wdt:P17/wdt:P279* wd:Q329618 .}}\
				 union\
				{optional{?item wdt:P2541/wdt:P279* wd:Q329618 .}}\
				 union\
				{ optional{?item wdt:P131/wdt:P279* wd:Q329618 .}} \
				 union\
				{ optional{?item wdt:P17/wdt:P279* wd:Q329618 .}}\
				 union\
				{ optional{?item wdt:P361/wdt:P279* wd:Q329618 .}}\
				 union\
				{ optional{?item wdt:P2650/wdt:P279* wd:Q329618 .}}\
				 union\
				{ optional{?item wdt:P937/wdt:P279* wd:Q329618 .}}\
				}\
				order by ?item'
			l=self._get_from_WDQ(query)
			if len(l['results']['bindings'])>0:
				for line in l['results']['bindings']:
					entities.append(self._short_entity(line['item']['value']))
			entities.append("Q329618")
			return entities
	
	def _index_keywords(self):
		"""
		Populates the KEYWORDS_FILE with dicts
		"""
		keyword={}
		# open the existing KEYWORDS
		self._open_keywords()
		# open the list of entities
		entities=self._get_entities(False)
		# start to iterate the entity list
		i=len(entities)
		j=0
		for entity in entities:
			print("[[ ",i," ]]")
			i-=1
			j+=1
			if j==10:
				self._store_objects()
				j=0
				print('stored')
			keyword.clear()
			if next((x for x in self.KEYWORDS if x['item']==entity.strip(" ")),None) is None:
				query='SELECT DISTINCT ?item (group_concat(?i;separator="|")  as ?instances)  \
					(group_concat(?d;separator="|")  as ?description)  \
					(group_concat(?short;separator="|")  as ?shortLabel)  \
					(group_concat(?de;separator="|")  as ?label_de) \
					(group_concat(?itemLabel;separator="|")  as ?Label)  \
					(group_concat(?itemAltLabel;separator="|")  as ?altLabel) \
					WHERE {\
					  bind(wd:'+entity+' as ?item).\
					  ?item wdt:P31 ?i.\
					  optional{?item rdfs:label ?itemLabel .}\
						optional{?item schema:description ?d.  FILTER (lang(?d) = "de" )}\
					  optional {?item wdt:P1813 ?short.}\
					  optional{?item skos:altLabel ?itemAltLabel .}\
					  FILTER (lang(?itemLabel) = "de" || lang(?itemLabel) = "en" || lang(?itemLabel) = "fr" ).\
					  bind(if(lang(?itemLabel)="de",?itemLabel,"") as ?de)\
					}\
					group by ?item '
				#l=json.loads(self._get_from_WDQ(query))
				l=self._get_from_WDQ(query)
				#print(l)
				if len(l['results']['bindings'])>0:
					keyword['item']=entity
					keyword['label_de'] = self._get_uniq(l['results']['bindings'][0]['label_de']['value'])[0]
					keyword['description'] = self._get_uniq(l['results']['bindings'][0]['description']['value'])[0]
					keyword['label_clean']=self._clean_label(keyword['label_de'])
					instances=self._get_uniq(l['results']['bindings'][0]['instances']['value'])
					keyword['instances']= [self._short_entity(b) for b in instances]
					keyword['search_terms']=self._get_uniq(l['results']['bindings'][0]['altLabel']['value'])
					keyword['search_terms'].extend(self._get_uniq(l['results']['bindings'][0]['shortLabel']['value']))
					keyword['search_terms'].extend(self._get_uniq(l['results']['bindings'][0]['Label']['value']))
					keyword['search_terms']=list(filter(None, keyword['search_terms']))
					keyword['timestamp']=time.strftime("%Y-%m-%d %H:%M:%S")
					
					# perhaps the WD item don't hav a german label
					if keyword['label_de']=="":
						if len(keyword['search_terms'])>0:
							keyword['label_de']=keyword['search_terms'][0]
						else:
							continue
					
					if keyword['label_clean']!="":
						params = {
							'action': 'wbsearchentities',
							'props':'',
							'language': g.CULTURE,
							'search': keyword['label_clean'],
							'limit':50,
							'format':'json'
						}
				
						url=self.WD_API_ENDPOINT +"?"+ urllib.parse.urlencode(params)
						print(url)
						headers={}
						headers['accept']='application/sparql-results+json'
						req = urllib.request.Request(url, None, headers)
						with urllib.request.urlopen(req, timeout=30) as response:
							the_page = response.read().decode("utf-8")
						r=requests.get(url)
						rjson=json.loads(r.text)
						keyword['frequency']=self._get_frequency(rjson)
					
					self.KEYWORDS.append(keyword.copy())
		
		
		self._store_objects()
		self._generate_identic_clean_labels()

	
	def _generate_identic_clean_labels(self):
		"""
		Counts and stores the number of identic cleaned labels inside KEYWORDS list
		"""
		self._open_keywords()
		for keyword in self.KEYWORDS:
			s=keyword['label_clean']
			keyword['identic_clean_labels']=sum(1 for d in self.KEYWORDS if d.get('label_clean') == s)
		self._store_objects()
	
	
	

	def predict_item(self,item_str, hierarchy_str="", show_stats=False, authority_data=False, return_related_keywords=False):
		"""
		it's the main process to determinate, if an item belongs to the theme or not.
		Returns a score between -1 and 1 where score = 1 means, that the item fits perfectly.
		
		Parameters:
		- item_str : the string to analyse. It should be composed as a concat of title and scope_and_content
		- hierarchy_str : a string which contains all the title and scope_and_content information of parent items
		- show_stats : wheater or not showing the log of the analysis
		- authority_data : True is a list of retrieved search_terms should be returned
		"""
		profession_switcher={
			"BEAMTER":["beamter","richter","jurist","verwalter", "assessor","schriftste","regierung", "gouverneur","leiter","zoll"],
			"MILITAER":["offizier","kommandeur","milit", "hauptmann", "leutnant","major", "ober"],
			"MISSIONAR":["angehöerig", "mission", "theolog"],
			"ANGESTELLTER":["heizer", "maurer","schlosser","angestellte", "hilfe","schreiber", "zeichner"],
			"UNTERNEHMER":["unternehmer","kaufmann","händler", "pionier"],
			"MEDIZINER":["arzt", "mediziner", "bakterio"],
			"WISSENSCHAFTLER":["geograph","reisende","geologe","ethnologe","wissenschaft", "backterio","forsch", "archäolog","biolog","zoolog", "ethnograph"]
			}
			
		instance_factors={
			"Q486972":10,				#settlement
			"Q5":10,						#human
			"Q327333":20,			#authority
			"Q4830453":20,			#enterprise
			"Q20746389":30,			#mission
			"Q45295908":30,			#military
			"Q133156":100,					#Colony
			"Q15815670":50,
			"Q":0					#other
			}
		
		
		item_str += hierarchy_str
		result=[]
		self._open_keywords()
		self._open_killers()
		self._open_names_suffixes()
		lower_item=item_str.lower()
		item_arr=re.findall(r'\w+', lower_item)
		item_arr_str=" "+" ".join(item_arr)+" "
		lower_hierarchy=hierarchy_str.lower()
		hierarchy_arr=re.findall(r'\w+', lower_hierarchy)	
			
		#result["label_suffix_found"]=[]
		#result['clean_label_found']=[] 
		#result["search_terms_found"]=[]
		#result['killers_found']=[]
		#result['clean_label_suffix_found']=[]
		# All the above fields are subdived by | as follows:
		# class of finding|moon|earth (or clean label or label_de)|position (absolute or relative in case if moon exists)|frequency|wd item | instance (as wd item)
		# class of findings:
		# 0 - containts killer words
		# 1 - clean label match (without suffix)
		# 2 - clean label with suffix match
		# 3 - name near family name
		# 4 - label match
		# 5 - label match with suffix near-by
		
		#print (item_arr)
		#print (lower_item)
	
		killers=list(set(self.KILLERS).intersection(item_arr))
		for killer in killers:
			result.append("0||"+killer+"||||Q")
		for keyword in self.KEYWORDS:
			if self._blocked_by_description(keyword['description']):
				continue
			if "frequency" not in keyword:
				keyword["frequency"]=50
			professions=[]
			# full text search
			for search_term in keyword['search_terms']:
				search_term_word_count=len(re.findall("(\S+)", search_term))
				
				if len(search_term)>3:
					if not (search_term_word_count==1 and self._get_instance(keyword)=="Q5"):
						if item_arr_str.find(" "+search_term.lower().replace("-"," ")+" ")>-1:
							result.append("4||"+search_term+"||"+str(keyword['frequency'])+"|"+keyword['item']+"|"+self._get_instance(keyword))
						
							#print("Found fulltext: ", search_term)				
			if len(keyword['label_clean'])>3:
				if item_arr_str.find(" "+keyword['label_clean'].lower()+" ")>-1:
					result.append("1||"+keyword['label_clean']+"||"+str(keyword['frequency'])+"|"+keyword['item']+"|"+self._get_instance(keyword))
					#print("Found clean label: ", keyword['label_clean'])
			# check if human
			if "Q5" in keyword['instances']:
				stopped= keyword['label_de'].replace(keyword['label_clean'],'').strip(" ")
				relative_position= self._is_near(keyword['label_clean'],stopped,item_arr,[1,1])
				if isinstance(relative_position,int):
					result.append("3|"+stopped+"|"+keyword['label_clean']+"|"+str(relative_position)+"|"+str(keyword['frequency'])+"|"+keyword['item']+"|Q5")
				if keyword['label_clean']!="":
					description=keyword["description"].lower()
					
					if self._is_noble(keyword):
						professions.append("ADEL")
					for k,v in profession_switcher.items():
						for snippet in v:
							if description.find(snippet)!=-1:
								professions.append(k)
					if len(professions)>0:
						
						for profession in professions:
							for suffix in self.NAME_SUFFIXES[profession]:
								if item_str.find(suffix.lower() + " " + keyword['label_clean'].lower())>-1:
									result.append("5|"+suffix+"|"+ keyword['label_clean']+"||"+str(keyword['frequency'])+"|"+keyword['item']+"|"+self._get_instance(keyword))
			# near-by search
			if keyword['label_clean']!="":
				# We'd like to have clean labels if they have only on word 
				# e=" ".join(re.findall(r'^\w+$', keyword['label_clean']))
				e=keyword['label_clean']
				
				if len(e)>3:
					s=e.lower()
					label_clean={}
					idx_list=[i for i, x in enumerate(item_arr) if x==s]
					if len(idx_list)>0:
						label_clean=e+"|"+','.join(str(x) for x in idx_list)
						result.append("1||"+label_clean+"|"+str(keyword['frequency'])+"|"+keyword['item']+"|"+self._get_instance(keyword))
						#print ("Found: " , e)
						if "Q5" in keyword['instances']:
							for profession in professions:
								for suffix in self.NAME_SUFFIXES[profession]:
									relative_position=self._is_near(e,suffix,item_arr)
									if isinstance(relative_position,int):
										#print("Found suffix", e,suffix)
										result.append("2|"+suffix+"|"+e+"|"+str(relative_position)+"|"+str(keyword['frequency'])+"|"+keyword['item']+"|Q5")


		#result['search_terms_found']=self._get_uniq(result['search_terms_found'])
		#result['clean_label_found']=self._get_uniq(result['clean_label_found'])
		#result['clean_label_suffix_found']=self._get_uniq(result['clean_label_suffix_found'])
		result=self._get_uniq(result)
		print(json.dumps(result, indent=2))						
		last_object=""
		object_count=0
		score=0
		related_keywords=""
		# decision making
		print(result)
		if len(result)>0:
			if result[0]!="":
				bc=result[0][0:1]
				if bc.isnumeric():
					best_class=int(bc)
				else:
					best_class=1
				worst_class=int(result[len(result)-1][0:1])
				
				score=best_class*100
				if worst_class==0:
					score-=200
				finding_class=best_class
				i=-1
				last_moon_earth=""
				while finding_class==best_class:
					i+=1

					if(i==len(result)):
						break
					
					lst=result[i].split("|")
					#print (lst, len(lst))
					if len(lst)==1:
						score=0
						break
					finding_class=int(lst[0])
					if finding_class!=best_class:
						break

					if last_moon_earth==lst[1]+" "+lst[2]:
						continue
					last_moon_earth=lst[1]+" "+lst[2]
					if lst[1]!="":
						moon=25
					else:
						moon=0				
					if lst[2]!="":
						earth=len(re.findall("(\S+)", lst[2]))*5+len(lst[2])
					else:
						score=0
						break
					if lst[4]!="":
						frequency=int(lst[4])*-1
					else:
						frequency=-100

					if lst[3]!="":
						if lst[1]=="":
							occ_arr=lst[3].split(",")
							occurance=((len(item_arr)-int(occ_arr[0]))/len(item_arr)) *20
							position=0
						else:
							occurance=0
							position= (6 - abs(int(lst[3]))) * 4 
					else:
						position=0
						occurance=0
					
					if lst[5]!=last_object:
						object_count+=1
					
					if lst[6]!="":
						instance_factor=instance_factors[lst[6]]
					else:
						instance_factor=1
							
					score+=frequency +occurance + position +earth + moon + (object_count * 30 * best_class)
					print ("score+=frequency +occurance + position +earth+moon +object_count")
					print (score,"|",frequency ,occurance , position ,earth,moon, object_count)
				else:
					score=0
		else:
			score=0
		print("-------------------------------------")
		
		#print (item_str, score)
		if authority_data:
			r_list=[]
			if len(result)>0:
				if result[0]!="":
					for e in result:
						lst=e.split("|")
						if int(lst[0])>2:
							r=next((item for item in self.KEYWORDS if item["item"] == lst[5]), False)
							if r:
								if 'label_de' in r:
									if self._get_access_point( lst[6]) :
										if (r['label_de'],self._get_access_point( lst[6])) not in r_list:
											r_list.append((r['label_de'],self._get_access_point( lst[6])))
			return r_list
						
		
		elif return_related_keywords:
			#print("related keywords")
			releated_keywords=""			
			for r in result:
				lst=r.split("|")
	
				if lst[0].isnumeric():
					print (lst,result, int(lst[0]), lst[5])
					if int(lst[0])>1:
						kw=next((x for x in self.KEYWORDS if x['item']==lst[5]),False)
						#print(kw)
						if kw:
							if "description" in kw:
								related_keywords+=lst[0]+"  " + kw['label_de'] + " ("+kw['description'] + ")\n"
							else:
								related_keywords+=lst[0]+"  " + kw['label_de'] +"\n"
					else:
						print("below 2")
				
			return related_keywords
		else:
			if score >300:
				return True
			else:
				return False

	def _create_slug(self,phrase):
		"""
		Creates a new slug for a specific phrase,checks existing slug with the same phrase on AtoM mysql
		"""
		phrase=slugify(phrase)
		sql='select slug from slug where slug = "'+phrase+'" or slug like "'+phrase+'-_" or slug like "'+phrase+'-__" or slug like "'+phrase+'-___" order by slug;'
		result_sql=self.get_mysql(sql,False)
		best=0
		if result_sql:
			for result in result_sql:
				i=result[0][result[0].rfind("-")+1:]
				if i.isnumeric():
					if int(i)>best:
						best=int(i)
		else:
			return(phrase)
		return phrase +"-"+ str(best+1)
		
				
					
				
		
		
	
	def _get_access_point(self,wdid):
		"""
		returns the AtoM event type of an WD item 
		
		Parameter:
		wdid : The Wikidata ID of the keyword
		"""
		if wdid!="":
			return g.ACCESS_POINTS[self._get_instance("",wdid)]
		else:
			return False
		
	def _get_level_of_description(self,htype):
		htype=htype.capitalize()
		if htype in self.LEVELS_OF_DESCRIPTION:
			return self.LEVELS_OF_DESCRIPTION[htype]
		else:
			return htype	

		
	def _is_noble(self,keyword):
		is_noble=False
		arr=re.findall(r'\w+', keyword['label_de'])
		for e in keyword['search_terms']:
			arr.extend(re.findall(r'\w+', e))
		for e in arr:
			if e in ['von','zu']:
				return True
		return False
	
	def _is_near(self,earth,moon, lst,distance=[6,5] , position_statement=True):
		"""
		Checks if a string (moon) is around another string (earth) within a list of strings.
		
		"""
		
		earth=earth.lower()
		moon=moon.lower()

		#print (earth,moon)
		for i in range(0,len(lst)):
			e=lst[i]
			if e==earth:
				sub=[i-distance[0],i+distance[1]]
				if sub[0]<0:
					sub[0]=0
				if moon in lst[sub[0]:sub[1]]:
					if position_statement:
						for j in range(sub[0],sub[1]):
							if lst[j]==moon:
								return i-j
				
					else:
						return "True"
		return "False"
				
	def _get_instance(self,keyword="",s=""):
		"""
		returns the main instance of a keyword
		
		Parameters:
		keyword : if set it represents an object of the self.KEYWORDS list
		s: if set it represents a string with a wdid 
		"""
		
		if keyword:
			lst=keyword['instances']
		else:
			lst=[s]	
		
		if len(list(set(lst).intersection(["Q5"])))>0:
			return "Q5"  #Human
		if len(list(set(lst).intersection(["Q20746389","Q1564373"])))>0:	
			return "Q20746389"    # Mission
		if len(list(set(lst).intersection(["Q4830453"])))>0:
			return "Q4830453"  #Enterprise
		if len(list(set(lst).intersection(["Q327333"])))>0:	
			return "Q327333"    # Authority	
		if len(list(set(lst).intersection(["Q45295908"])))>0:	
			return "Q45295908"    # Military	
		if len(list(set(lst).intersection(["Q486972","Q3024240","Q164142"])))>0:	
			return "Q486972"    # Human Settlement	
		if len(list(set(lst).intersection(["Q15815670"])))>0:	
			return "Q15815670"    # Event				
		if len(list(set(lst).intersection(["Q133156"])))>0:	
			return "Q133156"    # colony		
		return ""
			
	
	
	def _get_wd_from_search_term(self,term,strict=True):
		"""
		returns a list of self.KEYWORDS elements matching term
		"""
		self._open_keywords()
		result=[]
		term_arr=arr=re.findall(r'\w+', term)
		for keyword in self.KEYWORDS:
			for search_term in keyword['search_terms']:
				search_term_arr=re.findall(r'\w+', search_term)
				#print(search_term_arr)
				if len(set(term_arr).intersection(search_term_arr))==len(term_arr):
					if strict:
						if len(term_arr)==len(search_term_arr):
							result.append(keyword)
					else:
						result.append(keyword)
		return result	
	
	
	def search_term_generator(self,from_term="",without_frequent_names=True):
		"""
		is a generator for search terms to use when query external databases
		
		Parameters:
		from_term : Skip all term which come before from_term in keywords.json
		"""
		
		tmp=[]
		print("OK, we will start from : " + from_term)
		skip=False
		if from_term!="":
			skip=True
		self._open_keywords()
		for keyword in self.KEYWORDS:
			if keyword['label_clean']=="":
				continue
			if self._blocked_by_description(keyword['description']):
				continue	
			if without_frequent_names and self.is_frequent_name(keyword['label_de']):
				continue
					
			for item in keyword["search_terms"]:
				if item==from_term:
					skip=False
				if skip:
					print("skipping ", item)
					continue
				if without_frequent_names and not self.is_frequent_name(keyword['label_de']):
					if item.find(" ")==-1 and self._get_instance(keyword) =="Q5":
						pass
					else:
						tmp.append((self._reorder_name(item,self._get_instance(keyword)), self._get_instance(keyword)))
						tmp.append((item, self._get_instance(keyword)))
				else:
					tmp.append( (item, self._get_instance(keyword)))
				
			if keyword['label_clean']==from_term:
				skip=False
			if skip:
				print("skipping ", keyword['label_clean'])
				continue
			if without_frequent_names and not self.is_frequent_name(keyword['label_de']):
				if keyword['label_clean'].find(" ")==-1 and self._get_instance(keyword) =="Q5":
					pass
				else:
					tmp.append((keyword['label_clean'],self._get_instance(keyword)))
					tmp.append((self._reorder_name(keyword['label_clean'],self._get_instance(keyword)), self._get_instance(keyword)))
			else:
				tmp.append( (keyword['label_clean'],self._get_instance(keyword)))
		tmp=list(set(tmp))
		tmp.sort(key=lambda x: x[0])
		for e in tmp:
			yield e
			
			
	
	def _blocked_by_description(self,s):
		blocker=["eisenbahnverwaltung","schreiber","heizer", "polizei", "polizist"]
		for block_term in blocker:
			if s.lower().find(block_term)>-1:
				return True
		return False
	
	
	def _reorder_name(self,s,wd_type="Q5"):
		
		if wd_type=="Q5":
			arr=re.findall(r'\w+', s)
			if len(arr)>1:
				out=arr[len(arr)-1]+","
				for i in range(0,len(arr)-1):
					out+=" " + arr[i]
				
				print(out)
				return out
				
			else:
				return s
		else:
			#print (wd_type, " is not Q5")
			return s
		
	
	def imports(self, source,record_type="archival_description", sourcefile='', fromfile='', update=True, from_term="", source_str=""):
		""" 
		
		Imports records into the candidate index.
		
		Parameters:
		- source : One of APE, EADXML, DDB , NDB, KAL..
		- record_type : ElasticSearch _type. Ether 'archival_description' (standard), 'authority_record'
		- sourcefile : In case of source = EADXML, this indicates the relative file path to the source XML file
		- fromfile : If not empty, the retrieval of data from external web sites will be skipped and and a former created JSON file used. 
		             (Useful in case of frequent interruptions)
		- source_str: url of the metadata source
		
		Writes the imported data into tmp/export2es.json.
		Returns the number of retrieved records
		"""
		err=[]
		self._open_out_files()
		self._init_legacy_ids()
		print("We will start from : " + from_term)
		#try:
		while True:
			l={}
			err=''
			if fromfile=='':
				if source == 'APE':
					proc=ape.ape()
					#l=proc.export(0,False,update)
				if source == 'EADXML':
					from atom.imports import eadxml
					proc=eadxml.eadxml(sourcefile, source_str)
					
				if source == 'NDB':
					proc=ndb.ndb()
					#l=proc.export(0)
				if source == 'DDB':
					from atom.imports import ddb
					proc=ddb.Ddb()
				if source =='KAL':
					proc=Kalliope()
				
				for export in proc.export(g.WD_MIN_EXPORT_LIST_LEN,from_term):
					l=export
					print("länge export",len(l))
					self._add_to_current_identifiers(l)
					print("EXPORTED ------------------------------------------------------------------------------------------")
					#pprint.pprint(l)
					
					pa=self.TMP_RESULTS_PATH+time.strftime("%Y%m%d_%H%M%S")
					pathlib.Path(pa).mkdir(parents=True, exist_ok=True) 
					self._post_import(l,pa)
					
					self.write_csv(l,pa+"/import.csv",record_type)
					
				# write out to json
				#with open('tmp/export2es.json', 'w', encoding='UTF-8') as json_out:
					#dump = json.dumps(l, sort_keys=True, indent=4, ensure_ascii=False)
					#json_out.write(dump)
				# close the file
				#json_out.close
			else:
				
				with open(fromfile,'r') as json_in:
					l = json.load(json_in)
					
					print(len(l))
				json_in.close
				
			#self.write_to_index(l, record_type, source)
			
			
			print (err)
			return len(l)
		#except:
		else:
			print("Error:", sys.exc_info()[0])
			return
			
	def _init_legacy_ids(self):
		"""
		fills the self.LEGACY_IDS with id from the stored csv files at begin of th programm execution
		"""
		if len(self.LEGACY_IDS)==0:
			path = self.TMP_RESULTS_PATH
			for root in os.walk(path):
				print (root[0])
				print ("/import.csv")
				if root[0]==self.TMP_RESULTS_PATH:
					continue
				if  os.path.isfile(root[0]+"/import.csv") :
					print(root[0]+"/import.csv")
					l=fo.load_data(root[0]+"/import.csv")
					for e in l:
						if 'legacyId' in e:
							self.LEGACY_IDS.append(e['legacyId'])
						else:
							print("No legacyId for init")
				
	def _add_to_legacy_ids(self,legacyId):
		if legacyId not in self.LEGACY_IDS:
			self.LEGACY_IDS.append(legacyId)
	
	
	def _post_import(self,l,pname="", sort=True,add_keywords=True, create_parent_entries=False):
		"""
		checks the integrity of the imported data. 
		It manages also the acces points. Original accesspoint will be logged and replecaed by self.KEYWORDS
		
		Parameter
		l: a list of dict which represents the AtoM shema for csv imports
		pname:
		sort:
		add_keywords: True if default keywords should replaces by keywords from the own keyword list
		create_parent_entries: True if parent items should be created if not present. This feature tries to use the ISIL from INSTITUTION as well as the order of arrangement
		"""
		ap_log=""
		added_ap_log=""
		errors_log=""
		levels={}
		match_tuples=[]
		self._init_legacy_ids()
		tree="\nTREE VIEW\n=================================="
		rank={"Fonds":4,
			"Class":5,
			"Series":6,
			"Subseries":7,
			"File":8,
			"Item":9,
			"Collection":3,
			"Tektonik_collection":2,
			"Institution":1,
			"?":1,
			" ":1,
			"":1,
			}
		
		
		
		if sort:
			self.hierarchy_sort(l)[0]     
		for e in l:
			for fieldname in self.ARCHIVAL_DESCRIPTIONS_FIELDS:
				if fieldname not in e:
					e[fieldname]=""
				if not e[fieldname]:
					e[fieldname]=""	
			
			indent=rank[self._get_level_of_description(e['levelOfDescription'])]
			tree+=(indent*"  "+e['legacyId'])
			
			
			if e['parentId']!="":
				match_tuple=self.is_in_atom(e['parentId'],True,True)
				if match_tuple:
					if match_tuples not in match_tuples:
						match_tuples.append(match_tuple)
				
			if e['levelOfDescription'] in levels:
				levels[e['levelOfDescription']]+=1
			else:
				levels[e['levelOfDescription']]=1
			if e['title'] =="":
				errors_log += "Item  " + e['legacyId'] + " don't has a title!\n"
			tree+="    "+e['title']+"\n"

			if e['parentId']!="":
				if e['parentId'] not in self.LEGACY_IDS and not self.is_in_atom(e['parentId']):
					if create_parent_entries:
						r=self._create_parents(e,l)
						if r:
							l.extend(r[0])
							e['parentId']=r[1]['parentId']
							errors_log +="New parents created for item " + e['legacyId']+"\n"
					else:
						errors_log += "Parent " + e['parentId'] + " is unknown! Level of description is " + e['levelOfDescription'] + " Arrangement:" + e['arrangement']+"\n"
			else:
				if create_parent_entries:
					r=self._create_parents(e,l)
					
					if r:
						l.extend(r[0])
						e['parentId']=r[1]['parentId']
						errors_log +="New parents created for item "+ e['legacyId']+"\n"
				else:
					errors_log += "Item " + e['legacyId'] + " has no parent! Level of description is " + e['levelOfDescription'] + " Arrangement:" + e['arrangement']+"\n"
			if e['legacyId'] not in self.LEGACY_IDS and e['legacyId']!="":
				self.LEGACY_IDS.append(e['legacyId'])
			else:
				errors_log += "Item " + e['legacyId'] + " is a duplicate!\n"
			if e['legacyId']=="":
				errors_log += "Item " + e['title'] + " has no legacyId!\n"
			
			if add_keywords:
				if pname!="":
					for ap in ["subjectAccessPoints","placeAccessPoints","nameAccessPoints"]:
						if ap in e:
							if e[ap] !="":
								tmp=e[ap].split("|")
								e[ap]=""
								for t in tmp:
									ap_log+=ap+"\t"+ e['legacyId'] + "\t" + t+"\n"
					if e['scopeAndContent']!="":
						#e['scopeAndContent']= re.sub('<[^>]*>',' ',e['scopeAndContent']) # not sure
						e['scopeAndContent']=e['scopeAndContent'].replace("<br />", " ")

						scope=e['scopeAndContent']
					else:
						scope=""
					ap_list=self.predict_item(e['title'] + " " + scope,"",False,True)
					for keyword in ap_list:
						added_ap_log+=ap+"\t"+ e['legacyId'] + "\t" + keyword[0]+"\tadded to\t"+e['title']+"\n"
						if keyword[1] in e:
							if e[keyword[1]]=="":
								e[keyword[1]]=keyword[0]
							else:
								e[keyword[1]]+="|"+keyword[0]
						else:
							e[keyword[1]]=keyword[0]
			
		self.hierarchy_sort(l)	
		# removing duplicates
		r=[]
		seen=set()
		for e in l:
			 if e['legacyId'] not in seen:
				 r.append(e)
				 seen.add(e['legacyId'])
		l=r

		
		#if pname=="":
		#	fname=time.strftime("%Y%m%d_%H%M%S")+".log"
		print(pname)
		if pname!="":
			#pname+=self.TMP_RESULTS_PATH
			pname+="/"
		else:
			pname=self.TMP_RESULTS_PATH
		with open(pname+"import_preparation.log",'w') as f:
			f.write ("LOG FILE FOR "+pname + "/import.csv")
			f.write (str(len(l)))
			f.write ("\n----------------------\n\n")
			f.write(json.dumps(levels,indent=2) + "\n\n")
			f.write ( errors_log+ "\n\n" + ap_log)
			f.write ("\n----------------------\n\n" + added_ap_log)
			f.write(tree)
		f.close()
		return l
				
				
	def _create_parents(self,e,l):
		"""
		Tries to create parent items up to institution level if not provided
		Returns a list of parent items
		
		"""	
		hasArrangement=False	
		hasIsil=False
		hasParentId=False	
		r_list=[]		
		if e['levelOfDescription'] in ['Institution','Collection','Tektonik_collection']:
			return None
		l=[]
		arrangement_arr=e['arrangement'].split(" - ")  # looking for a tree in arrangement
		if len(arrangement_arr)>1:
			hasArrangement=True
		isil=self.get_isil(e['repository'])
		if isil:
			hasIsil=True
		if e['parentId']!="":
			hasParentId=True
		
		# Case: No arrangement but ISIL. We just add institution item if not exist already 
		if hasIsil and not hasArrangement:
			query='select id from repository where identifier="'+isil+'";'

		# Case: No arrangement and no ParentID. We just create a collection below the top level description
		
		parentId=self.get_top_archival_legacy_id_by_institution_name(e['repository'])
		if not parentId:    
			d=self.create_empty_dict(e)
			if hasIsil:
				d['legacyId']=isil
			else:
				d['legacyId']=str.replace(e['repository']," ", "_").lower()
			d['levelOfDescription']="Institution"
			d['culture']="de"
			d['title']=e['repository']
			d['repository']=e['repository']
			e['parentId']=d['legacyId']
			element=next((x for x in l if x['legacyId']==d['legacyId']),None)
			parentId=d['legacyId']
			if not element:
				r_list.append(d.copy())
			
		d=self.create_empty_dict(e)
		d['parentId']=parentId
		d['legacyId']=parentId+"_Collection"
		if e['arrangement'].find("Autographensammlung")>-1:
			d['title']="Autographensammlung"
		else:
			d['title']="Sonstige"
		
		d['levelOfDescription']="Collection"
		d['repository']=e['repository']
		e['parentId']=d['legacyId']
		element=next((x for x in l if x['legacyId']==d['legacyId']),None)
		if not element:
			r_list.append(d.copy())
		if len(r_list)>0:
			return (r_list,e)
		else:
			return False

	def create_empty_dict(self,e):
		d=e.copy()
		for k in e.keys():
			d[k]=""
		
		d['culture']=e['culture']
		return d.copy()


	def get_isil(self,institution):
		"""
		retrieves the ISIL from institution name
		"""
		self._open_institutions()
		institution=institution.lower()
		e=next((x for x in self.INSTITUTIONS if x['itemLabel'].lower()==institution),False)
		if e:
			if 'isil' in e:
				return e['isil']
		return False	


	def get_isil_from_repository(self,institution):
		"""
		retrieves the isil from a repository which is already know by AtoM
		"""
		sql='select a.id, authorized_form_of_name, r.identifier from actor_i18n a join repository_i18n ri on a.id=ri.id join repository r on r.id=ri.id where authorized_form_of_name like"%'+institution+'%";'
		r=self.get_mysql(sql,True,False)
		if r:
			if r[2]:
				return r[2]
		return False

	def get_top_archival_legacy_id_by_institution_name(self,institution):
		"""
		returns the first top level description id from an institution if it is a Collection, Fond or Tektonik-Collection
		
		Parameter:
		institution : name of the institution (case sensitive)
		"""
		sql='select a.id, authorized_form_of_name, r.identifier,ti.name, i.id, k.source_id \
		from actor_i18n a join repository_i18n ri on a.id=ri.id \
		join repository r on r.id=ri.id \
		join information_object i on a.id=i.repository_id \
		join term_i18n ti on i.level_of_description_id=ti.id \
		join keymap k on k.target_id=i.id \
		where authorized_form_of_name like"%'+institution+'%" and i.parent_id=1 and ti.culture="de" ;'
		r=self.get_mysql(sql,False,False)
		if r:
			for line in r:
				if line[3] in ['Institution','Collection','Fonds','Tektonik_collection']:
					return line[5]
		return False

		

	
	def _add_to_current_identifiers(self,l):
		for e in l:
			if e['legacyId'] not in self.CURRENT_DESCRIPTION_IDENTIFIERS:
				self.CURRENT_DESCRIPTION_IDENTIFIERS.append(e['legacyId'])
				
			
	
	"""
	def write_to_index(self,l,record_type, source):
		for e in l:
				e=self.flat(e)
				print(e)
				print('--------------------------------------------------------------------------')
				# this because AtoM 2.4. changed the ISO code for 'language' from 639-3 to 639-2
				if 'language' in e:
					if 'language'!='':
						e['language']=self.iso639_3to2(e['language'])
				
				
				
				if 'legacyId' in e:
					e=self.prepare_json(e)
					r=self.id_in_index(e["legacyId"],'_id',source,self.indexstore,record_type)
					if not r:
						#print(e)
						
						self.es.index(index=self.indexstore, doc_type=record_type,id=str(e["legacyId"]), body=json.dumps(self.drop_empty_eventdates(e)))
					else:
						d=self.merge(r,e)  
						
						if d:
							#self.es.delete(index=self.indexstore,doc_type=record_type,id=e["legacyId"])
							self.es.index(index=self.indexstore,doc_type=record_type,id=e["legacyId"], body=json.dumps(self.drop_empty_eventdates(d)))
				else:
					if 'title' in e:
						err+='Record titled '+ e['title'] + ' has no LegacyId \n' + json.dumps(e, sort_keys=True, indent=4, ensure_ascii=False)
						
					else:
						err+='unknown record skipped'
			

	"""

	def export(self,record_type='archival_description'):
		"""
		Takes confirmed records from candidates index to AtoM csv import format
		
		Marks taken records as exported		
		Returns a file tmp/export.csv
		
		"""
		
		#d=self.es.search(index="candidates", doc_type=record_type, body={"size":200000,"query": {"term": {"_status":"confirmed"}}})
		d=self.es.search(index="candidates", doc_type=record_type, body={"size":200000,"query": {"match": {"_original_from":"DDB"}}})
		l=[]
			
		for hit in d['hits']['hits']:
			e=hit['_source']
			if True:    # Test
				if not 'culture' in e:
					e['culture']=CULTURE	
				if 'acquisition' in e:
					print (e['acquisition'])
					acquisition=e['acquisition']
					parent_list=self.get_parents(e['legacyId'])	

					for ee in parent_list:
						if not 'acquisition' in ee:
							self.es.update(index='candidates',doc_type=record_type,id=ee['legacyId'],body={"doc": {"acquisition": acquisition }})  
							parent_element=next((item for item in d['hits']['hits'] if item['_id'] == ee['legacyId']), None)
							parent_element['acquisition']=acquisition
							if 'levelOfDescription' in parent_element:
								if parent_element['levelOfDescription']=='Fonds':
									break
				
			l.append(e)
			
			self.es.update(index='candidates',doc_type=record_type,id=e['legacyId'],body={"doc": {"_status": "exported" }})  
		self.hierarchy_sort(l)[0]
		
		count=0
		
		self.write_csv(l,'tmp/export.csv',record_type)
		
		self.check_exported(record_type)
		
		self.split_export_files(record_type)

		return (len(l))	
	
	
	def strip_tags(self,culture="de"):
		print("start stripping tags")
		sql="select scope_and_content,id from information_object_i18n where culture='"+culture+"'; "
		l=self.get_mysql(sql,False)
		print(len(l), " records")
		i=0
		for e in l:
			if not(e[0] is None):
				i+=1
				text=str(e[0])
				new_text=so.stripBr(text)
				new_text=so.stripHtml(new_text," ")
				new_text=so.dedupSpaces(new_text)
				new_text=so.replaceChars(new_text,"'","’")
				new_text=so.replaceChars(new_text,"","")
				if new_text != text:
					
					sql="update information_object_i18n set scope_and_content ='"+new_text+"' where id="+str(e[1])+" and culture='"+culture+"';"
					print(sql)
					a=self.get_mysql(str(sql),False,True)
		print (i , " records modified")
	
	
	def write_csv(self,l,filename, record_type):
		"""
		Writes a list of dict into a csv file depending on record_type
		
		Parameter:
		filename: relative path and file name of the csv file to create
		record_type: either archival_descripition or authority_record
		
		"""
		with open(filename, 'w') as csvfile:
			if record_type=='archival_description':
				fieldnames = ['out','legacyId','parentId','qubitParentSlug','identifier','accessionNumber','title','levelOfDescription','extentAndMedium','repository','archivalHistory','acquisition','scopeAndContent','appraisal','accruals','arrangement','accessConditions','reproductionConditions','language','script','languageNote','physicalCharacteristics','findingAids','locationOfOriginals','locationOfCopies','relatedUnitsOfDescription','publicationNote','digitalObjectURI','generalNote','subjectAccessPoints','placeAccessPoints','nameAccessPoints','genreAccessPoints','descriptionIdentifier','institutionIdentifier','rules','descriptionStatus','levelOfDetail','revisionHistory','languageOfDescription','scriptOfDescription','sources','archivistNote','publicationStatus','physicalObjectName','physicalObjectLocation','physicalObjectType','alternativeIdentifiers','alternativeIdentifierLabels','eventDates','eventTypes','eventStartDates','eventEndDates','eventDescriptions','eventActors','eventActorHistories','eventPlaces','culture','status']
				fieldnames = self.ARCHIVAL_DESCRIPTIONS_FIELDS
			if record_type=='authority_record':
				fieldnames = ['culture', 'typeOfEntity', 'authorizedFormOfName', 'corporateBodyIdentifiers', 'datesOfExistence', 'history', 'places', 'legalStatus', 'functions', 'mandates', 'internalStructures', 'generalContext', 'descriptionIdentifier', 'institutionIdentifier', 'rules', 'status', 'levelOfDetail', 'revisionHistory', 'sources', 'maintenanceNotes']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames,extrasaction='ignore', delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for e in l:
				writer.writerow(e)

		csvfile.close()
		
	def read_csv(self,filename):
		"""
		depriciated
		"""
		with open(filename) as csvfile:
			reader = csv.DictReader(csvfile)
			meta=[]
			l = [row for row in reader]
		return l.copy()
	
	def split_export_files(self,record_type,include_acquisition=False):
		"""
		This reopens the export.csv and creates export csv files for each combination found of repository and acquisition
		"""
		with open('tmp/export.csv') as csvfile:
			reader = csv.DictReader(csvfile)
			meta=[]
			l = [row for row in reader]
		if record_type=='archival_description':
			directory='tmp/export/archival_description/'
			for e in l:
				if 'acquisition'  in e and include_acquisition:
					ee=self.in_meta(meta,e['repository'], e['acquisition'])
				else:
					ee=self.in_meta(meta,e['repository'])
				if ee:
					print ("-->" + ee['repository'])
					if 'elements' in ee:
						ee['elements'].append(e.copy())
					else:
						ee['elements']=[]
						ee['elements'].append(e.copy())
				else:
					d={}
					d['repository']=e['repository']
					if 'acquisition' in e and include_acquisition:
						d['acquisition']=e['acquisition']
					d['elements']=[]
					d['elements'].append(e.copy())
					meta.append(d.copy())
					d.clear()
		for e in meta:
			if 'acquisition' in e and include_acquisition:
				filename=''.join(el for el in e['repository'].capitalize() if el.isalnum()) + "__" + ''.join(el for el in e['acquisition'].capitalize() if el.isalnum()) + ".csv"
			else:
				filename=''.join(el for el in e['repository'].capitalize() if el.isalnum()) + ".csv"
			self.write_csv(e['elements'],directory+filename, record_type)
			print(filename)
		csvfile.close()
		return
		
		
		

	def in_meta(self,meta,repository,acquisition=None):
		"""
		This helps split_export_files()
		"""
		print (repository)
		print (acquisition)
		for ee in meta:
			if ee['repository']==repository:
				if acquisition:
					if ee['acquisition']==acquisition:
						return ee
				else:
					return ee
		return None
				
	
	
	
	def OLDcheck_splited_files(self):
		"""
		Checks splites csv files if respecting hierachies
		"""
		dir_str='tmp/export/archival_description/'
		directory = os.fsencode(dir_str)
		for filename in os.listdir(directory):
			fn=filename.decode()
			with open(dir_str+fn) as csvfile:
				reader = csv.DictReader(csvfile)
				meta=[]
				l = [row for row in reader]
			print('~~~~~ ' + fn)
			
			fneu=fn.strip('.')
			if self.quality_check(l)>0:
				os.rename(dir_str+fn, dir_str+'.'+fneu)
				print('not OK \n')
			else:
				os.rename(dir_str+fn, dir_str+fneu)
				print('OK\n')
			csvfile.close()
	
	def sort_file(self,filename,record_type):
		l=fo.load_data(filename)
		self.hierarchy_sort(l)[0]
		self.write_csv(l,filename,record_type)
		
	
	def OLDcheck_exported(self, record_type):
		"""
		1) Checks if legacyId of exported records already exists in atom mysql.
		2) Checks if there are parents ether among the exported records or inside the database
		3) Informs about new eventActors
		"""
		
		with open('tmp/export.csv') as csvfile:
			reader = csv.DictReader(csvfile)
			lc=[]
			lcc=[]
			
			l = [row for row in reader]
		for e in l:
			problem_count={'None':0,'Empty':0,'not in stack':0, 'not in sql':0,'changed':0}
			q='select t2.id from keymap t1  left join information_object t2 on t1.target_id=t2.id where t1.source_id="' + e['legacyId'] + '";'
			answer=self.get_mysql(q)
			if answer:
				print(e['legacyId'] + " already exist in atom")
				
			else:
				#print(e['legacyId'] + " doesn't exist in atom")
				lc.append(e)
		
		if record_type=='archival_description':
			
			for i in range (0, len(lc)-1):
				problem=False
				if lc[i]['levelOfDescription']=='File' or lc[i]['levelOfDescription']=='Item':
					if 'parentId' not in lc[i]:
						problem=True
						problem_count['None']+=1
					elif lc[i]['parentId']=='':
						problem=True
						problem_count['Empty']+=1
					else:
						p=self.get_parent(lc,lc[i]['parentId'])
						if p > -1:
							problem=False
						else:
							problem=True
							problem_count['not in stack']+=1
						if problem:
							q='select t2.id from keymap t1  left join information_object t2 on t1.target_id=t2.id where t1.source_id="' + lc[i]['parentId'] + '";'
							answer=self.get_mysql(q)
							print(q)
							print(answer)
							if answer:
								problem=False
							else:
								problem_count['not in sql']+=1
				if problem:
					problem_count['changed']+=1
					self.es.update(index='candidates',doc_type=record_type,id=lc[i]['legacyId'],body={"doc": {"_status": "orphan"}})
					print(lc[i]['legacyId'] + " is orphan")
				else:
					lcc.append(lc[i])
			self.write_csv(lcc,'tmp/export.csv',record_type)
			eventActors=[]
			for e in lcc:
				if 'eventActors' in lc:
					if e['eventActors'] not in eventActors:
						eventActors.append(e['eventActors'])
			print (eventActors)
		else:
			self.write_csv(lc,'tmp/export.csv', record_type)
			print("problem_count ---> " )
			print (problem_count)
		
		return
	
		
	def index_update(self,search_query, update_query, indexstore='candidates', record_type='archival_description', do_it=True):
		"""
		Retrieves records from index and update them one by one partially.
		
		Parameters:
		- search_query : a valid ElasticSearch search query used to retreive specific recods from index (json dump str)
		- update_query : a valid ElasticSearch statement used for update one or more fields of the records (son dump str)
		- indexstore: the ElasticSearch -type element. Standard is 'archival_description'
		- do_it: False if only searching
		
		
		Returns number of hits, if just searching, or search result if updating
		"""

		d=self.es.search(index=indexstore, doc_type=record_type, body=search_query)
		l=d['hits']['hits']
		print(d)
		if do_it:
			for e in l:
				self.es.update(index=indexstore,doc_type=record_type,id=e['_id'],body=update_query)
		else:
			pass
			return d['hits']['total']
		return d
		
		



	def get_jump_fonds(self):
		"""
		Returns a list of Fonds which are already included completly 
		"""
		return self.jump_fonds
		
			
	def drop_empty_eventdates(self,d):
		"""
		A work around for a ElasticSearch bug.
		
		Due to a bug (?) in elasticsearch.py eventStartDates or eventEndDates must be filled with a date string.
		Search will not working if both are empty. So this workaround will eliminate those if emty
		
		Parameter:
		- d: record dict
		
		"""
		if 'eventStartDates' in d:
			if d['eventStartDates']=="":
				d.pop('eventStartDates',None)
		if 'eventEndDates' in d:
			if d['eventEndDates']=="":
				d.pop('eventEndDates',None)
		return d			
	
	def lookup_orphans(self, record_type='archival_description', source='DDB'):
		"""
		retrieve orphans from the candidate index depending on their source
		"""
		d=self.es.search(index="candidates", doc_type=record_type, body={"size":self.max_results,"query": {"bool": {"must": [ {"match": { "_status": "orphan" }},{ "match": {"_original_from": source }}]}}})
		#print(d['hits']['total'])
		return d['hits']['hits']
	
	#def id_in_index(self, legacy_id, source='APE', indexstore='candidates', doc_type='archival_description', field='_id'):
	def id_in_index(self, legacy_id,field='_id', source='APE', indexstore='candidates', record_type='archival_description'):
		"""
		Returns the record source data if a record with given Id was found in the index, else None
		
		Parameters:
		- legacy_id : AtoM's legacyId for the record
		- source: One of APE,APEXML, DDB, etc
		- indexstore: The ElasticSearch index. Standard is 'candidates'
		- doc_type: The ElasticSearch document type
		"""
		
		d=self.es.search(index=indexstore, doc_type=record_type, body={"size":self.max_results,"query": {"match": {field:legacy_id}}})
		if d['hits']['total']>0:
			return d['hits']['hits'][0]['_source']
		else:
			return None
	
	
	def prepare_json(self,d):
		"""
		Cleans the record dict, trimming '|' caracters, add culture CULTURE if void
		"""
		if 'culture'in d:
			if d['culture']=='':
				d['culture']=CULTURE
		else:
			d['culture']=CULTURE
				
		for key, value in d.items():
			if isinstance(value,str):
				
				#if value.find('|',0,1)>-1:
					#d[key]=value[1:]
				if value[-1:]== '|': 
					d[key] = value[:-1]
				if value[:1]=='|' : 
					d[key]=value[1:]
		return d.copy()
	
	
	def iso639_3to2(self,lang639_3):
		"""
		Work around due to change of the 'language' field in AtoM from ISO 639-3 to ISO 639-2
		
		Parameter:
		- lang639_3 : The ISO 639-3 code of a given language
		"""
		if not lang639_3:
			return
		if len(lang639_3)==2:
			return lang639_3
		
		d={'ger':'de','cze':'cs','chi':'zh','dut':'nl','gre':'el','per':'fa','fre':'fr'}
		if lang639_3 in d:
			return d[lang639_3]
		else:
			try:
				lang=pycountry.languages.get(alpha_3=lang639_3)
				return lang.alpha_2
			except:
				print('There is no ISO 639-3 code for language %s ', lang639_3 )
				return ""
		
	def OLDevaluate_candidates(self, record_type='archival_description'):
		"""
		Evaluation and relevance check of import candidates.
		
		Not yet automatic...
		
		After manuel confirmation of relevance, all parents will be also be confirmed
		
		"""
		d=self.es.search(index="candidates", doc_type=record_type, body={"size":200000,"query": {"term": {"_status":"candidate"}}})
		l=[]	
		print('HITS: ' + str(d['hits']['total']))
		for i,e in enumerate(d['hits']['hits']):
			if not ('is_parent' or 'is_child') in e['_source']:
				#print(json.dumps(e['_source'], sort_keys=True, indent=4, ensure_ascii=False))
				print('===========================================================================================================')
				#print(e)
				for k,v in e['_source'].items():
					if v:
						print(k,'\t.......\t',v)
				print('===========================================================================================================')
				print(str(i+1) + ' of ' + str(len(d['hits']['hits'])))
				ok=True
				while ok==True:
					a=input("(c)onfirm - (r)efuse - k(e)ep : ")
					ok=False
					if a=='c':
						status='confirmed'
						print(str(self.confirm_parents(e['_id']))+ " parents confirmed")
					elif a=='r':
						status='refused'
						print(str(self.confirm_parents(e['_id'],'archival_description','candidates','refused'))+ " parents refused")
					elif a== 'k':
						status='candidate'
					else:
						ok=True
			
				self.es.update(index='candidates',doc_type=record_type,id=e['_id'],body={"doc": {"_status": status }})
			else:
				pass
				#self.es.update(index='candidates',doc_type='archival_description',id=e['_id'],body={"doc": {"_status": 'confirmed'}})					 
	
	
	def get_parents(self,r_id):
		"""
		Returns a list of dict with all parents of an specific record, which are already part of the index
		
		Parameter:
		- r_id: ID of the child record
		"""
		l=[]
		finish=False
		while not finish:
			r=self.id_in_index(r_id)
			if r:
				l.append(r)
				if 'parentId' in r:
					if (r['parentId'] == r['legacyId']) or (r['parentId']=='') :
						finish=True
					else:
						r_id=r['parentId']
				else:
					finish=True
			else:
				finish=True
		return l
		
		
		
	
	
	
	def confirm_parents(self,r_id, record_type='archival_description',indexstore='candidates',status='confirmed'):
		"""
		Confirms parents if record has been confirmed, this will skip the relevance check for the parent.
		
		Parameters:
		- r_id: ID of the child record
		- record_type: The ElasticSearch docucument_type, standard is 'archival_description'
		- indexstrore: The name of the index, where he parents are located, standard is 'candidates'
		"""
		count=-1
		l=self.get_parents(r_id)
		for e in l:
			print(e)
			self.es.update(index=indexstore,doc_type=record_type,id=e['descriptionIdentifier'],body={"doc": {"_status": status}})	
			count+=1

	
	def merge(self,d1,d2):
		
		return d1 # AUSNAHME !!!
		"""
		Merge two records, returns dict
		
		"""
		
		
		if '_original_id' in d1:
			if '_original_id' in d2:
				if d1['_original_id']==d2['_original_id']:
					identic=True
					if len(d1)!=len(d2):
						identic=False
					for k in d1.keys():
						if k in d2:
							if k !='_last_modified':
								if d1[k]!=d2[k]:
									identic=False
					for k in d2.keys():
						if k !='_last_modified':
							if k in d1:
								if d1[k]!=d2[k]:
									identic=False			
					if identic:
						return None
		for k in d1.keys():
			if k in d2:
				if k not in ('is_parent', 'is_child', '_last_modified'):
					if d1[k]!=d2[k]:
						if d1[k]=='':
							d1[k]=d2[k]
						elif d2[k]=='':
							pass
						else:
							d1[k]+=' | '+d2[k]
		for k in d2.keys():
			if not k in d1:
				d1[k]=d2[k]	
				
		d1['_status']='candidate'
		print('merged')
		return d1.copy()
						
	
	def hierarchy_sort(self,l):
		nodes=[]
		forrest=[]
		lst=[]
		startlen=len(l)
		print("len(l) ",len(l))
		for e in l:
			if 'legacyId' in e:
				if 'parentId' in e:
					nodes.append({"l":e['legacyId'],"p":e['parentId']})
				else:
					nodes.append({"l":e['legacyId'],"p":""})
		#print((json.dumps(nodes, sort_keys=True, indent=2, ensure_ascii=False)))	
		print(len(nodes))
		f=open("nodex.json","w")
		f.write(json.dumps(nodes, sort_keys=True, indent=2, ensure_ascii=False))
		f.close()
		
		
		# adding all top node to forrest
		for e in l:
			found=True
			if 'parentId' in e:
				parent=e['parentId']
			else:
				parent=""
			if 'legacyId' in e:
				legacy=e['legacyId']
			else:
				continue
			node={"l":legacy,"p":parent}
			while found:
				x=next((x for x in nodes if x["l"]==parent),False)
				#print (x)
				if x:
					parent=x["p"]
					legacy=x["l"]
					node={"l":legacy,"p":parent}
				else:
					found=False
			if node not in forrest:
				forrest.append(node)
		#pprint.pprint(forrest)
		#print("================================================")
		#print(len(forrest))
		self.Counter=0
		for b in forrest:
			b=self._add_child(b,nodes,forrest)
			
		#print(json.dumps(forrest, sort_keys=True, indent=2, ensure_ascii=False))	
		#pprint.pprint(forrest)
		#print(len(forrest))
		# start flatten
		ids=[]
		pprint.pprint(forrest)
		print("Forrest len",len(forrest))
		with open("tmp_forrest.json", 'w') as file:
			file.write(json.dumps(forrest, sort_keys=True, indent=4, ensure_ascii=False))

		file.close()
		
		for legacy in self._forrest_generator(forrest):
			ids.append(legacy)
		#print(startlen,len(ids))
		#pprint.pprint(ids)
		#for legacy in ids:
		print("-----")
		l.sort(key=lambda x: ids.index(x['legacyId']))
		return (l.copy(),nodes.copy())
		

		

	def _remove_duplicates(self,l,key):
		"""
		removes items form a list if another item has the same key value
		"""
		out=[]
		c=0
		c_key=0
		for e in l:
			if key in e:
				if not next((x for x in out if x[key]==e[key]),False):
					out.append(e)
				else:
					c+=1
			else:
				c_key+=1
		
				
		#print(c," duplicates found in list\n",c_key, " items without specific key found")
		return out.copy()
		
			
	
	def _add_child(self,b,nodes,forrest):
		"""
		add information to the csv for better understanding during manual inspection of the file
		"""
		c=0

		for e in (x for x in nodes if x["p"]==b["l"]):
			if self._is_in_forrest(forrest,e['l']):
				continue
			c+=1
			
			if 'c' in b:
				b['c'].append(self._add_child(e,nodes,forrest))
			else:
				b['c']=[self._add_child(e,nodes,forrest)]
		self.COUNTER +=1
		print (self.COUNTER)
		
		print(c,"added to: l:",b['l'])
		return b.copy()
		
	def _forrest_generator(self,lst):
		for e in lst:
			#print(e['l'],"<--- e['l']")
			yield e['l']	
			if 'c' in e:
				yield from self._forrest_generator(e['c'])

	
	def _is_in_forrest(self,forrest,legacyid):
		for e in self._forrest_generator(forrest):
			if e==legacyid:
				return True
		return False
					

	
	def OLDhierarchy_sort(self,l):
		"""
		This sorts the exported csv list in a specific way. AtoM accepts only children if the parent was read before
		
		Parameter:
		- l : A list of dict, representing the records to export
		"""
		print (len(l))

		go=True
		roots=0
		x=0
		while go:
			c=0
			x+=1
			print(x)
			go=False	
			for i in range(0,len(l)-1):
				if not 'checked' in l[i]:
					if 'parentId' in l[i]:
						p=self.get_parent(l,l[i]['parentId'])
					else:
						p=-1
					if p == -1: # means that i is a root
						roots+=1
						if i > roots-1: #root isn't already at one of the first positions
							l[0]['checked']=True
							c+=1
							d=l.pop(i)
							l.insert(0,d) # brings the root at first position
							i-=1
							go=True
					if p>i:
						l[i]['checked']=True
						c+=1
						l[i],l[p]=l[p],l[i]
						go=True
		
			
			print (str(c) + " checked")
			qc=self._quality_check(l)

			
			print ("Quality check: " + str(qc))
		#any dict unchecked?
		c=0
		for e in l:
			if not 'checked' in e:
				c+=1
		print(str(c) + " unchecked")
		
	
	def OLD_quality_check(self,l):
		"""
		This works together with hierarchy_sort. 
		
		After each sort loop a quality check will test if there are still missorted records.
		Returns the number of records in disorder. This could also be records with missing parents (orphans)
		"""
		err=0
		id_list=[]
		for d in l:
			if 'legacyId' in d:
				id_list.append(d['legacyId'])
				if 'parentId' in d:
					if d['parentId']!="":
						for k in id_list:
							num=1
							if k==d['parentId']:
								num=0
								break
						if d['levelOfDescription'] in ('Fonds','Collection'):
							num=0
						if num==1:
							print(d['legacyId'] + " " + d['parentId'] + " " + d['levelOfDescription'])
							if 'checked' in d: del d['checked']
						err+=num
		return err
	
		
	def get_parent(self,l, parent_id):
		"""
		This helps sorting export lists
		
		Parameter:
		- l : list of dict representing all records above the child
		- parent_id: parentId of child = legacyId of parent
		
		Returns the record, which one is the direct parent
		"""
		i=0
		if parent_id != "":
			for d in l:
				
				if d['legacyId']==parent_id:
					return i
				i+=1
		return -1  # has no real parent
	
	
	def get_mysql(self,sql,one=True, update=False):
		"""
		Returns the result of a select statement in mysql
		
		Parameter:
		sql : the query string
		one : True if just the first line of results should be returned 
		
		Return type are tuple or a list of tuple
		
		"""
		print (self)
		cursor = self.MYSQL_CONNECTION.cursor()
		cursor.execute(sql)
		if update:
			self.MYSQL_CONNECTION.commit()
			print("updated")
			return cursor.lastrowid
		else:
			if one:
				return cursor.fetchone()
			else:
				return cursor.fetchall() 
	
	def _is_in_time(self,eventStartDates, eventEndDates):
		"""
		returns True if eventDates are partially within the time frame
		"""
		if eventStartDates[0:4].isnumeric():
			start=int(eventStartDates[0:4])
		else:
			start=self.LIMIT_FROM
		if eventEndDates[0:4].isnumeric():
			end=int(eventEndDates[0:4])
		else:
			end=self.LIMIT_TO
		if start>end:
			start,end =end,start				
		if start>self.LIMIT_TO or end<self.LIMIT_FROM:
			return False
		else:
			print("record is in time")
			return True
		
	def is_frequent_name(self,name):
		"""
		Checks if a given name is a too frequent, so there will be to much noise during the search
		"""
		self._open_frequent_names()
		name_arr=re.findall(r'\w+', name)
		not_in_list=False
		for n in name_arr:
			if n not in self.FREQUENT_NAMES:
				not_in_list=True
			if not_in_list:
				return False
		return True
		
	def is_in_atom(self,key, strict=False, import_match=False):
		"""
		checks if an item found in a external database ist already stored in atom. (or inside one of the csv files waiting for import into atom
		
		Parameter:
		key : A string representing the key (should be identical with the key stored in atom.keymap.source_id or atom.information_object.description_identifier)
		strict: If True, the key will only be checked against the AtoM database. 
		import_match : returns a tuple (source_id,source_name) which is needed for the import match. If True strict should also be True
		"""
		
		
		if key in self.CURRENT_DESCRIPTION_IDENTIFIERS:
			if not strict:
				return True
		else:
			sql='SELECT source_id, source_name FROM keymap WHERE source_id = "'+key+'";'
			result=self.get_mysql(sql)
			self._open_current_description_identifiers()

			if result:
				if import_match and strict:
					return result
				elif not import_match:
					return True
				else:
					return False
			else:
				if import_match:
					return False
				sql='SELECT description_identifier,id FROM information_object WHERE description_identifier= "'+key+'";'
				result=self.get_mysql(sql)
				if result:
					return True
				else:
					return False
	
		
	def publish(self):
		""" 
		Changes the publication status inside AtoM from draft to publish for all records, using an MySQL call.
		
		"""
		sql="select count( status_id) as c from status where object_id<>1 and type_id=158 group by status_id order by c ;" 
		r=self.get_mysql(sql,True,False)
		print("The status of " ,r[0]," records will be set from draft to published. \nDon't forget to repopulate the search index, using the command: sudo php symfony search:populate")
		sql="UPDATE status SET status_id=160 WHERE type_id=158 AND object_id <> 1;"
		r=self.get_mysql(sql,None,True)
		return 	
	
	def normalize_authority_data(self, record_type, proceed=True):
		sql='select a.id, authorized_form_of_name, description_identifier, entity_type_id  \
		from actor a join actor_i18n ai on a.id=ai.id join object o on a.id=o.id \
		where  o.class_name="QubitActor" order by description_identifier , entity_type_id;'
		log=""
		self._open_stopwords()
		stopwords=set(self.STOPWORDS)
		person_stopwords=stopwords.symmetric_difference(set(self.COORPORATE_DIFFENCES))
		l=self.get_mysql(sql,False,False)
		l_new=[]
		l_old=[]
		for e in l:
			if e[2]:
				l_old.append(list(e))
			else:
				f=list(e)
				if f[1]:
					n_arr=re.findall(r'\w+', f[1].lower())
					
					f.append(n_arr)
					l_new.append(f)
		for e_source in l_new:
			n_arr=re.findall(r'\w+', e_source[1].lower())
			for e_target in (x for x in l_old if self._check_appearance(x[1],e_source[4])):
				sql='select subject_id from relation where object_id='+str(e_source[0])+';'
				r=self.get_mysql(sql,False,False)
				if r:
					log+="Archival descriptions connected to "+ e_source[1] + " (id:"+str(e_source[0])+"):"
					for object_id in r:
						log +=str(object_id[0]) + ", "
					log+="\t -> append to "+e_target[1] + " (id:"+str(e_target[0]) + ")\n"
				if proceed:
					sql='update relation set object_id='+str(e_target[0])+' where object_id='+str(e_source[0])+';'
					x=self.get_mysql(sql,False,True)
					sql='delete from object where id='+str(e_source[0])+';'
					x=self.get_mysql(sql,False,True)
			else:
				#print(n_arr)
				if not e_source[3]:
					#print(e_source)
					if len(list(set(n_arr).intersection(person_stopwords)))>0:
						log+= e_source[1] + " (id:" + str(e_source[0]) +") [" + self.convert_name(e_source[1]) + "] could be a person?\n"
						k_list=self._get_wd_from_search_term(e_source[1])
						if k_list:
							if len(k_list)==1:
								log+="---> Could be identic to " + k_list[0]['label_de'] + " [" +k_list[0]['item']+ "] ("+ self._get_instance(k_list[0])+")\n"
								if proceed:
									sql='Update actor set description_identifier="http://www.wikidata.org/entity/'+k_list[0]['item']+'", entity_type_id = ' + str(A_ENTITY_TYPE_HUMAN)+ ' where id='+str(e_source[0])+';'
									x=self.get_mysql(sql,False,True)
						if proceed:
							sql='Update actor set entity_type_id = '+str(A_ENTITY_TYPE_HUMAN) + ' where id=' + str(e_source[0])+';'
							x=self.get_mysql(sql,False,True)
							sql='update actor_i18n set authorized_form_of_name="'+self.convert_name(e_source[1])+'" where culture="'+CULTURE+'" and id=' + str(e_source[0])+';'
							x=self.get_mysql(sql,False,True)
							self._add_other_forms_of_name(e_source[0],A_TYPE_OTHER_FORM_OF_NAME,e_source[1])
							
		print(log)
		
		
	def find_authority_data(self, proceed=True):
		"""
		Looks if some label from autority data are present in a specific information object.
		"""
		self._open_names_suffixes()
		suffixes =[]
		common_names=[]
		for e in self.NAME_SUFFIXES.values():
			suffixes.extend(e)
		suffixes=[x.lower() for x in suffixes]
		print(suffixes)
		self._open_stopwords()
		stopwords=set(self.STOPWORDS)
		person_stopwords=stopwords.symmetric_difference(set(self.COORPORATE_DIFFENCES))  # just a list of first names
		sql='select oi.id, title, scope_and_content,slug from information_object_i18n oi join slug s on oi.id=s.object_id ;'
		information_objects=self.get_mysql(sql,False,False)
		sql='select a.id,a.entity_type_id, authorized_form_of_name, a.description_identifier,ai.functions from actor_i18n ai join actor a on a.id=ai.id where a.entity_type_id in \
			('+ str(A_ENTITY_TYPE_HUMAN) + ',' + str(A_ENTITY_TYPE_ORGANISATION) + ',' + str(A_ENTITY_TYPE_BUSINESS) + ',' + str(A_ENTITY_TYPE_AUTHORITY) + ',' + str(A_ENTITY_TYPE_ASSOCIATION) + ',' + str(A_ENTITY_TYPE_MILITARY) + ',' + str(A_ENTITY_TYPE_SCIENCE) + ',' + str(A_ENTITY_TYPE_RELIGION) +') ;'
		authority_data=self.get_mysql(sql,False,False)
		#print(authority_data)
		sql='select id,subject_id, object_id from relation r where type_id='+str(A_RELATION_TYPE_NAME_ACCESS_POINTS)+' ;'
		relations=list(self.get_mysql(sql,False,False))
		new_relations=[]
		for information_object in information_objects:
			#print(information_object[0])
			test_text=" "+ (information_object[1] or '') + " xxxxxx " + (information_object[2] or '') +" "
			test_text_arr=re.findall(r'[\w|-]+',test_text.lower())
			#print(test_text_arr)
			for authority_data_item in authority_data:   # full check of labels already known as authority_data inside AtoM
				append=False
				if authority_data_item[2]:
					#print(authority_data_item[2])
					if test_text.find(" "+ authority_data_item[2].lower()+" ")>-1:
						print("gefunden: nachname, vorname")
						if authority_data_item[1]==A_ENTITY_TYPE_HUMAN:  # special check for Humans
							a_data_arr=re.findall(r'[\w|-]+',authority_data_item[2].lower())
							cn={}
							e=next((x for x in common_names if x['family_name']==a_data_arr),False)
							if e:
								e['count']+=1
								e['id'].append=authority_data[0]
							else:
								common_names.append({'count':1,'family_name':a_data_arr,'id':[authority_data[0]]})
							if len(a_data_arr)>1:
								r= [i for i,x in enumerate(test_text_arr) if x==a_data_arr[0] and x+1==a_data_arr[1]]
								if r:
									for e in r:
										if e>0:
											if test_text_arr[e-1].lower() not in person_stopwords:
												if not self._is_in_relation(information_object[0],authority_data_item[0], relations, new_relations):
													relations.append((None,information_object[0],authority_data_item[0]))
													print ("%s added to id: %d, %s" % (authority_data_item[2],information_object[0],information_object[1]))
								if test_text.find(" "+ self.convert_name(authority_data_item[2].lower())+" ")>-1:   #reverse check
									if not self._is_in_relation(information_object[0],authority_data_item[0], relations, new_relations):
										append=True	
													
							
						else:
							if not self._is_in_relation(information_object[0],authority_data_item[0], relations, new_relations):
								append=True

					if authority_data_item[1]==A_ENTITY_TYPE_HUMAN: 
						a_data_arr=re.findall(r'[\w|-]+',authority_data_item[2].lower())
						if len(a_data_arr)>1 and len(a_data_arr[1])>2 and a_data_arr[0] not in person_stopwords:
							family_name= a_data_arr[0].lower()
							r=[i for i,x in enumerate(test_text_arr) if x==family_name ]   #check für combination suffix + family_name
							if len(r)==0:
								r=[i for i,x in enumerate(test_text_arr) if x==(family_name +"s")]
							for e in r:
								if e>0:
									if test_text_arr[e-1].lower() in suffixes:
										if not self._is_in_relation(information_object[0],authority_data_item[0], relations, new_relations):
											append=True			
						
					if append:
						d={}
						d['append']=True
						d['url']=self.BASE_URL+information_object[3]
						d['functions']=authority_data_item[4]
						d['object_id']=information_object[0]
						d['subject_id']=authority_data_item[0]
						d['title']=test_text
						d['authority']=authority_data_item[2]
						d['description_identifier']=authority_data_item[3]
						new_relations.append(d)
						print ("%s added to id: %d, %s" % (authority_data_item[2],information_object[0],information_object[1]))											
											
		for e in common_names:
			if e['count']>1:
				new_relations.append(e)
												
		
		with open(self.NEW_RELATION_FILE, 'w') as file:
			file.write(json.dumps(new_relations, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()

		print (relations)
		if proceed:
			for e in relations:
				if not e[0]:
					sql='insert into object (class_name,created_at,updated_at,serial_number) value ("QubitRelation",Now(),Now(),0)'
					last_id=self.get_mysql(sql,False,True)
					sql='insert into relation (id, subject_id,object_id,type_id,source_culture) value ('+str(last_id)+','+ str(e[1])+',' + str(e[2]) + ',' +str(A_RELATION_TYPE_NAME_ACCESS_POINTS)+ ',"'+CULTURE +'");'
					r=self.get_mysql(sql,False,True)
					
	def normalize_authority_data_identifier(self):
		"""
		Changes the base_url of description_identifier in atom.actor to http://wikidata.org/entity/
		Checks all entries in atom.actor
		"""
		sql=' select id, description_identifier from actor where description_identifier like "%wikidata%" and description_identifier not like "%http://www.wikidata.org/entity%";'
		identifiers=self.get_mysql(sql,False,False)
		if identifiers:
			identifiers=list(identifiers)
			for identifier in identifiers:
				identifier=list(identifier)
				print(identifier)
				identifier[1]="http://www.wikidata.org/entity/"+ identifier[1][identifier[1].rfind("/")+1:]
				print("to ---> " + identifier[1])
				sql='update actor set description_identifier = "'+identifier[1] + '" where id= ' + str(identifier[0])+';'
				r=self.get_mysql(sql,False,True)
				

	
	def remove_duplicate_relations(self):
		"""
		Removes entries in atom.relation where the object_id subject_id pair are identical having start_date and end_date NULL and same source_culture
		"""	
		id_to_remove=[]
		new_relations=[]
		sql='select id, subject_id, object_id,start_date,end_date, source_culture from relation;'
		relations=self.get_mysql(sql,False,False)
		for relation in relations:
			e=next((x for x in new_relations if x[1]==relation[1] and x[2]==relation[2] and x[3]==relation[3] and x[4]==relation[4] and x[5]==relation[5]),False)
			if e:
				id_to_remove.append(relation[0])
			else:
				new_relations.append(relation)
				
		print(id_to_remove)
		print(len(id_to_remove))
		for e in id_to_remove:
			sql='delete from object where id='+str(e)+';'
			x=self.get_mysql(sql,False,True)
			
		
		

	def _is_in_relation(self, subject_id, object_id,relations, new_relations):
		"""
		Returns true if at least one pair of subject_id - object_id is found in relations
		"""
		e=next((x for x in relations if relations[1]==subject_id and relations[2]==object_id),False)
		if e:
			return True
		else:
			ee=next((x for x in new_relations if x['subject_id']==subject_id and x['object_id']==object_id), False)
			if ee:
				return True
			else:
				return False 
		
		
		
	def _add_other_forms_of_name(self, object_id, type_id,name, start_date="", end_date="", note="", dates=""):
		"""
		Adds parallel or other form of name to an authorithy entry.
		Checks if already present
		
		Parameter:
		object_id: AtoM's id of the authorithy item
		type_id : AtoM's id for other_form_of_name or parallel_form_of_name
		name: string value
		start_date : Optional  (in sql format)
		end_date: Optional  (in sql format)
		dates: Optional  (in sql format)
		note: Optional  (in sql format)
		"""	
		
		sql='select name  from other_name o join other_name_i18n oi on o.id=oi.id  where object_id='+str(object_id)+' and type_id='+str(type_id)+';'
		r=self.get_mysql(sql,False,False)
		if r:
			for e in r:
				if e[0].lower()==name.lower():
					return False
		sql='insert into other_name (object_id, type_id, start_date, end_date, source_culture) \
			values ('+str(object_id)+','+str(type_id)+',"'+start_date+'","'+end_date+'","'+CULTURE+'");'
		increment=self.get_mysql(sql,False,True)
		if increment:
			sql='insert into other_name_i18n (name,note,dates,id,culture) \
				values ("'+name+'","'+note+'","'+dates+'",'+str(increment)+',"'+CULTURE+'");'
			r=self.get_mysql(sql,False,True)
			return True
	
	
	def _check_appearance(self,s,arr):
		"""
		Checks if all element arr are found in s. 
		Returns boolean
		
		Parameters:
		s: The test string, will be converted to lower lower()
		arr: An array which contains a number of string elements
		"""
		self._open_stopwords()
		check=True
		only_stopwords=True
		s=s.lower()
		

		for e in arr:	
			if len(e)>1:			
				if e.lower() not in self.STOPWORDS:
					only_stopwords=False
				if s.find(e.lower())==-1 :
					return False
		if only_stopwords:
			
			return False
		else:
			if len(arr)==len(re.findall(r'\w+', s)):
				return True
			else:
				return False
	
	
	
	def convert_name(self,s, directions=0):
		"""
		Change the "Ansetzungsform" (authorized_form)  of the name of a person from '[first name] [name]' to [name], [first name] and viceversa. 
		The direction depends of the presence of a ',' inside the given String
		
		Parameter:
		s= a string containing the name of the person	
		directions: restrains the conversion (0: Conversion in both directions; 1: Conversion from " " to ", " only; 2: conversions from ", " to " " only)
		"""
		if s.find(",")>-1:
			if directions<1:
				return (s[s.find(',')+1:] + " " + s[0:s.find(',')]).strip()
			else:
				return s

		else:
			if directions<1:
				name_arr=re.findall(r'[\w|-]+', s)
				if len(name_arr)>1:
					start=len(name_arr)-1
					for e in name_arr:
						if e.lower() in ['und','y']:
							start=name_arr.index(e)-1
					return " ".join(name_arr[start:])+", " + " ".join(name_arr[0:start])
				else:
					return s
			else:
				return s
		
		
		
		
	def replace_mysql_field(self,table, change_field, id_field, id_value,regex):
		"""Modify the value of an MySql field using regex
		
		table - name of the MySql table within atom database
		change_field  -the field to modify
		id_field - the name the field which serves as identifier
		id_value - the value of the identifer
		regex = the regular expression to apply
		"""
		
		cursor = self.mysql.cursor()
		sql="select " + change_field + " from "+table+" where "+id_field+"='"+id_value+"';"
		cursor.execute(sql)
		field_content = cursor.fetchall()
		if len(field_content)==0:
			return "none result"
		if len(field_content)>1:
			#check if the result set is identic for all items
			for i in range(1,len(field_content)-1):
				if field_content[i-1][0]!=field_content[i][0]:
					return "too much non identic results"
		
		print (field_content)
		print (type(field_content))
		#print (field_content[1][0])
		print (len(field_content))
		return
	
	
	def build_eventDates(self,dates):
		"""
		tries to extract eventStartDates and eventEndDates
		"""
		dates_arr=re.findall(r'\w+|[0-9]+', dates)
		start_year=""
		start_month=""
		start_day=""
		end_year=""
		end_month=""
		end_day=""
		for e in dates_arr:
			if e.isnumeric() :
				if int(e) in range(1000,2000):
					if start_year=="":
						start_year=e
					else:
						end_year=e
				if int(e) in range(0,31):
					if len(e)==1:
						e+="0"+e
					if start_day=="":
						start_day=e
					else:
						end_day=e

			if e.lower() in g.MONTHS.keys():
				if start_month=="":
					start_month=g.MONTHS[e.lower()]
				else:
					end_month=g.MONTHS[e.lower()]						
						
		if start_month=="":
			start_month="01"
		if end_month=="":
			if start_month!="01":
				end_month=start_month
			else:
				end_month="12"
		if start_day=="":
			start_day="01"
		if end_day=="":
			if start_day!="01":
				end_day=start_day
			else:
				end_day="31"
		if start_year=="":
			return("","")
		if end_year=="":
			return (start_year + "-"+start_month+"-"+start_day, start_year + "-"+end_month+"-"+end_day)
		return (start_year + "-"+start_month+"-"+start_day, end_year + "-"+end_month+"-"+end_day)
			
					

			
					
			
	def OLDbuild_eventDates(self,dates,se):
		"""
		Tries to identify eventStartDates or eventEndDates in normalized format. 
		
		Parameters:
		- dates: A string containing some date information
		- se: 'start' if eventStartDates is needed, else 'end' 
		"""
		if isinstance(dates,list):
			dates=' '.join(dates)
		m =re.search('(1[8-9][0-9][0-9]).*(1[8-9][0-9][0-9])',dates)
		#print (m.group(1))	
		if m is not None:
			if se=='start':
				return m.group(1)+'-01-01'
			else:
				return m.group(2)+'-12-31'
		else:
			m =re.search('(1[8-9][0-9][0-9])',dates)
			if m is not None:
				if se=='start':
					return m.group(1)+'-01-01'
				else:
					return m.group(1)+'-12-31'		
			else:
				return ""			
	
	def flat(self,d):
		"""
		It's an uggly attemp to workaround elasticsearchs incapacibility to accept a list dicts as field value 
		"""
		for k,v in d.items():
			if  isinstance(v,list):
				d[k]=json.dumps(v)
		#print (d)
		return d.copy()		
	
	def reduce_x(self,x):
		"""
		An uggly attemp to get values from a super nested list of dicts 
		"""
		print(type(x))
		if isinstance(x,bool):
			if x:
				return "True"
			else:
				return "False"
		if isinstance(x,list):
			for e in x:
				if not isinstance(e,str):
					e=self.reduce_x(e)
			if isinstance(x,dict):
				return "; ".join(x.values())
			print("---")
			print(type(x))
			return "; ".join(x)
		if isinstance(x,dict):
			for k,v in  x.items():
				if not isinstance(v,str):
					x[k]=self.reduce_x(v)
			if isinstance(x,list):
				return "; ".join(x)
			
			print(x)
			return "; ".join(x.values())


			
				
	def json2csv(self,rfile, wfile):
		"""
		A simple helper tool. Currently not used.
		
		Parameters:
		- rfile : input file as json
		- wfile : output file as csv
		
		WIll not completely work with nested json		
		"""
		with open(rfile, 'r') as rf:
			l= json.load(rf)
		rf.close()

	
		with open(wfile,'w') as wf:
			fieldnames = l[0].keys()
			writer = csv.DictWriter(wf, fieldnames=fieldnames,extrasaction='ignore', delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for e in l:
				writer.writerow(e)
		wf.close()
		
	def _get_from_WDQ(self,query):
		try:
			params={
					'format':"json",
					'query':query
					}
			url=self.WD_SPARQL_ENDPOINT +"?"+ urllib.parse.urlencode(params)
			headers={}
			headers['accept']='application/sparql-results+json'
			print(url)
			r = urllib.request.Request(url, None, headers)
			with urllib.request.urlopen(r) as response:
				the_page = response.read().decode("utf-8")
				return json.loads(the_page)
		except:
			return {'results':{'bindings':[]}}
	
	def _short_entity(self,url):
		"""
		cuts the path of the Wikidata entity
		"""
		ar=url.split("/")
		return ar[len(ar)-1]
		
	
	def _get_uniq(self,s):
		if isinstance(s, list):
			array=s
		else:
			array=s.split('|')
		ar=list(set(array))
		#print(ar)
		ar=list(filter(None, ar))
		if len(ar)==0:
			ar=['']
		ar.sort(reverse=True)
		return ar.copy()

	
	def _open_stopwords(self):
		if len(self.STOPWORDS)==0:
			if os.path.getsize(self.STOPWORDS_FILE) > 1:
				with open(self.STOPWORDS_FILE, 'r') as file:
					self.STOPWORDS = file.read().split('\n')
				file.close()

	def _clean_label(self,s):
		result=""
		self._open_stopwords()
		s_arr=re.findall(r'\w+', s)
		for word in s_arr:
			if word.lower() not in self.STOPWORDS:
				result+=word+" "
		return result.strip(" ")
		
	def _get_frequency(self,rjson):
		count=0
		search=rjson['searchinfo']['search']
		search=search.lower()
		for result in rjson['search']:
			if 'decription' in result:
				if result['description'] == "Wikimedia-Begriffsklärungsseite":
					continue
			if 'label' in result:
				s=result['label'].lower()
				s_arr=re.findall(r'\w+', s)
				if search in s_arr:
					count +=1
		if count==0:
			count=1
		return count
				
	def _store_objects(self):
		#return
		old_log=""
		
		with open(self.KEYWORDS_FILE, 'w') as file:
			file.write(json.dumps(self.KEYWORDS, sort_keys=True, indent=4, ensure_ascii=False))

		file.close()		
		
		if os.path.isfile(self.SEARCH_LOG_FILE):
			with open(self.SEARCH_LOG_FILE, 'r') as file:
				#old_log = file.read().split('\n')
				old_log = file.read()
			file.close()	
		
		with open(self.SEARCH_LOG_FILE,'w') as file:
			file.write(self.SEARCH_LOG+'\n'+old_log)
		file.close()	

