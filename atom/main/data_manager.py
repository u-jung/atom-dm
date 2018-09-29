#!/usr/bin/python3
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
import urllib.parse, urllib.request, urllib
from slugify import slugify
from elasticsearch import Elasticsearch
import json, csv, os, MySQLdb, re, time, pathlib, pprint, requests, shutil, getpass, sys
from datetime import datetime
from atom.config import mysql_opts
from atom.main import g
from atom.helpers.helper import fileOps, listOps, stringOps, osOps
fo = fileOps()
lo = listOps()
so = stringOps()
oo = osOps()

class DataManager(object):
	"""
	Core class of atom.py.
	
	Does manage:
	- The import from external source into the candidates index of elastic search.
	- Evaluation of imported records and relevance check.
	- Import of confirmed records into AtoM database
	- Some management tasks of AtoM
			
	"""
	indexstore = 'candidates'
	max_results = 200000
	jump_fonds = [
	 'BArch R 1001', 'BArch R 1002', 'BArch R 1003', 'BArch R 8023', 'BArch R 8024', 'BArch R 1003', 'BArch R 8124', 'BArch RW 51', 'BArch RH 88', 'BArch RW 51']
	KEYWORDS_FILE = 'atom/data/keywords.json'
	KILLERS_FILE = 'atom/data/killers.txt'
	ENTITIES_FILE = 'atom/data/entities.txt'
	STOPWORDS_FILE = 'atom/data/stopwords.txt'
	FREQUENT_NAMES_FILE = 'atom/data/frequent_names.txt'
	NAME_SUFFIXES_FILE = 'atom/data/name_suffixes.json'
	SEARCH_HISTORY_PATH = 'search_history/'
	SEARCH_HISTORY_FILE_SUFFIX = '_search_history.csv'
	TMP_RESULTS_PATH = 'tmp_results/'
	CURRENT_DESCRIPTION_IDENTIFIERS_FILE = 'atom/data/current_description_identifiers.txt'
	SEARCH_LOG_FILE = 'log/adm_search.log'
	TMP_OUT_FILE = 'search_history/tmp_out.txt'
	DEF_OUT_FILE = 'search_history/def_out.txt'
	INSTITUTIONS_FILE = 'atom/data/institutions.json'
	COUNTER = 0
	LANGUAGES = {'ger': 'de', 
	 'eng': 'en', 
	 'fre': 'fr', 
	 'spa': 'es', 
	 'prt': 'pt', 
	 'rus': 'ru', 
	 'jpn': 'jp', 
	 'zho': 'zh', 
	 'ita': 'it', 
	 'dan': 'dk', 
	 'swe': 'sw', 
	 'nld': 'nl', 
	 'pol': 'pl', 
	 'ara': 'ar', 
	 'tur': 'tk'}
	BASE_URL = 'http://vm.atom/'
	KEYWORDS = []
	STOPWORDS = []
	FREQUENT_NAMES = []
	NAME_SUFFIXES = []
	KILLERS = []
	INSTITUTIONS = []
	CURRENT_DESCRIPTION_IDENTIFIERS = []
	SEARCH_LOG = ''
	LEGACY_IDS = []
	TMP_OUT = []
	DEF_OUT = []
	COORPORATE_DIFFENCES = ['zu', 'von', 'ag', 'a.g.', 'g.m.b.h', 'gmbh', 'd.k.g', 'dkg', 'ohg',
	 'mbh', 'm.b.h', 'kg', 'k.g.']
	ARCHIVAL_DESCRIPTIONS_FIELDS = [
	 'prediction',
	 'out',
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
	WD_API_ENDPOINT = 'https://www.wikidata.org/w/api.php'
	WD_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'
	MYSQL_CONNECTION = None
	LIMIT_FROM = 1850
	LIMIT_TO = 1946
	LEVELS_OF_DESCRIPTION = {'htype_001': 'Section', 
	 'htype_002': 'Appendix', 
	 'htype_003': 'Contained work', 
	 'htype_004': 'Annotation', 
	 'htype_005': 'Address', 
	 'htype_006': 'Article', 
	 'htype_007': 'Volume', 
	 'htype_008': 'Additional', 
	 'htype_009': 'Intro', 
	 'htype_010': 'Entry', 
	 'htype_011': 'Fascicle', 
	 'htype_012': 'Fragment', 
	 'htype_013': 'Manuscript', 
	 'htype_014': 'Issue', 
	 'htype_015': 'Illustration', 
	 'htype_016': 'Index', 
	 'htype_017': 'Table of contents', 
	 'htype_018': 'Chapter', 
	 'htype_019': 'Map', 
	 'htype_020': 'Multivolume work', 
	 'htype_021': 'Monograph', 
	 'htype_022': 'Music', 
	 'htype_023': 'Serial', 
	 'htype_024': 'Privilege', 
	 'htype_025': 'Review', 
	 'htype_026': 'Text', 
	 'htype_027': 'Verse', 
	 'htype_028': 'Preface', 
	 'htype_029': 'Dedication', 
	 'htype_030': 'Fonds', 
	 'htype_031': 'Class', 
	 'htype_032': 'Series', 
	 'htype_033': 'Subseries', 
	 'htype_034': 'File', 
	 'htype_035': 'Item', 
	 'htype_036': 'Collection', 
	 'htype_037': '?', 
	 'htype_038': '?', 
	 'htype_039': '?', 
	 'tektonik_collection': 'Tektonik_collection', 
	 'repository': 'Collection', 
	 'institution': 'Institution'}
	ATOM_LOD_FONDS = 227
	ATOM_LOD_SUBFONDS = 228
	ATOM_LOD_COLLECTION = 229
	ATOM_LOD_SERIES = 230
	ATOM_LOD_SUBSERIES = 231
	ATOM_LOD_FILE = 232
	ATOM_LOD_ITEM = 233
	ATOM_LOD_PART = 290
	RECURSIVE_LOOP_COUNTER=0
	RECURSIVE_LOOP_MAX=1000

	def __init__(self, debug=False):
		self.es = Elasticsearch()
		self._open_keywords()
		if not debug:
			log = None
		self.MYSQL_CONNECTION = MySQLdb.connect(mysql_opts.mysql_opts['host'], mysql_opts.mysql_opts['user'], mysql_opts.mysql_opts['pass'], mysql_opts.mysql_opts['db'], use_unicode=True, charset='utf8')

	def __del__(self):
		pass

	def _open_current_description_identifiers(self):
		if os.path.isfile(self.CURRENT_DESCRIPTION_IDENTIFIERS_FILE):
			self.CURRENT_DESCRIPTION_IDENTIFIERS = fo.load_once(self.CURRENT_DESCRIPTION_IDENTIFIERS, self.CURRENT_DESCRIPTION_IDENTIFIERS_FILE)
		else:
			self.CURRENT_DESCRIPTION_IDENTIFIERS=""

	def _open_keywords(self):
		self.KEYWORDS = fo.load_once(self.KEYWORDS, self.KEYWORDS_FILE)

	def _open_institutions(self):
		self.INSTITUTIONS = fo.load_once(self.INSTITUTIONS, self.INSTITUTIONS_FILE)

	def _open_entities(self):
		self.ENTITIES = fo.load_once(self.ENTITIES, self.ENTITIES_FILE)

	def _open_names_suffixes(self):
		self.NAME_SUFFIXES = fo.load_once(self.NAME_SUFFIXES, self.NAME_SUFFIXES_FILE)

	def _open_killers(self):
		self.KILLERS = fo.load_once(self.KILLERS, self.KILLERS_FILE)

	def _open_frequent_names(self):
		self.FREQUENT_NAMES = fo.load_once(self.FREQUENT_NAMES, self.FREQUENT_NAMES_FILE)

	def _open_out_files(self):
		self.TMP_OUT = fo.load_once(self.TMP_OUT, self.TMP_OUT_FILE)
		self.DEF_OUT = fo.load_once(self.DEF_OUT, self.DEF_OUT_FILE)

	def store_out_files(self):
		ok = False
		if fo.save_data(self.TMP_OUT, self.TMP_OUT_FILE):
			ok = True
		if fo.save_data(self.DEF_OUT, self.DEF_OUT_FILE):
			ok = True
		return ok

	def add_tmp_out(self, item):
		"""
		adds an id to the list of temprary outs
		"""
		if item not in self.TMP_OUT:
			self.TMP_OUT.append(item)

	def add_def_out(self, item):
		"""
		adds an id to the list of definitivly outs
		"""
		if item not in self.DEF_OUT:
			self.DEF_OUT.append(item)

	def create_search_term_list(self, wdid=False):
		f = open('keywords/search_terms.txt', 'w')
		for term in self.search_term_generator():
			if wdid:
				f.write(term[0] + '\t' + term[1] + '\n')
			else:
				f.write(term[0] + '\n')
			print(term[0])

		f.close()
	
	def build_upper_levels(self,l,institution_id,field):
		"""
		creates a field in each dict of list with seperate records building upper hierarchy levels
		"""
		
		if l is None:
			filename=input("From File: ")
			l=fo.load_data(filename)
		if institution_id is None:
			institution_id=input("Instituion_id :")
		if field is None:
			field=input("Field :")		
			
		for e in l:
			for k,v in {'culture':'de','script':'latn','language':'de','languageNote':'deutsch'}.items():
				if not k in e:
					e[k]=v
			e['levelOfDescription']="File"
			if field in e:
				arr=e[field].split(">")
				
				e['c']=[]
				for ee in arr:
					
					d={}
					d['title']=ee.strip()
					for f in ("culture","repository","languageNote","script",'language','languageNote'):
						if f in e:
							d[f]=e[f]
					
					if arr.index(ee)==0:
						d['legacyId']=institution_id
						parent_id=institution_id
						d['levelOfDescription']="Tektonik_collection"
					else:
						d['legacyId']=institution_id+"_"+re.sub(r'\W',"",d['title'].lower())
						d['parentId']=parent_id
						parent_id=d['legacyId']
						d['levelOfDescription']="Class"
					if len(arr)>1 and arr.index(ee)==1:
						d['levelOfDescription']="Fonds"
					
					e['c'].append(d.copy())
				e['parentId']=parent_id
			else:
				e['parentId']=institution_id
		new_l=[]
		for e in l:
			if 'c' in e:
				for ee in e['c']:
					if not next((x for x in new_l if x['legacyId']==ee['legacyId']),False):
						new_l.append(ee.copy())
				del e['c']
			new_l.append(e.copy())
		new_l=self.hierarchy_sort(new_l)
		print(new_l)
		fo.save_data(new_l[0],filename+".json","json")
		self.write_csv( new_l[0], filename+".csv", record_type='archival_description')
		return new_l[0]
		

	def join_csv(self, add_keyword=True):
		"""
		joins all existing import.csv files inside self.TMP_RESULTS_PATH into one.
		Eliminates duplicates and reorders the items. The new file is stored in the root of self.TMP_RESULTS_PATH
		"""
		path = self.TMP_RESULTS_PATH
		l = []
		for root in os.walk(path):
			if root[0] == self.TMP_RESULTS_PATH:
				continue
			if os.path.isfile(root[0] + '/import.csv'):
				l.extend(fo.load_data(root[0] + '/import.csv'))

		print(len(l))
		l = self._remove_out_of_time(l)
		l = self._remove_duplicates(l, 'legacyId')
		l = self.hierarchy_sort(l)[0]
		l = self._ai_predict(l)
		l = self._add_access_points(l)
		self.write_csv(l, self.TMP_RESULTS_PATH + 'import.csv', 'archival_description')
		if add_keyword:
			l = self._add_keyword_info(l)
		self.write_csv(l, self.TMP_RESULTS_PATH + 'import.csv', 'archival_description')

	def _remove_out_of_time(self, l):
		"""
		removes "File" items from list if there are out of time
		"""
		new = []
		for e in l:
			if self._is_in_time(e['eventStartDates'], e['eventEndDates']):
				new.append(e)
			else:
				if e['levelOfDescription'] not in ('File', ):
					new.append(e)

		return new.copy()


	def parent_information_objects(self, parent_id,l, return_value):
		"""
		returns a list of atom information_object id, which are parents of the given id. The given id is element[0] of the list.
		l is a list of tuples where e[0]=id and e[1] = parent_id from atom.information_object
		"""
		e=next((x for x in l if x[0]==parent_id),False)
		#print(return_value)
		if e:
			parent_id=e[1] if len(e)>1  else False
			if parent_id :
				return_value.append(parent_id)
				return_value=self.parent_information_objects( parent_id,l,return_value.copy())
			else:
				return return_value.copy()
		else:
			return return_value.copy()
		return return_value.copy()	

		
		

	def reduce_csv(self, x=False, add_keyword=True, add_access_points=True):
		"""
		iterates once again the csv after manual inspection
		eleminates the marked items (and all children) and writes them into the self.DEF_OUT
		"""
		
		shutil.copy2(self.TMP_RESULTS_PATH + 'import.csv', self.TMP_RESULTS_PATH + 'import_old.csv')
		l = fo.load_data(self.TMP_RESULTS_PATH + 'import.csv')
		self.RECURSIVE_LOOP_MAX=len(l)
		new = []
		out = []
		incertain = []
		print('Reducing ', self.TMP_RESULTS_PATH + 'import.csv\n')

		#print([(x['out'],x['legacyId'],x['parentId']) for x in l])	
		#q=input("-----------------")		
		
		# check for manually confirmed (those parents and children wil also marked with ".")
		for i in range(0,len(l)):
			e=l[i]
			sys.stdout.write("\033[F")
			print(i)
			if 'legacyId' in e:
				if not (e['legacyId'] in self.LEGACY_IDS):
					self.LEGACY_IDS.append(e['legacyId'])
			if 'out' in e and e['out']==".":
				self.RECURSIVE_LOOP_COUNTER=0
				l = self._write_to_children(l, e['legacyId'], 'out', '.')
				self.RECURSIVE_LOOP_COUNTER=0
				l = self._write_to_parents(l, e['parentId'], 'out', '.')

		print([(x['out'],x['legacyId'],x['parentId']) for x in l])	
		#q=input("-----------------")
				
		# outake the manually not confirmed (thoose and children marked with x)
		for i in range(0,len(l)):
			e=l[i]
			sys.stdout.write("\033[F")
			print(i)
			
			if 'out' in e and e['out']=="x":
				self.RECURSIVE_LOOP_COUNTER=0
				print("----------------")
				l = self._write_to_children(l, e['legacyId'], 'out', 'x')

		print("manually confirmed items extended\n")
		#print([(x['out'],x['legacyId'],x['parentId']) for x in l])	
		#q=input("-----------------")
		# check for outdates  (outdate FILE are marked with "x" if not manually marked with ".")
		for i in range(0,len(l)):
			e=l[i]
			sys.stdout.write("\033[F")
			print(i)
			if 'eventDates' in e:
				e['eventStartDates'], e['eventEndDates'] = self.build_eventDates(e['eventDates'])
			else:
				if not('eventStartDates' in e and e['eventStartDates']!=""):
					e['eventStartDates']=""
				if not('eventEndDates' in e and e['eventEndDates']!=""):
					e['eventEndDates']=""			
			if not self._is_in_time(e['eventStartDates'], e['eventEndDates']):
				if e['levelOfDescription'] == 'File':
					if 'out' in e and e['out']==".":
						pass
					else:
						e['out'] = 'x'	
						self.RECURSIVE_LOOP_COUNTER=0
						l = self._write_to_children(l, e['legacyId'], 'out', 'x')
						self.add_def_out(e['legacyId'])	
		
		print("outdates marked \n")
		#print([(x['out'],x['legacyId'],x['parentId']) for x in l])	
		#q=input("-----------------")
		
		# check for dead end (all above "File" whitout a File child will be marked out="-"
		
		l = self._find_dead_ends(l)			
				
				
				
				
		x_id=[x for x in l if x['out']=="x"]
		p_id=[x for x in l if x['out']=="."]
		l_id=[x for x in l if x['out']==" " or not 'out' in x ]
		d_id=[x for x in l if x['out']=="-"]
		print(len(x_id), "x" , len(p_id), ".",len(d_id), "-", len(l_id), "(leer)",len(l))
		#print(x_id)
		#a=input("--------------")

		for e in l:
			if not (e['out'] in ("x","-")):
				new.append(e)
			else:
				self.add_def_out(e['legacyId'])

		if add_access_points:
			new = self._add_access_points(new, True)
		self.write_csv(new, self.TMP_RESULTS_PATH + 'import.csv', 'archival_description')
		self.write_csv(l, self.TMP_RESULTS_PATH + 'import_old_modified.csv', 'archival_description')
		self._post_import(new, '', True, add_keyword)

	def _find_dead_ends(self, l):
		"""
		marks non file items without children as out='-'
		"""
		for e in l:
			if e['levelOfDescription'] != 'File' and e['out'] == '':
				if not next((x for x in l if x['parentId'] == e['legacyId'] and x['out']!="x"), False):
					e['out'] = '-'
					l = self._write_to_parents(l, e['legacyId'], 'out', '-')

		return l.copy()

	def _write_to_children(self, l, legacyId, field, value):
		"""
		Writes a value into a field in each direct and indirect child of a given item
		"""
		#print(legacyId," : ", end='')
		self.RECURSIVE_LOOP_COUNTER+=1
		if self.RECURSIVE_LOOP_COUNTER>self.RECURSIVE_LOOP_MAX:
			raise ValueError('More than '+str(self.RECURSIVE_LOOP_MAX)+' level of children from parent', legacyId)
		#print(type(l), " - ", legacyId)
		for item in (x for x in l if x['parentId'] == legacyId):
			if item :
				item[field] = value
				l = self._write_to_children(l, item['legacyId'], field, value)

		return l.copy()

	def _write_to_parents(self, l, legacyId, field, value):
		"""
		writes a value into field of all parents of legacyId
		"""
		self.RECURSIVE_LOOP_COUNTER+=1
		if self.RECURSIVE_LOOP_COUNTER>self.RECURSIVE_LOOP_MAX:
			raise ValueError('More than '+str(self.RECURSIVE_LOOP_MAX)+' level of parents')
		item = next((x for x in l if x['legacyId'] == legacyId), False)
		if item:
			if value=='.':
				item[field] = value
				l = self._write_to_parents(l, item['parentId'], field, value)
			elif value=="-":
				if field in item:
					if item[field]!=".":
						item[field] = value
						l = self._write_to_parents(l, item['parentId'], field, value)
					else:
						return l.copy()
				else:
					item[field] = value
					l = self._write_to_parents(l, item['parentId'], field, value)
		else:
			return l.copy()
		return l.copy()
		"""
		if item:
			if item[field] != '':
				if value == '.':
					l = self._write_to_parents(l, item['parentId'], field, value)
				else:
					return l.copy()
			else:
				item[field] = value
				l = self._write_to_parents(l, item['parentId'], field, value)
		else:
			return l.copy()
		return l.copy()
		"""
	def get_from_atom(self, item):
		"""
		retrieves all information concerning a specific information_object from atom using AtoM's id
		returns a dict representing an csv entry
		Parameters:
		item : the AtoM id of the informtion_object
		"""
		d = {}
		fields_str = 'extent_and_medium,archival_history,acquisition,scope_and_content,appraisal,accruals,arrangement,access_conditions,reproduction_conditions,physical_characteristics,finding_aids,location_of_originals,location_of_copies, related_units_of_description,rules,sources,revision_history,culture,identifier,level_of_description_id, repository_id ,parent_id ,description_identifier,title,institution_responsible_identifier'
		fields = fields_str.split(',')
		query = 'select ' + fields_str + ' from information_object i join information_object_i18n ii on i.id=ii.id where i.id=' + item + ';'
		r = self.get_mysql(query, True)
		if r:
			i = 0
			for e in r:
				d[re.sub('_([a-z])', lambda m: m.group(1).upper(), fields[i])] = e
				i += 1

		print(d)
		if 'levelOfDescription' in d:
			query = 'select name from term_i18n where id=' + str(d['levelOfDescriptionId']) + ';'
			r = self.get_mysql(query, True)
			if r:
				d['levelOfDescription'] = r[0]
		if 'repositoryId' in d and d['repositoryId']:
			query = 'select respository_id from actor_i18n where id=' + e['repositoryId'] + ';'
			r = self.get_mysql(query, True)
			d['repository'] = r[0]
		d['institutionIdentifier'] = d['institutionResponsibleIdentifier']
		query = 'select source_id from keymap where target_id=' + item + ';'
		r = self.get_mysql(query, True)
		if r:
			d['legacyId'] = r[0]
		if d['parentId']:
			query = ' select slug from slug where object_id=' + d['parentId'] + ';'
			r = self.get_mysql(query, True)
			if r:
				d['qubitParentSlug'] = r[0]
			query = 'select source_id from keymap where target_id=' + d['parentId'] + ';'
			r = self.get_mysql(query, True)
			if r:
				d['parentId'] = r[0]
		query = 'select content from note_i18n ni join note n on ni.id=n.id where object_id=' + item + ' and type_id=124;'
		r = self.get_mysql(query, False)
		if r:
			value = []
			for e in r:
				value.append(e[0])

			d['archivistNote'] = ('|').join(value)
		query = 'select content from note_i18n ni join note n on ni.id=n.id where object_id=' + item + ' and type_id=174;'
		r = self.get_mysql(query, False)
		if r:
			value = []
			for e in r:
				value.append(e[0])

			d['languageNote'] = ('|').join(value)
		query = 'select content from note_i18n ni join note n on ni.id=n.id where object_id=' + item + ' and type_id=125;'
		r = self.get_mysql(query, False)
		if r:
			value = []
			for e in r:
				value.append(e[0])

			d['generalNote'] = ('|').join(value)
		query = 'select content from note_i18n ni join note n on ni.id=n.id where object_id=' + item + ' and type_id=120;'
		r = self.get_mysql(query, False)
		if r:
			value = []
			for e in r:
				value.append(e[0])

			d['publicationNote'] = ('|').join(value)
		query = 'select name from term_i18n t join status s on t.id=s.status_id where s.object_id=' + item + ' and t.culture="de";'
		r = self.get_mysql(query, True)
		if r:
			d['publicationStatus'] = r[0]
		query = 'select scope,name, value from property p join property_i18n pi on p.id=pi.id where object_id=' + item + ';'
		r = self.get_mysql(query, False)
		alternativeIdentifiers = []
		alternativeIdentifierLabels = []
		language = []
		script = []
		languageOfDescription = []
		scriptOfDescription = []
		if r:
			for line in r:
				if line[0]:
					if line[0] == 'alternativeIdentifiers':
						alternativeIdentifiers.append(line[2])
						alternativeIdentifierLabels.append(line[1])
				else:
					if line[1] == 'language':
						m = re.search('"([^"]*)"', line[2])
						if m:
							language.append(m.group(1))
					if line[1] == 'script':
						m = re.search('"([^"]*)"', line[2])
						if m:
							script.append(m.group(1))
					if line[1] == 'languageOfDescription':
						m = re.search('"([^"]*)"', line[2])
						if m:
							languageOfDescription.append(m.group(1))
					if line[1] == 'scriptOfDescription':
						m = re.search('"([^"]*)"', line[2])
						if m:
							scriptOfDescription.append(m.group(1))

			d['languageOfDescription'] = ('|').join(languageOfDescription)
			d['scriptOfDescription'] = ('|').join(scriptOfDescription)
			d['alternativeIdentifiers'] = ('|').join(alternativeIdentifiers)
			d['alternativeIdentifierLabels'] = ('|').join(alternativeIdentifierLabels)
			d['language'] = ('|').join(language)
			d['script'] = ('|').join(script)
		query = 'select tai.name, ti.name from object_term_relation o join term t on o.term_id=t.id join term_i18n ti on t.id=ti.id join taxonomy_i18n tai on t.taxonomy_id=tai.id where o.object_id=153735 and tai.culture="en";'
		r = self.get_mysql(query, False)
		if r:
			subjectAccessPoints = []
			placeAccessPoints = []
			genreAccessPoints = []
			for line in r:
				if line[0] == 'Places':
					placeAccessPoints.append(line[1])
				if line[0] == 'Subjects':
					subjectAccessPoints.append(line[1])
				if line[0] == 'Genre':
					genreAccessPoints.append(line[1])

			d['subjectAccessPoints'] = ('|').join(subjectAccessPoints)
			d['placeAccessPoints'] = ('|').join(placeAccessPoints)
			d['genreAccessPoints'] = ('|').join(genreAccessPoints)
		query = 'select  authorized_form_of_name from relation r join actor_i18n ai on r.object_id=ai.id where type_id=161 and r.subject_id=' + item + ';'
		r = self.get_mysql(query, False)
		if r:
			nameAccessPoints = []
			for line in r:
				nameAccessPoints.append(line[0])

			d['nameAccessPoints'] = ('|').join(nameAccessPoints)
		query = 'select  date,start_date,end_date, description from event e join event_i18n ei on e.id=ei.id where e.type_id=111 and e.object_id=' + item + ' ;'
		r = self.get_mysql(query, True)
		if r:
			if r[3]:
				d['eventDescriptions'] = r[3]
			if r[0]:
				d['eventDates'] = r[0]
			if r[1]:
				d['eventStartDates'] = r[1]
			if r[2]:
				d['eventEndDates'] = r[2]
			query = 'select name from term_i18n where id=111 and culture="' + d['culture'] + '"'
			rr = r = self.get_mysql(query, True)
			if rr:
				d['eventTypes'] = rr[0]
		print(d)

	def _has_child_file(self, l, legyacId):
		"""
		"""
		pass

	def _add_keyword_info(self, l):
		i = 0
		for e in l:
			if 'keywordInfo' not in e:
				e['keywordInfo'] = self.predict_item(e['title'], e['scopeAndContent'], False, False, True)
			else:
				if e['keywordInfo'] == '':
					e['keywordInfo'] = self.predict_item(e['title'], e['scopeAndContent'], False, False, True)
			print(i, ' of ', len(l))
			i += 1

		return l.copy()

	def _get_entities(self, from_file=False):
		"""
		retrieves all related wikidata items from wikidata
		"""
		entities = []
		if from_file:
			if os.path.getsize(self.ENTITIES_FILE) > 1:
				with open(self.ENTITIES_FILE, 'r') as (file):
					entities = file.read().split('\n')
					return entities
				file.close()
		else:
			query = 'SELECT DISTINCT ?item where{\
				{optional{?item wdt:P17/wdt:P279* wd:Q329618 .}} \
				union \
				{optional{?item wdt:P2541/(wdt:P131|wdt:P279) wd:Q329618 .}}\
				union{ optional{?item wdt:P131/wdt:P279* wd:Q329618 .}} \
				union{ optional{?item wdt:P361/wdt:P279* wd:Q329618 .}}\
				union{ optional{?item wdt:P2650/wdt:P279* wd:Q329618 .}}\
				union{ optional{?item wdt:P937/(wdt:P131|wdt:P279)* wd:Q329618 .}}\
                }\
				order by ?item'
			l = self._get_from_WDQ(query)
			if len(l['results']['bindings']) > 0:
				for line in l['results']['bindings']:
					entities.append(self._short_entity(line['item']['value']))

			entities.append('Q329618')
			return entities

	def _index_keywords(self):
		"""
		Populates the KEYWORDS_FILE with dicts
		"""
		keyword = {}
		self._open_keywords()
		entities = self._get_entities(False)
		#print(entities[0:10] , len(self.KEYWORDS))
		#a=input(len(entities))
		i = len(entities)
		j = 0
		counter={"created":0,"modified":0,"not modified":0,"entities":0, "empty":[]}
		for entity in entities:
			counter['entities']+=1
			
			print ("Eintitiy: ", entity, end=" : ")
			#print('[[ ', i, ' ]]')
			i -= 1
			j += 1
			if j == 1000:
				self._store_objects()
				j = 0
				print('stored')
			keyword.clear()
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
				  bind(if(lang(?itemLabel)="de",?itemLabel,"") as ?de).}\
				  group by ?item '				
			l = self._get_from_WDQ(query)
			e=next((x for x in self.KEYWORDS if x['item'] == entity.strip()), None)
			#print("exist?", e)
			if  e is None:
				print(entity, " is unknown")
				if len(l['results']['bindings']) > 0:
					#print(".. and it has some values.")
					keyword['item'] = entity
					keyword['label_de'] = self._get_uniq(l['results']['bindings'][0]['label_de']['value'])[0]
					keyword['description'] = self._get_uniq(l['results']['bindings'][0]['description']['value'])[0]
					keyword['label_clean'] = self._clean_label(keyword['label_de'])
					instances = self._get_uniq(l['results']['bindings'][0]['instances']['value'])
					keyword['instances'] = [self._short_entity(b) for b in instances]
					keyword['search_terms'] = self._get_uniq(l['results']['bindings'][0]['altLabel']['value'])
					keyword['search_terms'].extend(self._get_uniq(l['results']['bindings'][0]['shortLabel']['value']))
					keyword['search_terms'].extend(self._get_uniq(l['results']['bindings'][0]['Label']['value']))
					keyword['search_terms'] = list(filter(None, keyword['search_terms']))
					keyword['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
					if keyword['label_de'] == '':
						if len(keyword['search_terms']) > 0:
							keyword['label_de'] = keyword['search_terms'][0]
						else:
							continue
					if keyword['label_clean'] != '':
						params = {'action': 'wbsearchentities', 
						 'props': '', 
						 'language': g.CULTURE, 
						 'search': keyword['label_clean'], 
						 'limit': 50, 
						 'format': 'json'}
						url = self.WD_API_ENDPOINT + '?' + urllib.parse.urlencode(params)
						#print(url)
						headers = {}
						headers['accept'] = 'application/sparql-results+json'
						req = urllib.request.Request(url, None, headers)
						with urllib.request.urlopen(req, timeout=30) as (response):
							the_page = response.read().decode('utf-8')
						r = requests.get(url)
						rjson = json.loads(r.text)
						keyword['frequency'] = self._get_frequency(rjson)
					self.KEYWORDS.append(keyword.copy())
					print(keyword['label_de'], " created")
					counter['created']+=1
				else:
					print(l)
					counter['empty'].append(entity)
			else:
				if len(l['results']['bindings']) > 0:
					modified=False
					label_de= self._get_uniq(l['results']['bindings'][0]['label_de']['value'])[0]
					if label_de!=e['label_de'] and label_de!='':
						modified=True
						e['label_de']=label_de
						e['label_clean']=self._clean_label(label_de)
					description=self._get_uniq(l['results']['bindings'][0]['description']['value'])[0]
					if description!=e['description']:
						modified=True
						e['description']=description
					instances = self._get_uniq(l['results']['bindings'][0]['instances']['value'])
					instance_arr=[self._short_entity(b) for b in instances]
					if instance_arr != e['instances']:
						modified=True
						e['instances']=instance_arr
					search_terms=[]
					search_terms=self._get_uniq(l['results']['bindings'][0]['altLabel']['value'])
					search_terms.extend(self._get_uniq(l['results']['bindings'][0]['shortLabel']['value']))
					search_terms.extend(self._get_uniq(l['results']['bindings'][0]['Label']['value']))
					search_terms = list(filter(None, search_terms))
					if e['search_terms']!=search_terms:
						modified=True
						e['search_terms']=search_terms
					if modified:
						e['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
						print("    ", e['label_de'], " modified")
						counter['modified']+=1
					else:
						print("    ", e['label_de'], " not modified")
						counter['not modified']+=1
				else:
					#print(entity, l)
					counter['empty'].append(entity)
					
			
		self._store_objects()
		self._generate_identic_clean_labels()
		print(counter)

	def _generate_identic_clean_labels(self):
		"""
		Counts and stores the number of identic cleaned labels inside KEYWORDS list
		"""
		self._open_keywords()
		for keyword in self.KEYWORDS:
			s = keyword['label_clean']
			keyword['identic_clean_labels'] = sum((1 for d in self.KEYWORDS if d.get('label_clean') == s))

		self._store_objects()

	def predict_item(self,item_str, hierarchy_str="", show_stats=False, authority_data=False, return_related_keywords=False):
		"""
		it's the main process to determinate, if an item belongs to the theme or not.
		Returns a score between -1 and 1 where score = 1 means, that the item fits perfectly.
		
		Parameters:
		- item_str : the string to analyse. It should be composed as a concat of title and scope_and_content
		- hierarchy_str : a string which contains all the title and scope_and_content information of parent items
		- show_stats : wheater or not showing the log of the analysis
		- authority_data : True if a list of retrieved search_terms should be returned
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
			'Q486972':30,
			'Q2385804':30,
			'Q48204':30,
			'Q43229':30,
			'Q618123':30,
			'Q483394':1,
			'Q82550':5,
			'Q2472587':40,

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



	def OLDpredict_item(self, item_str, hierarchy_str='', show_stats=False, authority_data=False, return_related_keywords=False):
		"""
		it's the main process to determinate, if an item belongs to the theme or not.
		Returns a score between -1 and 1 where score = 1 means, that the item fits perfectly.
		
		Parameters:
		- item_str : the string to analyse. It should be composed as a concat of title and scope_and_content
		- hierarchy_str : a string which contains all the title and scope_and_content information of parent items
		- show_stats : wheater or not showing the log of the analysis
		- authority_data : True if a list of retrieved search_terms should be returned
		"""
		ap = access_points()
		profession_switcher = {'BEAMTER': ['beamter', 'richter', 'jurist', 'verwalter', 'assessor', 'schriftste', 'regierung', 'gouverneur', 'leiter', 'zoll'], 
		 'MILITAER': ['offizier', 'kommandeur', 'milit', 'hauptmann', 'leutnant', 'major', 'ober'], 
		 'MISSIONAR': ['angehoerig', 'mission', 'theolog'], 
		 'ANGESTELLTER': ['heizer', 'maurer', 'schlosser', 'angestellte', 'hilfe', 'schreiber', 'zeichner'], 
		 'UNTERNEHMER': ['unternehmer', 'kaufmann', 'handler', 'pionier'], 
		 'MEDIZINER': ['arzt', 'mediziner', 'bakterio'], 
		 'WISSENSCHAFTLER': ['geograph', 'reisende', 'geologe', 'ethnologe', 'wissenschaft', 'backterio', 'forsch', 'archaolog', 'biolog', 'zoolog', 'ethnograph']}
		
		# estimated frequency of relevant occurrences in percent
		instance_factors = {'Q486972': 10, 
		 'Q5': 10, 
		 'Q327333': 20, 
		 'Q4830453': 20, 
		 'Q20746389': 30, 
		 'Q45295908': 30, 
		 'Q133156': 100, 
		 'Q15815670': 50, 
		 'Q2472587':60,
		 'Q': 0,
		 'Q486972':30,
		'Q2385804':30,
		'Q48204':30,
		'Q43229':30,
		'Q618123':30,
		'Q483394':1,
		'Q82550':5,
		 }
		 


		 
		 
		item_str += hierarchy_str
		result = []
		self._open_keywords()
		self._open_killers()
		self._open_names_suffixes()
		lower_item = item_str.lower()
		item_arr = re.findall('\\w+', lower_item)
		item_arr_str = ' ' + (' ').join(item_arr) + ' '
		lower_hierarchy = hierarchy_str.lower()
		hierarchy_arr = re.findall('\\w+', lower_hierarchy)
		killers = list(set(self.KILLERS).intersection(item_arr))
		for killer in killers:
			result.append('0||' + killer + '||||Q')

		for keyword in self.KEYWORDS:
			if self._blocked_by_description(keyword['description']):
				continue
			if 'frequency' not in keyword:
				keyword['frequency'] = 50
			professions = []
			for search_term in keyword['search_terms']:
				search_term_word_count = len(re.findall('(\\S+)', search_term))
				if len(search_term) > 3:
					if search_term_word_count == 1 and self._get_instance(keyword) == 'Q5' or item_arr_str.find(' ' + search_term.lower().replace('-', ' ') + ' ') > -1:
						result.append('4||' + search_term + '||' + str(keyword['frequency']) + '|' + keyword['item'] + '|' + self._get_instance(keyword))

			if len(keyword['label_clean']) > 3 and item_arr_str.find(' ' + keyword['label_clean'].lower() + ' ') > -1:
				result.append('1||' + keyword['label_clean'] + '||' + str(keyword['frequency']) + '|' + keyword['item'] + '|' + self._get_instance(keyword))
			if 'Q5' in keyword['instances']:
				stopped = keyword['label_de'].replace(keyword['label_clean'], '').strip(' ')
				relative_position = self._is_near(keyword['label_clean'], stopped, item_arr, [1, 1])
				if isinstance(relative_position, int):
					result.append('3|' + stopped + '|' + keyword['label_clean'] + '|' + str(relative_position) + '|' + str(keyword['frequency']) + '|' + keyword['item'] + '|Q5')
				if keyword['label_clean'] != '':
					description = keyword['description'].lower()
					if self._is_noble(keyword):
						professions.append('ADEL')
					for k, v in profession_switcher.items():
						for snippet in v:
							if description.find(snippet) != -1:
								professions.append(k)

					if len(professions) > 0:
						for profession in professions:
							for suffix in self.NAME_SUFFIXES[profession]:
								if item_str.find(suffix.lower() + ' ' + keyword['label_clean'].lower()) > -1:
									result.append('5|' + suffix + '|' + keyword['label_clean'] + '||' + str(keyword['frequency']) + '|' + keyword['item'] + '|' + self._get_instance(keyword))

			if keyword['label_clean'] != '':
				e = keyword['label_clean']
				if len(e) > 3:
					s = e.lower()
					label_clean = {}
					idx_list = [i for i, x in enumerate(item_arr) if x == s]
					if len(idx_list) > 0:
						label_clean = e + '|' + (',').join((str(x) for x in idx_list))
						result.append('1||' + label_clean + '|' + str(keyword['frequency']) + '|' + keyword['item'] + '|' + self._get_instance(keyword))
						if 'Q5' in keyword['instances']:
							for profession in professions:
								for suffix in self.NAME_SUFFIXES[profession]:
									relative_position = self._is_near(e, suffix, item_arr)
									if isinstance(relative_position, int):
										result.append('2|' + suffix + '|' + e + '|' + str(relative_position) + '|' + str(keyword['frequency']) + '|' + keyword['item'] + '|Q5')

		result = self._get_uniq(result)
		last_object = ''
		object_count = 0
		score = 0
		related_keywords = ''
		if len(result) > 0:
			if result[0] != '':
				bc = result[0][0:1]
				if bc.isnumeric():
					best_class = int(bc)
				else:
					best_class = 1
				worst_class = int(result[len(result) - 1][0:1])
				score = best_class * 100
				if worst_class == 0:
					score -= 200
				finding_class = best_class
				i = -1
				last_moon_earth = ''
				while finding_class == best_class:
					i += 1
					if i == len(result):
						break
					lst = result[i].split('|')
					if len(lst) == 1:
						score = 0
						break
					finding_class = int(lst[0])
					if finding_class != best_class:
						break
					if last_moon_earth == lst[1] + ' ' + lst[2]:
						continue
					last_moon_earth = lst[1] + ' ' + lst[2]
					if lst[1] != '':
						moon = 25
					else:
						moon = 0
					if lst[2] != '':
						earth = len(re.findall('(\\S+)', lst[2])) * 5 + len(lst[2])
					else:
						score = 0
						break
					if lst[4] != '':
						frequency = int(lst[4]) * -1
					else:
						frequency = -100
					if lst[3] != '':
						if lst[1] == '':
							occ_arr = lst[3].split(',')
							occurance = (len(item_arr) - int(occ_arr[0])) / len(item_arr) * 20
							position = 0
						else:
							occurance = 0
							position = (6 - abs(int(lst[3]))) * 4
					else:
						position = 0
						occurance = 0
					if lst[5] != last_object:
						object_count += 1
					if lst[6] != '':
						instance_factor = instance_factors[lst[6]]
					else:
						instance_factor = 1
					score += frequency + occurance + position + earth + moon + object_count * 30 * best_class
					if show_stats:
						print('score+=frequency +occurance + position +earth+moon +object_count')
						print(score, '|', frequency, occurance, position, earth, moon, object_count)
				else:
					score = 0

		else:
			score = 0
		print('-------------------------------------')
		if authority_data:
			r_list = []
			if len(result) > 0 and result[0] != '':
				for e in result:
					lst = e.split('|')
					if int(lst[0]) > 2:
						r = next((item for item in self.KEYWORDS if item['item'] == lst[5]), False)
						if r and 'label_de' in r and self._get_access_point(lst[6]):
							if 'Q5' in r['instances']:
								label = ap._reorder_name(r['label_de'])
							else:
								label = r['label_de']
							if (
							 label, self._get_access_point(lst[6])) not in r_list:
								r_list.append((label, self._get_access_point(lst[6])))

			return r_list
		elif return_related_keywords:
			releated_keywords = ''
			for r in result:
				lst = r.split('|')
				if lst[0].isnumeric():
					if show_stats:
						print(lst, result, int(lst[0]), lst[5])
					if int(lst[0]) > 1:
						kw = next((x for x in self.KEYWORDS if x['item'] == lst[5]), False)
						if kw:
							if 'description' in kw:
								related_keywords += lst[0] + '  ' + kw['label_de'] + ' (' + kw['description'] + ')\n'
							else:
								related_keywords += lst[0] + '  ' + kw['label_de'] + '\n'
						else:
							if show_stats:
								print('below 2')

			return related_keywords
		elif score > 300:
			return True
		else:
			return False

	def _ai_predict(self, l):
		"""
		predict csv archival discriptions using neuronal nets. Writes the result into e['prediction']
		"""
		from atom.helpers.ai import ai
		ki = ai()
		sum1 = 0
		total = 0
		for e in l:
			test_str = ''
			if 'title' in e:
				test_str = e['title'] + ' '
			if 'scopeAndContent' in e:
				test_str += e['scopeAndContent'] + ' '
			if 'arrangement' in e:
				test_str += e['arrangement']
			if 'levelOfDescription' in e and e['levelOfDescription'] == 'File':
				e['prediction'] = ki.predict(test_str)
				total += 1
				sum1 += e['prediction']

		print('Predicted: ', str(sum1), ' over ', str(total), ' as correct.')
		return l.copy()

	def _add_access_points(self, l, files_only=True):
		ap = access_points()
		total = 0
		sum_subject_ap = 0
		sum_place_ap = 0
		sum_genre_ap = 0
		for e in l:
			total += 1
			test_str = ''
			if 'title' in e:
				test_str = e['title'] + ' '
			if 'scopeAndContent' in e:
				test_str += e['scopeAndContent'] + ' '
			if 'arrangement' in e:
				test_str += e['arrangement']
			if 'culture' in e:
				culture = e['culture']
			else:
				culture = g.CULTURE
			if files_only and 'levelOfDescription' in e and e['levelOfDescription'] != 'File':
				continue
			print(test_str[0:100])
			r = ap.find_other_access_points_in_text(test_str, culture, True)
			sum_subject_ap += len(r[ap.SUBJECT_AP])
			sum_place_ap += len(r[ap.PLACE_AP])
			sum_genre_ap += len(r[ap.GENRE_AP])
			if r != {ap.SUBJECT_AP: [], ap.PLACE_AP: [], ap.GENRE_AP: []}:
				if len(r[ap.SUBJECT_AP]) > 0:
					if 'subjectAccessPoints' in e:
						e['subjectAccessPoints'] = ('|').join(list(set(r[ap.SUBJECT_AP]).union(set(e['subjectAccessPoints'].split('|')))))
					else:
						e['subjectAccessPoints'] = ('|').join(r[ap.SUBJECT_AP])
					if len(r[ap.PLACE_AP]) > 0:
						if 'placeAccessPoints' in e:
							e['placeAccessPoints'] = ('|').join(list(set(r[ap.PLACE_AP]).union(set(e['placeAccessPoints'].split('|')))))
						else:
							e['placeAccessPoints'] = ('|').join(r[ap.PLACE_AP])
						if len(r[ap.GENRE_AP]) > 0:
							if 'genreAccessPoints' in e:
								e['genreAccessPoints'] = ('|').join(list(set(r[ap.GENRE_AP]).union(set(e['genreAccessPoints'].split('|')))))
							else:
								e['genreAccessPoints'] = ('|').join(r[ap.GENRE_AP])
							e['subjectAccessPoints'] = so.replaceChars(e['subjectAccessPoints'], '||', '|')
							e['subjectAccessPoints'] = e['subjectAccessPoints'].strip('|')
						print(r)

		print('new subjects:', sum_subject_ap, ' new places:', sum_place_ap, ' new genre:', sum_genre_ap)
		return l.copy()

	def _create_slug(self, phrase):
		"""
		Creates a new slug for a specific phrase,checks existing slug with the same phrase on AtoM mysql
		"""
		phrase = slugify(phrase)
		sql = 'select slug from slug where slug = "' + phrase + '" or slug like "' + phrase + '-_" or slug like "' + phrase + '-__" or slug like "' + phrase + '-___" order by slug;'
		result_sql = self.get_mysql(sql, False)
		best = 0
		if result_sql:
			for result in result_sql:
				i = result[0][result[0].rfind('-') + 1:]
				if i.isnumeric() and int(i) > best:
					best = int(i)

		else:
			return phrase
		return phrase + '-' + str(best + 1)

	def _get_access_point(self, wdid):
		"""
		returns the AtoM event type of an WD item 
		
		Parameter:
		wdid : The Wikidata ID of the keyword
		"""
		if wdid != '':
			return g.ACCESS_POINTS[self._get_instance('', wdid)]
		else:
			return False

	def _get_level_of_description(self, htype):
		if htype in self.LEVELS_OF_DESCRIPTION:
			return self.LEVELS_OF_DESCRIPTION[htype]
		else:
			return htype

	def _is_noble(self, keyword):
		is_noble = False
		arr = re.findall('\\w+', keyword['label_de'])
		for e in keyword['search_terms']:
			arr.extend(re.findall('\\w+', e))

		for e in arr:
			if e in ('von', 'zu'):
				return True

		return False

	def _is_near(self, earth, moon, lst, distance=[
 6, 5], position_statement=True):
		"""
		Checks if a string (moon) is around another string (earth) within a list of strings.
		
		"""
		earth = earth.lower()
		moon = moon.lower()
		for i in range(0, len(lst)):
			e = lst[i]
			if e == earth:
				sub = [
				 i - distance[0], i + distance[1]]
				if sub[0] < 0:
					sub[0] = 0
				if moon in lst[sub[0]:sub[1]]:
					if position_statement:
						for j in range(sub[0], sub[1]):
							if lst[j] == moon:
								return i - j

					else:
						return 'True'

		return 'False'

	def _get_instance(self, keyword='', s=''):
		"""
		returns the main instance of a keyword
		
		Parameters:
		keyword : if set it represents an object of the self.KEYWORDS list
		s: if set it represents a string with a wdid 
		"""
		if keyword:
			lst = keyword['instances']
		else:
			lst = [
			 s]
		if len(list(set(lst).intersection(['Q5', 'Q1371796', 'Q189290']))) > 0:
			return 'Q5'
		if len(list(set(lst).intersection(['Q2061186', 'Q1384677', 'Q879146', 'Q20746389', 'Q1564373', 'Q384003', 'Q20746389', 'Q732948']))) > 0:
			return 'Q20746389'
		if len(list(set(lst).intersection(['Q4830453', 'Q567521', 'Q740752', 'Q1807108', 'Q188913', 'Q6500733', 'Q50825050', 'Q6881511','Q4830453']))) > 0:
			return 'Q4830453'
		if len(list(set(lst).intersection(['Q48074023', 'Q48069095', 'Q362636', 'Q47537097', 'Q33874813', 'Q19298672', 'Q327333', 'Q22687', 'Q1321241', 'Q16239032', 'Q47537101', 'Q47538966', 'Q854140', 'Q854399', 'Q41762668', 'Q334453']))) > 0:
			return 'Q327333'
		if len(list(set(lst).intersection(['Q45295908']))) > 0:
			return 'Q45295908'
		if len(list(set(lst).intersection(['Q486972', 'Q3024240', 'Q164142']))) > 0:
			return 'Q486972'
		if len(list(set(lst).intersection(['Q15815670', 'Q41397', 'Q188055', 'Q178561', 'Q124734', 'Q198', 'Q38723', 'Q18564543', 'Q831663']))) > 0:
			return 'Q15815670'
		if len(list(set(lst).intersection(['Q2385804', 'Q31855', 'Q3914', 'Q33506', 'Q50825046']))) > 0:
			return 'Q2385804'
		if len(list(set(lst).intersection(['Q48204', 'Q847017']))) > 0:
			return 'Q48204'
		if len(list(set(lst).intersection(['Q43229', 'Q108696', 'Q294163', 'Q2385804', 'Q50825057', 'Q4686866', 'Q8425', 'Q719456']))) > 0:
			return 'Q43229'
		if len(list(set(lst).intersection(['Q2472587', 'Q41710']))) > 0:
			return 'Q2472587'
		if len(list(set(lst).intersection(['Q133156', 'Q16917', 'Q4260475']))) > 0:
			return 'Q133156'
		if len(list(set(lst).intersection(['Q618123', 'Q33837', 'Q133156', 'Q486972',  'Q41762037', 'Q6256', 'Q1306755', 'Q618123', 'Q190354', 'Q164142', 'Q7404467', 'Q748149', 'Q112099', 'Q486972', 'Q3624078', 'Q1402592', 'Q250811', 'Q183366', 'Q3024240']))) > 0:
			return 'Q618123'
		if len(list(set(lst).intersection(['Q483394']))) > 0:
			return 'Q483394'
		if len(list(set(lst).intersection(['Q82550', 'Q1002697', 'Q93288', 'Q8142', 'Q19832486','Q41298']))) > 0:
			return 'Q82550'
		return ''

	def _get_wd_from_search_term(self, term, strict=True):
		"""
		returns a list of self.KEYWORDS elements matching term
		"""
		self._open_keywords()
		result = []
		term_arr = arr = re.findall('\\w+', term)
		for keyword in self.KEYWORDS:
			for search_term in keyword['search_terms']:
				search_term_arr = re.findall('\\w+', search_term)
				if len(set(term_arr).intersection(search_term_arr)) == len(term_arr):
					if strict:
						if len(term_arr) == len(search_term_arr):
							result.append(keyword)
						else:
							result.append(keyword)

		return result

	def information_object_generator(self, fields=(
 'ii.title', 'ii.scope_and_content', 'ii.arrangement'), levels=range(0, 1000000), concat=True, culture=('de')):
		"""
		generates out AtoM's information_object_i18n table line by line
		Field names must be noted as ii.field_name (if in atom.information_object_i18n) or i.field_name (if in atom.information_object)
		Levels schould be noted as id. To get the correct id, please use the following sql statement in atom database: 
			select count(i.id), level_of_description_id , name from information_object i left join term_i18n ti on i.level_of_description_id = ti.id  where ti.culture in ("de","en") group by level_of_description_id;
		
		"""
		sql = 'select i.id,i.level_of_description_id,o.updated_at, ' + (',').join(fields) + ' from information_object_i18n ii join information_object i on i.id=ii.id join object o on o.id=i.id where culture = "' + str(culture) + '";'
		#print(sql)
		r = self.get_mysql(sql, False, False)
		if r:
			for e in r:
				if e[1] in levels:
					if concat:
						yield {'id': e[0], 'level_of_description_id': e[1],'last_modified':e[2], 'cargo': (' ').join([str(x) for x in e if x is not None])}
					else:
						d={}
						d['id']=e[0]
						d['level_of_description_id']=e[1]
						for i in range(0,len(fields)):
							#if not (e[i+2] is None):
							d[fields[i]]=e[i+3]
						
						yield d.copy()

	def search_term_generator(self, from_term='', without_frequent_names=True, return_keyword_object=False, messages=True, predefined=[]):
		
		"""
		is a generator for search terms to use when query external databases
		
		Parameters:
		from_term : Skip all term which come before from_term in keywords.json
		"""
		not_to_provide=["hauses","marianne", "engelberg", "sb"]
		
		if predefined !=[]:
			for e in predefined:
				yield (e,"")
			return
		
		tmp = []
		ap = access_points()
		if messages:
			print('OK, we will start from : ' + from_term)
		skip = False
		if from_term != '':
			skip = True
		self._open_keywords()
		for keyword in self.KEYWORDS:
			if keyword['label_clean'] == '':
				continue
			if self._blocked_by_description(keyword['description']):
				if messages:
					print(keyword['label_clean'], ' is blocked by description.')
				continue
			if without_frequent_names and self.is_frequent_name(keyword['label_de']):
				if messages:
					print(keyword['label_de'], ' is a frequent name.')
				continue
			for item in keyword['search_terms']:
				if item == from_term:
					skip = False
				if skip:
					if messages:
						print('skipping ', item)
					continue
				if without_frequent_names and not self.is_frequent_name(keyword['label_de']):
					if item.find(' ') == -1 and self._get_instance(keyword) == 'Q5':
						if messages:
							print(keyword['label_de'], ' has no white space')
					else:
						tmp.append((ap._reorder_name(item, self._get_instance(keyword)), self._get_instance(keyword), keyword['item']))
						tmp.append((item, self._get_instance(keyword), keyword['item']))
				else:
					tmp.append((item, self._get_instance(keyword), keyword['item']))

			if keyword['label_clean'] == from_term:
				skip = False
			if skip:
				if messages:
					print('skipping ', keyword['label_clean'])
				continue
			if without_frequent_names and not self.is_frequent_name(keyword['label_de']):
				if keyword['label_clean'].find(' ') == -1 and self._get_instance(keyword) == 'Q5':
					if messages:
						print(keyword['label_de'], ' has no white space')
				else:
					tmp.append((keyword['label_clean'], self._get_instance(keyword), keyword['item']))
					tmp.append((ap._reorder_name(keyword['label_clean'], self._get_instance(keyword)), self._get_instance(keyword), keyword['item']))
					#print((keyword['label_clean'], self._get_instance(keyword), keyword['item']))
			else:
				tmp.append((keyword['label_clean'], self._get_instance(keyword), keyword['item']))
		#print (tmp)
		tmp = list(set(tmp))
		#print (tmp)
		tmp.sort(key=lambda x: x[0])

		for e in tmp:
			kw = next((x for x in self.KEYWORDS if x['item'] == e[2]), False)
			if return_keyword_object:
				if e[0].lower() in not_to_provide or self._is_excluded(e[0],kw):
					continue
				kw_obj = {'id': kw['item'], 
				 'label': kw['label_de'], 
				 'description_identifier': 'http://www.wikidata.org/entity/' + kw['item'], 
				 'indicators': kw['search_terms'] if 'search_terms' in kw else [], 
				 'exclusions': [], 
				 'entity_type': e[1], 
				 'parent_id': None, 
				 'description': kw['description'] if 'description' in kw else '',
				 'last_modified':kw['timestamp'] if 'timestamp' in kw else '2000-01-01 00:00:00'
				 }
				yield (e[0], e[1], kw_obj)
			else:
				if e[0].lower() in not_to_provide or self._is_excluded(e[0],kw):
					continue
				yield (
				 e[0], e[1])


	def _is_excluded(self, search_term, keyword):
		"""
		returns True if search_term is part of the "exclusions" list inside the keyword item
		"""
		if "exclusions" in keyword:
			for e in keyword['exclusions']:
				if e.lower().strip()==search_term.lower().strip():
					return True
		return False

	def _blocked_by_description(self, s):
		blocker = [
		 'eisenbahnverwaltung', 'schreiber', 'heizer', 'polizei', 'polizist']
		for block_term in blocker:
			if s.lower().find(block_term) > -1:
				return True

		return False

	def imports(self, source,record_type="archival_description", sourcefile='', fromfile='', update=True, from_term="", source_str="", archive_url="", archive_id="",predefined=[]):
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
				if source == 'FBN':
					from atom.imports import findbuch
					proc=findbuch.Findbuch(archive_url,archive_id)
				if source =='KAL':
					proc=Kalliope()
				for export in proc.export(g.MIN_EXPORT_LIST_LEN,from_term,predefined):
					l=export
					print("länge export",len(l))
					self._add_to_current_identifiers(l)
					print("EXPORTED ------------------------------------------------------------------------------------------")
					#pprint.pprint(l)
					pa=self.TMP_RESULTS_PATH+time.strftime("%Y%m%d_%H%M%S")
					pathlib.Path(pa).mkdir(parents=True, exist_ok=True) 
					self.write_csv(l,"/tmp/import.csv",record_type)
					self._post_import(l,pa)
					self.write_csv(l,pa+"/import.csv",record_type)
			else:
				with open(fromfile,'r') as json_in:
					l = json.load(json_in)
					print(len(l))
				json_in.close
			#self.write_to_index(l, record_type, source)
			print (err)

			self.join_csv()
			return len(l)
		#except:
		else:
			print("Error:", sys.exc_info()[0])
			return

	def _init_legacy_ids(self):
		"""
		fills the self.LEGACY_IDS with id from the stored csv files at begin of th programm execution
		"""
		if len(self.LEGACY_IDS) == 0:
			path = self.TMP_RESULTS_PATH
			for root in os.walk(path):
				print(root[0])
				print('/import.csv')
				if root[0] == self.TMP_RESULTS_PATH:
					continue
				if os.path.isfile(root[0] + '/import.csv'):
					print(root[0] + '/import.csv')
					l = fo.load_data(root[0] + '/import.csv')
					for e in l:
						if 'legacyId' in e:
							self.LEGACY_IDS.append(e['legacyId'])
						else:
							print('No legacyId for init')

	def _add_to_legacy_ids(self, legacyId):
		if legacyId not in self.LEGACY_IDS:
			self.LEGACY_IDS.append(legacyId)

	def _post_import(self, l, pname='', sort=True, add_keywords=True, create_parent_entries=False):
		"""
		checks the integrity of the imported data. 
		It manages also the access points. Original accesspoint will be logged and replecaed by self.KEYWORDS
		
		Parameter
		l: a list of dict which represents the AtoM shema for csv imports
		pname:
		sort:
		add_keywords: True if default keywords should replaces by keywords from the own keyword list
		create_parent_entries: True if parent items should be created if not present. This feature tries to use the ISIL from INSTITUTION as well as the order of arrangement
		"""
		ap_log = ''
		added_ap_log = ''
		errors_log = ''
		levels = {}
		match_tuples = []
		self._init_legacy_ids()
		tree = '\nTREE VIEW\n=================================='
		rank = {'Fonds': 4, 
		 'Class': 5, 
		 'Series': 6, 
		 'Subseries': 7, 
		 'File': 8, 
		 'Item': 9, 
		 'Collection': 3, 
		 'Tektonik_collection': 2, 
		 'Institution': 1, 
		 '?': 1, 
		 ' ': 1, 
		 '': 1}
		if sort:
			self.hierarchy_sort(l)[0]
		for e in l:
			for fieldname in self.ARCHIVAL_DESCRIPTIONS_FIELDS:
				if fieldname not in e:
					e[fieldname] = ''
				if not e[fieldname]:
					e[fieldname] = ''

			indent = rank[self._get_level_of_description(e['levelOfDescription'].capitalize())]
			tree += indent * '  ' + e['legacyId']
			if e['parentId'] != '':
				match_tuple = self.is_in_atom(e['parentId'], True, True)
				if match_tuple and match_tuples not in match_tuples:
					match_tuples.append(match_tuple)
			if e['levelOfDescription'] in levels:
				levels[e['levelOfDescription']] += 1
			else:
				levels[e['levelOfDescription']] = 1
			if e['title'] == '':
				errors_log += 'Item  ' + e['legacyId'] + " don't has a title!\n"
			tree += '	' + e['title'] + '\n'
			if e['parentId'] != '':
				if e['parentId'] not in self.LEGACY_IDS and not self.is_in_atom(e['parentId']):
					if create_parent_entries:
						r = self._create_parents(e, l)
						if r:
							l.extend(r[0])
							e['parentId'] = r[1]['parentId']
							errors_log += 'New parents created for item ' + e['legacyId'] + '\n'
					else:
						errors_log += 'Parent ' + e['parentId'] + ' is unknown! Level of description is ' + e['levelOfDescription'] + ' Arrangement:' + e['arrangement'] + '\n'
			else:
				if create_parent_entries:
					r = self._create_parents(e, l)
					if r:
						l.extend(r[0])
						e['parentId'] = r[1]['parentId']
						errors_log += 'New parents created for item ' + e['legacyId'] + '\n'
				else:
					errors_log += 'Item ' + e['legacyId'] + ' has no parent! Level of description is ' + e['levelOfDescription'] + ' Arrangement:' + e['arrangement'] + '\n'
				if e['legacyId'] not in self.LEGACY_IDS and e['legacyId'] != '':
					self.LEGACY_IDS.append(e['legacyId'])
				else:
					errors_log += 'Item ' + e['legacyId'] + ' is a duplicate!\n'
				if e['legacyId'] == '':
					errors_log += 'Item ' + e['title'] + ' has no legacyId!\n'
				if add_keywords and pname != '':
					for ap in ['subjectAccessPoints', 'placeAccessPoints', 'nameAccessPoints']:
						if ap in e and e[ap] != '':
							tmp = e[ap].split('|')
							e[ap] = ''
							for t in tmp:
								ap_log += ap + '\t' + e['legacyId'] + '\t' + t + '\n'

					if e['scopeAndContent'] != '':
						e['scopeAndContent'] = e['scopeAndContent'].replace('<br />', ' ')
						scope = e['scopeAndContent']
					else:
						scope = ''
					ap_list = self.predict_item(e['title'] + ' ' + scope, '', False, True)
					for keyword in ap_list:
						added_ap_log += ap + '\t' + e['legacyId'] + '\t' + keyword[0] + '\tadded to\t' + e['title'] + '\n'
						if keyword[1] in e:
							if e[keyword[1]] == '':
								e[keyword[1]] = keyword[0]
							else:
								e[keyword[1]] += '|' + keyword[0]
						else:
							e[keyword[1]] = keyword[0]

		self.hierarchy_sort(l)
		r = []
		seen = set()
		for e in l:
			if e['legacyId'] not in seen:
				r.append(e)
				seen.add(e['legacyId'])

		l = r
		print(pname)
		if pname != '':
			pname += '/'
		else:
			pname = self.TMP_RESULTS_PATH
		with open(pname + 'import_preparation.log', 'w') as (f):
			f.write('LOG FILE FOR ' + pname + '/import.csv')
			f.write(str(len(l)))
			f.write('\n----------------------\n\n')
			f.write(json.dumps(levels, indent=2) + '\n\n')
			f.write(errors_log + '\n\n' + ap_log)
			f.write('\n----------------------\n\n' + added_ap_log)
			f.write(tree)
		f.close()
		return l

	def _create_parents(self, e, l):
		"""
		Tries to create parent items up to institution level if not provided
		Returns a list of parent items
		
		"""
		hasArrangement = False
		hasIsil = False
		hasParentId = False
		r_list = []
		if e['levelOfDescription'] in ('Institution', 'Collection', 'Tektonik_collection'):
			return
		l = []
		arrangement_arr = e['arrangement'].split(' - ')
		if len(arrangement_arr) > 1:
			hasArrangement = True
		isil = self.get_isil(e['repository'])
		if isil:
			hasIsil = True
		if e['parentId'] != '':
			hasParentId = True
		if hasIsil and not hasArrangement:
			query = 'select id from repository where identifier="' + isil + '";'
		parentId = self.get_top_archival_legacy_id_by_institution_name(e['repository'])
		if not parentId:
			d = self.create_empty_dict(e)
			if hasIsil:
				d['legacyId'] = isil
			else:
				d['legacyId'] = str.replace(e['repository'], ' ', '_').lower()
			d['levelOfDescription'] = 'Institution'
			d['culture'] = 'de'
			d['title'] = e['repository']
			d['repository'] = e['repository']
			e['parentId'] = d['legacyId']
			element = next((x for x in l if x['legacyId'] == d['legacyId']), None)
			parentId = d['legacyId']
			if not element:
				r_list.append(d.copy())
		d = self.create_empty_dict(e)
		d['parentId'] = parentId
		d['legacyId'] = parentId + '_Collection'
		if e['arrangement'].find('Autographensammlung') > -1:
			d['title'] = 'Autographensammlung'
		else:
			d['title'] = 'Sonstige'
		d['levelOfDescription'] = 'Collection'
		d['repository'] = e['repository']
		e['parentId'] = d['legacyId']
		element = next((x for x in l if x['legacyId'] == d['legacyId']), None)
		if not element:
			r_list.append(d.copy())
		if len(r_list) > 0:
			return (r_list, e)
		else:
			return False

	def create_empty_dict(self, e):
		d = e.copy()
		for k in e.keys():
			d[k] = ''

		d['culture'] = e['culture']
		return d.copy()

	def get_isil(self, institution):
		"""
		retrieves the ISIL from institution name
		"""
		self._open_institutions()
		institution = institution.lower()
		e = next((x for x in self.INSTITUTIONS if x['itemLabel'].lower() == institution), False)
		if e and 'isil' in e:
			return e['isil']
		return False

	def get_isil_from_repository(self, institution):
		"""
		retrieves the isil from a repository which is already know by AtoM
		"""
		sql = 'select a.id, authorized_form_of_name, r.identifier from actor_i18n a join repository_i18n ri on a.id=ri.id join repository r on r.id=ri.id where authorized_form_of_name like"%' + institution + '%";'
		r = self.get_mysql(sql, True, False)
		if r and r[2]:
			return r[2]
		return False

	def get_top_archival_legacy_id_by_institution_name(self, institution):
		"""
		returns the first top level description id from an institution if it is a Collection, Fond or Tektonik-Collection
		
		Parameter:
		institution : name of the institution (case sensitive)
		"""
		sql = 'select a.id, authorized_form_of_name, r.identifier,ti.name, i.id, k.source_id \t\tfrom actor_i18n a join repository_i18n ri on a.id=ri.id \t\tjoin repository r on r.id=ri.id \t\tjoin information_object i on a.id=i.repository_id \t\tjoin term_i18n ti on i.level_of_description_id=ti.id \t\tjoin keymap k on k.target_id=i.id \t\twhere authorized_form_of_name like"%' + institution + '%" and i.parent_id=1 and ti.culture="de" ;'
		r = self.get_mysql(sql, False, False)
		if r:
			for line in r:
				if line[3] in ('Institution', 'Collection', 'Fonds', 'Tektonik_collection'):
					return line[5]

		return False

	def _add_to_current_identifiers(self, l):
		for e in l:
			if e['legacyId'] not in self.CURRENT_DESCRIPTION_IDENTIFIERS:
				self.CURRENT_DESCRIPTION_IDENTIFIERS.append(e['legacyId'])

	def export(self, record_type='archival_description'):
		"""
		Takes confirmed records from candidates index to AtoM csv import format
		
		Marks taken records as exported		 
		Returns a file tmp/export.csv
		
		"""
		d = self.es.search(index='candidates', doc_type=record_type, body={'size': 200000, 'query': {'match': {'_original_from': 'DDB'}}})
		l = []
		for hit in d['hits']['hits']:
			e = hit['_source']
			if 'culture' not in e:
				e['culture'] = g.CULTURE
			if 'acquisition' in e:
				print(e['acquisition'])
				acquisition = e['acquisition']
				#parent_list = self.(e['legacyId'])
				for ee in parent_list:
					if 'acquisition' not in ee:
						self.es.update(index='candidates', doc_type=record_type, id=ee['legacyId'], body={'doc': {'acquisition': acquisition}})
						parent_element = next((item for item in d['hits']['hits'] if item['_id'] == ee['legacyId']), None)
						parent_element['acquisition'] = acquisition
						if 'levelOfDescription' in parent_element and parent_element['levelOfDescription'] == 'Fonds':
							break

			l.append(e)
			self.es.update(index='candidates', doc_type=record_type, id=e['legacyId'], body={'doc': {'_status': 'exported'}})

		self.hierarchy_sort(l)[0]
		count = 0
		self.write_csv(l, 'tmp/export.csv', record_type)
		self.check_exported(record_type)
		self.split_export_files(record_type)
		return len(l)

	def strip_tags(self, culture='de'):
		print('start stripping tags')
		sql = "select scope_and_content,id from information_object_i18n where culture='" + culture + "'; "
		l = self.get_mysql(sql, False)
		print(len(l), ' records')
		i = 0
		for e in l:
			if e[0] is not None:
				i += 1
				text = str(e[0])
				new_text = so.stripBr(text)
				new_text = so.stripHtml(new_text, ' ')
				new_text = so.dedupSpaces(new_text)
				new_text = so.replaceChars(new_text, "'", '')
				new_text = so.replaceChars(new_text, '', '')
				if new_text != text:
					sql = "update information_object_i18n set scope_and_content ='" + new_text + "' where id=" + str(e[1]) + " and culture='" + culture + "';"
					print(sql)
					a = self.get_mysql(str(sql), False, True)

		print(i, ' records modified')

	def write_csv(self, l, filename, record_type='archival_description'):
		"""
		Writes a list of dict into a csv file depending on record_type
		
		Parameter:
		filename: relative path and file name of the csv file to create
		record_type: either archival_descripition or authority_record
		
		"""
		with open(filename, 'w') as (csvfile):
			if record_type == 'archival_description':
				fieldnames = [
				 'search term','prediction', 'out', 'legacyId', 'parentId', 'qubitParentSlug', 'identifier', 'accessionNumber', 'title', 'levelOfDescription', 'extentAndMedium', 'repository', 'archivalHistory', 'acquisition', 'scopeAndContent', 'appraisal', 'accruals', 'arrangement', 'accessConditions', 'reproductionConditions', 'language', 'script', 'languageNote', 'physicalCharacteristics', 'findingAids', 'locationOfOriginals', 'locationOfCopies', 'relatedUnitsOfDescription', 'publicationNote', 'digitalObjectURI', 'generalNote', 'subjectAccessPoints', 'placeAccessPoints', 'nameAccessPoints', 'genreAccessPoints', 'descriptionIdentifier', 'institutionIdentifier', 'rules', 'descriptionStatus', 'levelOfDetail', 'revisionHistory', 'languageOfDescription', 'scriptOfDescription', 'sources', 'archivistNote', 'publicationStatus', 'physicalObjectName', 'physicalObjectLocation', 'physicalObjectType', 'alternativeIdentifiers', 'alternativeIdentifierLabels', 'eventDates', 'eventTypes', 'eventStartDates', 'eventEndDates', 'eventDescriptions', 'eventActors', 'eventActorHistories', 'eventPlaces', 'culture', 'status']
				fieldnames = self.ARCHIVAL_DESCRIPTIONS_FIELDS
			if record_type == 'authority_record':
				fieldnames = [
				 'culture', 'typeOfEntity', 'authorizedFormOfName', 'corporateBodyIdentifiers', 'datesOfExistence', 'history', 'places', 'legalStatus', 'functions', 'mandates', 'internalStructures', 'generalContext', 'descriptionIdentifier', 'institutionIdentifier', 'rules', 'status', 'levelOfDetail', 'revisionHistory', 'sources', 'maintenanceNotes']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore', delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for e in l:
				writer.writerow(e)

		csvfile.close()

	def read_csv(self, filename):
		"""
		depriciated
		"""
		with open(filename) as (csvfile):
			reader = csv.DictReader(csvfile)
			meta = []
			l = [row for row in reader]
		return l.copy()

	def split_export_files(self, record_type, include_acquisition=False):
		"""
		This reopens the export.csv and creates export csv files for each combination found of repository and acquisition
		"""
		with open('tmp/export.csv') as (csvfile):
			reader = csv.DictReader(csvfile)
			meta = []
			l = [row for row in reader]
		if record_type == 'archival_description':
			directory = 'tmp/export/archival_description/'
			for e in l:
				if 'acquisition' in e and include_acquisition:
					ee = self.in_meta(meta, e['repository'], e['acquisition'])
				else:
					ee = self.in_meta(meta, e['repository'])
				if ee:
					print('-->' + ee['repository'])
					if 'elements' in ee:
						ee['elements'].append(e.copy())
					else:
						ee['elements'] = []
						ee['elements'].append(e.copy())
				else:
					d = {}
					d['repository'] = e['repository']
					if 'acquisition' in e and include_acquisition:
						d['acquisition'] = e['acquisition']
					d['elements'] = []
					d['elements'].append(e.copy())
					meta.append(d.copy())
					d.clear()

		for e in meta:
			if 'acquisition' in e and include_acquisition:
				filename = ('').join((el for el in e['repository'].capitalize() if el.isalnum())) + '__' + ('').join((el for el in e['acquisition'].capitalize() if el.isalnum())) + '.csv'
			else:
				filename = ('').join((el for el in e['repository'].capitalize() if el.isalnum())) + '.csv'
			self.write_csv(e['elements'], directory + filename, record_type)
			print(filename)

		csvfile.close()

	def in_meta(self, meta, repository, acquisition=None):
		"""
		This helps split_export_files()
		"""
		print(repository)
		print(acquisition)
		for ee in meta:
			if ee['repository'] == repository:
				if acquisition:
					if ee['acquisition'] == acquisition:
						return ee
					else:
						return ee

	def sort_file(self, filename, record_type):
		l = fo.load_data(filename)
		self.hierarchy_sort(l)[0]
		self.write_csv(l, filename, record_type)

	def get_jump_fonds(self):
		"""
		Returns a list of Fonds which are already included completly 
		"""
		return self.jump_fonds

	def drop_empty_eventdates(self, d):
		"""
		A work around for a ElasticSearch bug.
		
		Due to a bug (?) in elasticsearch.py eventStartDates or eventEndDates must be filled with a date string.
		Search will not working if both are empty. So this workaround will eliminate those if emty
		
		Parameter:
		- d: record dict
		
		"""
		if 'eventStartDates' in d and d['eventStartDates'] == '':
			d.pop('eventStartDates', None)
		if 'eventEndDates' in d and d['eventEndDates'] == '':
			d.pop('eventEndDates', None)
		return d

	def lookup_orphans(self, record_type='archival_description', source='DDB'):
		"""
		retrieve orphans from the candidate index depending on their source
		"""
		d = self.es.search(index='candidates', doc_type=record_type, body={'size': self.max_results, 'query': {'bool': {'must': [{'match': {'_status': 'orphan'}}, {'match': {'_original_from': source}}]}}})
		return d['hits']['hits']

	def id_in_index(self, legacy_id, field='_id', source='APE', indexstore='candidates', record_type='archival_description'):
		"""
		Returns the record source data if a record with given Id was found in the index, else None
		
		Parameters:
		- legacy_id : AtoM's legacyId for the record
		- source: One of APE,APEXML, DDB, etc
		- indexstore: The ElasticSearch index. Standard is 'candidates'
		- doc_type: The ElasticSearch document type
		"""
		d = self.es.search(index=indexstore, doc_type=record_type, body={'size': self.max_results, 'query': {'match': {field: legacy_id}}})
		if d['hits']['total'] > 0:
			return d['hits']['hits'][0]['_source']
		else:
			return

	def prepare_json(self, d):
		"""
		Cleans the record dict, trimming '|' caracters, add culture CULTURE if void
		"""
		if 'culture' in d:
			if d['culture'] == '':
				d['culture'] = g.CULTURE
		else:
			d['culture'] = g.CULTURE
		for key, value in d.items():
			if isinstance(value, str):
				if value[-1:] == '|':
					d[key] = value[:-1]
				if value[:1] == '|':
					d[key] = value[1:]

		return d.copy()

	def iso639_3to2(self, lang639_3):
		"""
		Work around due to change of the 'language' field in AtoM from ISO 639-3 to ISO 639-2
		
		Parameter:
		- lang639_3 : The ISO 639-3 code of a given language
		"""
		if not lang639_3:
			return
		elif len(lang639_3) == 2:
			return lang639_3
		else:
			d = {'ger': 'de', 'cze': 'cs', 'chi': 'zh', 'dut': 'nl', 'gre': 'el', 'per': 'fa', 'fre': 'fr'}
			if lang639_3 in d:
				pass
			return d[lang639_3]
		try:
			lang = pycountry.languages.get(alpha_3=lang639_3)
			return lang.alpha_2
		except:
			print('There is no ISO 639-3 code for language %s ', lang639_3)
			return ''

	def get_parents(self, r_id):
		"""
		Returns a list of dict with all parents of an specific record, which are already part of the index
		
		Parameter:
		- r_id: ID of the child record
		obsolete
		"""
		l = []
		finish = False
		while not finish:
			r = self.id_in_index(r_id)
			if r:
				l.append(r)
				if 'parentId' in r:
					if r['parentId'] == r['legacyId'] or r['parentId'] == '':
						finish = True
					else:
						r_id = r['parentId']
				else:
					finish = True
			else:
				finish = True

		return l

	def confirm_parents(self, r_id, record_type='archival_description', indexstore='candidates', status='confirmed'):
		"""
		Confirms parents if record has been confirmed, this will skip the relevance check for the parent.
		
		Parameters:
		- r_id: ID of the child record
		- record_type: The ElasticSearch docucument_type, standard is 'archival_description'
		- indexstrore: The name of the index, where he parents are located, standard is 'candidates'
		obsolete
		"""
		count = -1
		l = self.get_parents(r_id)
		for e in l:
			print(e)
			self.es.update(index=indexstore, doc_type=record_type, id=e['descriptionIdentifier'], body={'doc': {'_status': status}})
			count += 1

	def merge(self, d1, d2):
		return d1
		if '_original_id' in d1 and '_original_id' in d2 and d1['_original_id'] == d2['_original_id']:
			identic = True
			if len(d1) != len(d2):
				identic = False
			for k in d1.keys():
				if k in d2 and k != '_last_modified' and d1[k] != d2[k]:
					identic = False

			for k in d2.keys():
				if k != '_last_modified' and k in d1 and d1[k] != d2[k]:
					identic = False

			if identic:
				return
			for k in d1.keys():
				if k in d2 and k not in ('is_parent', 'is_child', '_last_modified') and d1[k] != d2[k]:
					if d1[k] == '':
						d1[k] = d2[k]
					elif d2[k] == '':
						pass
					else:
						d1[k] += ' | ' + d2[k]

			for k in d2.keys():
				if k not in d1:
					d1[k] = d2[k]

			d1['_status'] = 'candidate'
			print('merged')
		return d1.copy()

	def hierarchy_sort(self, l):
		print("Starting hierarchy sort\n")
		nodes = []
		forrest = []
		lst = []
		startlen = len(l)
		print('len(l) ', len(l))
		for e in l:
			if 'legacyId' in e:
				if 'parentId' in e:
					nodes.append({'l': e['legacyId'], 'p': e['parentId']})
				else:
					nodes.append({'l': e['legacyId'], 'p': ''})

		print("Node count: ", len(nodes))
		f = open('/tmp/nodex.json', 'w')
		f.write(json.dumps(nodes, sort_keys=True, indent=2, ensure_ascii=False))
		f.close()
		for e in l:
			found = True
			if 'parentId' in e:
				parent = e['parentId']
			else:
				parent = ''
			if 'legacyId' in e:
				legacy = e['legacyId']
			else:
				continue
			node = {'l': legacy, 'p': parent}
			while found:

				x = next((x for x in nodes if x['l'] == parent), False)
				if x:
					parent = x['p']
					legacy = x['l']
					node = {'l': legacy, 'p': parent}
				else:
					found = False

			if node not in forrest:
				forrest.append(node)

		self.Counter = 0
		for b in forrest:
			self.RECURSIVE_LOOP_COUNTER=0
			b = self._add_child(b, nodes, forrest)

		ids = []
		print('Forrest len', len(forrest))
		with open('/tmp/tmp_forrest.json', 'w') as (file):
			file.write(json.dumps(forrest, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		for legacy in self._forrest_generator(forrest):
			ids.append(legacy)

		print('-----')
		l.sort(key=lambda x: ids.index(x['legacyId']))
		return (
		 l.copy(), nodes.copy())

	def _remove_duplicates(self, l, key):
		"""
		removes items form a list if another item has the same key value
		"""
		out = []
		c = 0
		c_key = 0
		for e in l:
			if key in e:
				if not next((x for x in out if x[key] == e[key]), False):
					out.append(e)
				else:
					c += 1
			else:
				c_key += 1

		return out.copy()

	def _add_child(self, b, nodes, forrest):
		"""
		add information to the csv for better understanding during manual inspection of the file
		"""
		c = 0
		self.RECURSIVE_LOOP_COUNTER+1
		if self.RECURSIVE_LOOP_COUNTER>self.RECURSIVE_LOOP_MAX:
			raise("We have an infinite loop. ", b, nodes, forrest)
		for e in (x for x in nodes if x['p'] == b['l']):
			if self._is_in_forrest(forrest, e['l']):
				continue
			c += 1
			if 'c' in b:
				b['c'].append(self._add_child(e, nodes, forrest))
			else:
				b['c'] = [
				 self._add_child(e, nodes, forrest)]

		self.COUNTER += 1
		print(self.COUNTER)
		print(c, 'added to: l:', b['l'])
		return b.copy()

	def _forrest_generator(self, lst):
		for e in lst:
			yield e['l']
			if 'c' in e:
				yield from self._forrest_generator(e['c'])

	def _is_in_forrest(self, forrest, legacyid):
		for e in self._forrest_generator(forrest):
			if e == legacyid:
				return True

		return False

	def get_parent(self, l, parent_id):
		"""
		This helps sorting export lists
		
		Parameter:
		- l : list of dict representing all records above the child
		- parent_id: parentId of child = legacyId of parent
		
		Returns the record, which one is the direct parent
		"""
		i = 0
		if parent_id != '':
			for d in l:
				if d['legacyId'] == parent_id:
					return i
				i += 1

		return -1

	def get_mysql(self, sql, one=True, update=False):
		"""
		Returns the result of a select statement in mysql
		
		Parameter:
		sql : the query string
		one : True if just the first line of results should be returned 
		
		Return type are tuple or a list of tuple
		
		"""
		cursor = self.MYSQL_CONNECTION.cursor()
		dummy = cursor.execute(sql)
		if update:
			self.MYSQL_CONNECTION.commit()
			print('updated: ', sql)
			return cursor.lastrowid
		elif one:
			return cursor.fetchone()
		else:
			return cursor.fetchall()

	def _is_in_time(self, eventStartDates, eventEndDates):
		"""
		returns True if eventDates are partially within the time frame
		"""
		test=True
		if eventStartDates[0:4].isnumeric():
			if int(eventStartDates[0:4].strip(" "))>int(g.SEARCH_TO):
				test=False
		if eventEndDates[0:4].isnumeric():
			#print(int(eventEndDates[0:4].strip(" ")), "::", int(g.SEARCH_FROM))
			if int(eventEndDates[0:4].strip(" "))<int(g.SEARCH_FROM):
				test=False
			
		if test:
			#print( "TRUE", eventStartDates, eventEndDates)
			return True	
		else:
			#print( "FALSE", eventStartDates, eventEndDates)
			return False	
			


	def is_frequent_name(self, name):
		"""
		Checks if a given name is a too frequent, so there will be to much noise during the search
		"""
		self._open_frequent_names()
		name_arr = re.findall('\\w+', name)
		not_in_list = False
		for n in name_arr:
			if n not in self.FREQUENT_NAMES:
				not_in_list = True
			if not_in_list:
				return False

		return True

	def is_in_atom(self, key, strict=False, import_match=False):
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
			sql = 'SELECT source_id, source_name FROM keymap WHERE source_id = "' + key + '";'
			result = self.get_mysql(sql)
			self._open_current_description_identifiers()
			if result:
				if import_match and strict:
					return result
				if not import_match:
					return True
				return False
			else:
				if import_match:
					return False
				sql = 'SELECT description_identifier,id FROM information_object WHERE description_identifier= "' + key + '";'
				result = self.get_mysql(sql)
				if result:
					return True
				return False

	def publish(self):
		""" 
		Changes the publication status inside AtoM from draft to publish for all records, using an MySQL call.
		
		"""
		sql = 'select count( status_id) as c from status where object_id<>1 and type_id=158 group by status_id order by c ;'
		r = self.get_mysql(sql, True, False)
		print('The status of ', r[0], " records will be set from draft to published. \nDon't forget to repopulate the search index, using the command: sudo php symfony search:populate")
		sql = 'UPDATE status SET status_id=160 WHERE type_id=158 AND object_id <> 1;'
		r = self.get_mysql(sql, None, True)

	def normalize_authority_data(self, record_type, proceed=True):
		sql = 'select a.id, authorized_form_of_name, description_identifier, entity_type_id  \t\tfrom actor a join actor_i18n ai on a.id=ai.id join object o on a.id=o.id \t\twhere  o.class_name="QubitActor" order by description_identifier , entity_type_id;'
		log = ''
		self._open_stopwords()
		stopwords = set(self.STOPWORDS)
		person_stopwords = stopwords.symmetric_difference(set(self.COORPORATE_DIFFENCES))
		l = self.get_mysql(sql, False, False)
		l_new = []
		l_old = []
		for e in l:
			if e[2]:
				l_old.append(list(e))
			else:
				f = list(e)
				if f[1]:
					n_arr = re.findall('\\w+', f[1].lower())
					f.append(n_arr)
					l_new.append(f)

		for e_source in l_new:
			n_arr = re.findall('\\w+', e_source[1].lower())
			for e_target in (x for x in l_old if self._check_appearance(x[1], e_source[4])):
				sql = 'select subject_id from relation where object_id=' + str(e_source[0]) + ';'
				r = self.get_mysql(sql, False, False)
				if r:
					log += 'Archival descriptions connected to ' + e_source[1] + ' (id:' + str(e_source[0]) + '):'
					for object_id in r:
						log += str(object_id[0]) + ', '

					log += '\t -> append to ' + e_target[1] + ' (id:' + str(e_target[0]) + ')\n'
				if proceed:
					sql = 'update relation set object_id=' + str(e_target[0]) + ' where object_id=' + str(e_source[0]) + ';'
					x = self.get_mysql(sql, False, True)
					sql = 'delete from object where id=' + str(e_source[0]) + ';'
					x = self.get_mysql(sql, False, True)
			else:
				if not e_source[3]:
					if len(list(set(n_arr).intersection(person_stopwords))) > 0:
						log += e_source[1] + ' (id:' + str(e_source[0]) + ') [' + self.convert_name(e_source[1]) + '] could be a person?\n'
						k_list = self._get_wd_from_search_term(e_source[1])
						if k_list and len(k_list) == 1:
							log += '---> Could be identic to ' + k_list[0]['label_de'] + ' [' + k_list[0]['item'] + '] (' + self._get_instance(k_list[0]) + ')\n'
							if proceed:
								sql = 'Update actor set description_identifier="http://www.wikidata.org/entity/' + k_list[0]['item'] + '", entity_type_id = ' + str(A_ENTITY_TYPE_HUMAN) + ' where id=' + str(e_source[0]) + ';'
								x = self.get_mysql(sql, False, True)
						if proceed:
							sql = 'Update actor set entity_type_id = ' + str(A_ENTITY_TYPE_HUMAN) + ' where id=' + str(e_source[0]) + ';'
							x = self.get_mysql(sql, False, True)
							sql = 'update actor_i18n set authorized_form_of_name="' + self.convert_name(e_source[1]) + '" where culture="' + g.CULTURE + '" and id=' + str(e_source[0]) + ';'
							x = self.get_mysql(sql, False, True)
							self._add_other_forms_of_name(e_source[0], A_TYPE_OTHER_FORM_OF_NAME, e_source[1])

		print(log)

	def _check_appearance(self, s, arr):
		"""
		Checks if all element arr are found in s. 
		Returns boolean
		
		Parameters:
		s: The test string, will be converted to lower lower()
		arr: An array which contains a number of string elements
		"""
		self._open_stopwords()
		check = True
		only_stopwords = True
		s = s.lower()
		for e in arr:
			if len(e) > 1:
				if e.lower() not in self.STOPWORDS:
					only_stopwords = False
				if s.find(e.lower()) == -1:
					return False

		if only_stopwords:
			return False
		elif len(arr) == len(re.findall('\\w+', s)):
			return True
		else:
			return False

	def convert_name(self, s, directions=0):
		"""
		Change the "Ansetzungsform" (authorized_form)  of the name of a person from '[first name] [name]' to [name], [first name] and viceversa. 
		The direction depends of the presence of a ',' inside the given String
		
		Parameter:
		s= a string containing the name of the person   
		directions: restrains the conversion (0: Conversion in both directions; 1: Conversion from " " to ", " only; 2: conversions from ", " to " " only)
		"""
		if s.find(',') > -1:
			if directions < 1:
				return (s[s.find(',') + 1:] + ' ' + s[0:s.find(',')]).strip()
			return s
		else:
			if directions < 1:
				name_arr = re.findall('[\\w|-]+', s)
				if len(name_arr) > 1:
					start = len(name_arr) - 1
					for e in name_arr:
						if e.lower() in ('und', 'y'):
							start = name_arr.index(e) - 1

					return (' ').join(name_arr[start:]) + ', ' + (' ').join(name_arr[0:start])
				return s
			else:
				return s

	def replace_mysql_field(self, table, change_field, id_field, id_value, regex):
		"""Modify the value of an MySql field using regex
		
		table - name of the MySql table within atom database
		change_field  -the field to modify
		id_field - the name the field which serves as identifier
		id_value - the value of the identifer
		regex = the regular expression to apply
		"""
		cursor = self.mysql.cursor()
		sql = 'select ' + change_field + ' from ' + table + ' where ' + id_field + "='" + id_value + "';"
		cursor.execute(sql)
		field_content = cursor.fetchall()
		if len(field_content) == 0:
			return 'none result'
		if len(field_content) > 1:
			for i in range(1, len(field_content) - 1):
				if field_content[i - 1][0] != field_content[i][0]:
					return 'too much non identic results'

		print(field_content)
		print(type(field_content))
		print(len(field_content))

	def build_eventDates(self, dates):
		"""
		tries to extract eventStartDates and eventEndDates
		"""
		dates_arr = re.findall('\\w+|[0-9]+', dates)
		start_year = ''
		start_month = ''
		start_day = ''
		end_year = ''
		end_month = ''
		end_day = ''
		for e in dates_arr:
			if e.isnumeric():
				if int(e) in range(1000, 2000):
					if start_year == '':
						start_year = e
					else:
						end_year = e
					if int(e) in range(0, 31):
						if len(e) == 1:
							e += '0' + e
						if start_day == '':
							start_day = e
						else:
							end_day = e
						if e.lower() in g.MONTHS.keys():
							if start_month == '':
								start_month = g.MONTHS[e.lower()]
							else:
								end_month = g.MONTHS[e.lower()]

		if start_month == '':
			start_month = '01'
		if end_month == '':
			if start_month != '01':
				end_month = start_month
			else:
				end_month = '12'
			if start_day == '':
				start_day = '01'
			if end_day == '':
				if start_day != '01':
					end_day = start_day
				else:
					end_day = '31'
				if start_year == '':
					return ('', '')
				if end_year == '':
					return (start_year + '-' + start_month + '-' + start_day, start_year + '-' + end_month + '-' + end_day)
		return (
		 start_year + '-' + start_month + '-' + start_day, end_year + '-' + end_month + '-' + end_day)

	def reduce_x(self, x):
		"""
		An uggly attemp to get values from a super nested list of dicts 
		"""
		print(type(x))
		if isinstance(x, bool):
			if x:
				return 'True'
			return 'False'
		if isinstance(x, list):
			for e in x:
				if not isinstance(e, str):
					e = self.reduce_x(e)

			if isinstance(x, dict):
				return ('; ').join(x.values())
			print('---')
			print(type(x))
			return ('; ').join(x)
		if isinstance(x, dict):
			for k, v in x.items():
				if not isinstance(v, str):
					x[k] = self.reduce_x(v)

			if isinstance(x, list):
				return ('; ').join(x)
			print(x)
			return ('; ').join(x.values())


	def merge_csv(self, file1, file2):
		"""
		merge 2 csv files and resort 
		"""
		l1=fo.load_data(file1,"csv",True)
		l2=fo.load_data(file2,"csv",True)
		for e in l2:
			item=next((x for x in l1 if x['legacyId']==e['legacyId']),False)
			if not item:
				l1.append(e.copy())
		l1=self.hierarchy_sort(l1)[0]
		self.write_csv(l1,"merged_"+time.strftime("%Y%m%d_%H%M%S")+".csv")

	def json2csv(self, rfile, wfile):
		"""
		A simple helper tool. Currently not used.
		
		Parameters:
		- rfile : input file as json
		- wfile : output file as csv
		
		WIll not completely work with nested json			   
		"""
		with open(rfile, 'r') as (rf):
			l = json.load(rf)
		rf.close()
		with open(wfile, 'w') as (wf):
			fieldnames = l[0].keys()
			writer = csv.DictWriter(wf, fieldnames=fieldnames, extrasaction='ignore', delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for e in l:
				writer.writerow(e)

		wf.close()

	def _get_from_WDQ(self, query):
		try:
			params = {'format': 'json', 
			 'query': query}
			url = self.WD_SPARQL_ENDPOINT + '?' + urllib.parse.urlencode(params)
			headers = {}
			headers['accept'] = 'application/sparql-results+json'
			#print(url)
			r = urllib.request.Request(url, None, headers)
			with urllib.request.urlopen(r) as (response):
				the_page = response.read().decode('utf-8')
				return json.loads(the_page)
		except:
			return {'results': {'bindings': []}}

	def _short_entity(self, url):
		"""
		cuts the path of the Wikidata entity
		"""
		ar = url.split('/')
		return ar[len(ar) - 1]

	def _get_uniq(self, s):
		if isinstance(s, list):
			array = s
		else:
			array = s.split('|')
		ar = list(set(array))
		ar = list(filter(None, ar))
		if len(ar) == 0:
			ar = [
			 '']
		ar.sort(reverse=True)
		return ar.copy()

	def _open_stopwords(self):
		if len(self.STOPWORDS) == 0 and os.path.getsize(self.STOPWORDS_FILE) > 1:
			with open(self.STOPWORDS_FILE, 'r') as (file):
				self.STOPWORDS = file.read().split('\n')
			file.close()

	def _clean_label(self, s):
		result = ''
		self._open_stopwords()
		s_arr = re.findall('\\w+', s)
		for word in s_arr:
			if word.lower() not in self.STOPWORDS:
				result += word + ' '

		return result.strip(' ')

	def _get_frequency(self, rjson):
		count = 0
		search = rjson['searchinfo']['search']
		search = search.lower()
		for result in rjson['search']:
			if 'decription' in result and result['description'] == 'Wikimedia-Begriffsklarungsseite':
				continue
			if 'label' in result:
				s = result['label'].lower()
				s_arr = re.findall('\\w+', s)
				if search in s_arr:
					count += 1

		if count == 0:
			count = 1
		return count

	def _store_objects(self):
		old_log = ''
		with open(self.KEYWORDS_FILE, 'w') as (file):
			file.write(json.dumps(self.KEYWORDS, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		if os.path.isfile(self.SEARCH_LOG_FILE):
			with open(self.SEARCH_LOG_FILE, 'r') as (file):
				old_log = file.read()
			file.close()
		with open(self.SEARCH_LOG_FILE, 'w') as (file):
			file.write(self.SEARCH_LOG + '\n' + old_log)
		file.close()


class access_points(object):
	FROM_AP_ITERATOR=[]
	FROM_IO_ITERATOR=[]
	ACCESS_POINTS_LIST = []
	ACCESS_POINTS_LIST_FILE = 'atom/data/access_points.json'
	dm = DataManager()
	SUBJECT_AP = 35  #predefined by atom' installation routine
	PLACE_AP = 42
	GENRE_AP = 78
	NEW_RELATION_FILE = 'tmp_results/new_authority_relations.json'
	RELATION_STORE_FILE = 'atom/data/relation_store.csv'
	UNWANTED_RELATIONS_FILE = 'atom/data/unwanted_relations.csv'

	def __init__(self):
		self.ACCESS_POINTS_LIST = fo.load_data(self.ACCESS_POINTS_LIST_FILE)
		

	def update_access_points_list(self):
		print("Updating the access points list")
		sql='select t.id, t.parent_id,ti.name,ti.culture, t.taxonomy_id, o.updated_at \
			from term_i18n ti join term t on ti.id=t.id join object o on t.id=o.id \
			where t.taxonomy_id in ('+str(self.GENRE_AP)+','+str(self.PLACE_AP)+','+str(self.SUBJECT_AP)+') and ti.culture in ("en","fr","de") order by ti.name;'
		r = self.dm.get_mysql(sql, False)
		sql = 'select group_concat(name) as term from other_name o join other_name_i18n oi on o.id=oi.id where object_id=' + str(r[0][0]) + ' group by object_id;'
		alt_names = self.dm.get_mysql(sql, True)
		counter={'items':0,'not modified':0, 'created':0, 'modified':0}
		for e in r:
			counter['items']+=1
			item = next((x for x in self.ACCESS_POINTS_LIST if x['id'] == e[0]), False)
			if item:
				modified=False
				if e[2].strip(" ")!=item['culture_' + e[3]] if 'culture'+e[3] in item else "" :
					item['culture_' + e[3]] = e[2].strip(" ")
					modified=True
				if e[1]!=item['parent_id']:
					item['parent_id'] = e[1]
					modified=True
				if e[4]!=item['type']:
					item['type'] = e[4]
					modified = True
				
				
				if alt_names:
					if 'indicators' in e:
						item['indicators'].extend(list(set(item['indicators']).union(set(alt_names[0].split(',')))))
					else:
						item['indicators']=alt_names[0].split(',')
						modified=True
				if modified:
					item['last_modified']=datetime.strftime(e[5],'%Y-%m-%d_%H:%M:%S')
					counter['modified']+=1
				else:
					counter['not modified']+=1
			else:
				d = {}
				d['culture_' + e[3]] = e[2]
				d['parent_id'] = e[1]
				d['type'] = e[4]
				d['id'] = e[0]
				d['last_modified']=datetime.strftime(e[5],'%Y-%m-%d_%H:%M:%S')
				if alt_names:
					d['indicators'] = alt_names[0].split(',')
				self.ACCESS_POINTS_LIST.append(d)
				counter['created']+=1

		terms_in_atom=[x[0] for x in r]
		new_ACCESS_POINTS=[]
		for item in self.ACCESS_POINTS_LIST:
			if item['id'] in terms_in_atom:
				new_ACCESS_POINTS.append(item)
		
		print (len(self.ACCESS_POINTS_LIST) - len(new_ACCESS_POINTS), " access points have been deleted from the list because they no longer exists in AtoM")


		fo.save_data(new_ACCESS_POINTS, self.ACCESS_POINTS_LIST_FILE)
		print("done ",  counter)


	def find_other_access_points(self, proceed=True):
		"""
		Looks for other access points in AtoM database
		(DEPRECIATED)
		"""
		sql = 'select oi.id, title, scope_and_content,slug from information_object_i18n oi join slug s on oi.id=s.object_id ;'
		information_objects = self.dm.get_mysql(sql, False, False)
		for information_object in information_objects:
			test_text = ' ' + (information_object[1] or '') + ' xxxxxx ' + (information_object[2] or '') + ' '
			r = self.find_other_access_points_in_text(test_text, g.CULTURE, False)
			print(test_text[0:200])
			print(r)
			if proceed:
				for e in r[self.SUBJECT_AP]:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitObjectTermRelation",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, False, True)
					sql = 'insert into object_term_relation (id, object_id,term_id) value (' + str(last_id) + ',' + str(information_object[0]) + ',' + str(e) + ');'
					last_id = self.dm.get_mysql(sql, False, True)

				for e in r[self.PLACE_AP]:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitObjectTermRelation",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, False, True)
					sql = 'insert into object_term_relation (id, object_id,term_id) value (' + str(last_id) + ',' + str(information_object[0]) + ',' +str(e) + ');'
					last_id = self.dm.get_mysql(sql, False, True)

				for e in r[self.GENRE_AP]:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitObjectTermRelation",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, False, True)
					sql = 'insert into object_term_relation (id, object_id,term_id) value (' + str(last_id) + ',' + str(information_object[0]) + ',' + str(e) + ');'
					last_id = self.dm.get_mysql(sql, False, True)

	def find_other_access_points_in_text(self, text, culture='de', return_text=True):
		"""
		Checks if one or mor access_points could be connected to a given text,
		If return_text==False, term_ids will be returned instead
		(depreciated)
		"""
		text = text.strip(' ')
		tmp = {self.SUBJECT_AP: [], self.PLACE_AP: [], self.GENRE_AP: []}
		placeAccessPoints = []
		genreAccessPoints = []
		for ap in self.ACCESS_POINTS_LIST:
			test = False
			if 'exclusions' in ap:
				for ex in ap['exclusions']:
					pattern = re.compile('\\w' + ex + '\\W', re.IGNORECASE)
					if re.search(pattern, ' ' + text + ' '):
						continue

			if 'culture_' + culture in ap:
				pattern = re.compile('\\W' + ap['culture_' + culture] + '\\W', re.IGNORECASE)
				if re.search(pattern, ' ' + text + ' '):
					test = True
			if 'indicators' in ap:
				for ind in ap['indicators']:
					pattern = re.compile('\\w' + ind + '\\W', re.IGNORECASE)
					if re.search(pattern, ' ' + text + ' '):
						test = True

			if test:
				if return_text:
					if 'culture_' + culture in ap:
						tmp[ap['type']].append(ap['culture_' + culture])
					elif 'culture_de' in ap:
						tmp[ap['type']].append(ap['culture_de'])
					else:
						tmp[ap['type']].append(ap['id'])
				else:
					tmp[ap['type']].append(ap['id'])

		return tmp

	def _normalize_instance(self, instance):
		"""
		returns an AtoM entitiy_type_id which is needed for name access points
		"""
		#self.update_access_points_list()
		if isinstance(instance,int):
			return instance
		if isinstance(instance,str):
		
			if instance[0:1] == 'Q':
				for k, v in g.ENTITY_TYPES.items():
					if instance in v[1]:
						return v[0]
			if instance.isnumeric():
				return int(instance)

			return 0

	def find_name_access_points(self, proceed=True):
		"""
		Looks if some label from autority data are present in a specific information object.
		proceed: False if changes in database should be avoided 
		(DEPRECIATED)
		"""
		self.update_access_points_list()
		self.dm._open_names_suffixes()
		suffixes = []
		common_names = []
		for e in self.dm.NAME_SUFFIXES.values():
			suffixes.extend(e)

		suffixes = [x.lower() for x in suffixes]
		self.dm._open_stopwords()
		stopwords = set(self.dm.STOPWORDS)
		person_stopwords = stopwords.symmetric_difference(set(self.dm.COORPORATE_DIFFENCES))
		sql = 'select oi.id, title, scope_and_content,slug from information_object_i18n oi join slug s on oi.id=s.object_id ;'
		information_objects = self.dm.get_mysql(sql, False, False)
		sql = 'select a.id,a.entity_type_id, authorized_form_of_name, a.description_identifier,ai.functions from actor_i18n ai join actor a on a.id=ai.id \t\twhere a.entity_type_id in (' + (',').join(["'" + str(x) + "'" for x in g.A_ENTITY_TYPES.values()]) + ') ;'
		authority_data = self.dm.get_mysql(sql, False, False)
		sql = 'select id,subject_id, object_id from relation r where type_id=' + str(g.A_RELATION_TYPE_NAME_ACCESS_POINTS) + ' ;'
		relations = list(self.dm.get_mysql(sql, False, False))
		new_relations = []
		r = self.find_name_access_points_in_text()
		if append:
			d = {}
			d['append'] = True
			d['url'] = self.dm.BASE_URL + information_object[3]
			d['functions'] = authority_data_item[4]
			d['subject_id'] = information_object[0]
			d['object_id'] = authority_data_item[0]
			d['title'] = test_text
			d['authority'] = authority_data_item[2]
			d['description_identifier'] = authority_data_item[3]
			new_relations.append(d)
			print('%s added to id: %d, %s' % (authority_data_item[2], information_object[0], information_object[1]))
		with open(self.NEW_RELATION_FILE, 'w') as (file):
			file.write(json.dumps(new_relations, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		if proceed:
			for e in new_relations:
				if not e[0]:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitRelation",Now(),Now(),0)'
					last_id = self.dm.get_mysql(sql, False, True)
					sql = 'insert into relation (id, subject_id,object_id,type_id,source_culture) value (' + str(last_id) + ',' + str(e["subject_id"]) + ',' + str(e["object_id"]) + ',' + str(g.A_RELATION_TYPE_NAME_ACCESS_POINTS) + ',"' + g.CULTURE + '");'
					r = self.dm.get_mysql(sql, False, True)

	def access_points_generator(self, culture):
		"""
		iterates over self.ACCESS_POINTS_LIST and actors with object relation
		"""
		#self.update_access_points_list()
		for access_point in self.ACCESS_POINTS_LIST:
			if 'culture_' + culture in access_point:
				yield (
				 access_point['culture_' + culture], access_point['type'],
				 {'id': access_point['id'], 
				  'label': access_point['culture_' + culture], 
				  'description_identifier': '', 
				  'indicators': access_point['indicators'] if 'indicators' in access_point else [], 
				  'exclusions': access_point['exclusions'] if 'exclusions' in access_point else [], 
				  'entity_type': access_point['type'] if 'type' in access_point else None, 
				  'parent_id': access_point['parent_id'] if 'parent_id' in access_point else None, 
				  'description': '',
				   'last_modified':access_point['last_modified'] if 'last_modified' in access_point else '2000-01-01 00:00:00'})
			if 'indicators' in access_point:
				for e in access_point['indicators']:
					yield (
					 e, access_point['taxonomy_id'], access_point)

		sql = 'select a.id,a.entity_type_id, authorized_form_of_name, a.description_identifier,ai.functions, o.updated_at \
			from actor_i18n ai join actor a on a.id=ai.id \
			join object o on a.id=o.id \
			where a.entity_type_id in (' + (',').join(["'" + str(x) + "'" for x in g.A_ENTITY_TYPES.values()]) + ') ;'
		r = self.dm.get_mysql(sql, False, False)
		for e in r:
			if e[2] is not None:
				
				yield (
				 e[2], e[1],
				 {'id': e[0], 
				  'label': e[2], 
				  'description_identifier': e[3], 
				  'indicators': [], 
				  'exclusions': [], 
				  'entity_type': e[1], 
				  'parent_id': None, 
				  'description': '',
				  'last_modified':e[5].strftime('%Y-%m-%d %H:%M:%S')})
				if e[1] == g.A_ENTITY_TYPES['HUMAN']:
					yield (
					 self._reorder_name(e[2]), e[1],
					 {'id': e[0], 
					  'label': self._reorder_name(e[2]), 
					  'description_identifier': e[3], 
					  'indicators': [], 
					  'exclusions': [], 
					  'entity_type': e[1], 
					  'parent_id': None, 
					  'description': '',
					  'last_modified':e[5].strftime('%Y-%m-%d %H:%M:%S')})

	def find_access_points_in_atom(self, ap_iterator_name, last_revision=""):
		self.update_access_points_list()
		#r=self.dm._index_keywords()
		counter = {'information_objects':0,'actors': 0, 'actor_relations': 0, 'terms': 0, 'object_term_relations': 0}
		
		# generate a list of unwanted relations
		old_unwanted_relations = fo.load_data(self.UNWANTED_RELATIONS_FILE, None, True)
		if old_unwanted_relations is None:
			old_unwanted_relations = []
		old_relations = fo.load_data(self.RELATION_STORE_FILE, None, True)
		if old_relations is None:
			old_relations = []
		sql = 'select subject_id as information_object,object_id as ap from relation union select object_id as information_object, term_id as ap from object_term_relation order by information_object;'
		r = self.dm.get_mysql(sql, False, False)
		if not r:
			r = []
		current_relations = [list(x) for x in r]
		unwanted_relations = old_unwanted_relations.extend([x for x in old_relations if x not in current_relations])
		if unwanted_relations is None:
			unwanted_relations = []
		f = open(self.UNWANTED_RELATIONS_FILE, 'w')
		for e in unwanted_relations:
			f.write = e[0] + ',' + e[1] + '\n'
		f.close()
		if last_revision in ("now", "", "all"):
			#last_revision=datetime.now()
			last_revision=datetime(1900, 1, 1)
			#print (last_revision)
		else:
			last_revision=datetime.strptime(last_revision.replace("_"," "),'%Y-%m-%d %H:%M:%S')
			
		if ap_iterator_name == 'wd':
			ap_iterator = self.dm.search_term_generator('', True, True, False)
		else:
			ap_iterator = self.access_points_generator(g.CULTURE)
		print(last_revision)
		
		# generate list of new ap
		for ap in ap_iterator:
			#print(datetime.strptime(ap[2]['last_modified'],'%Y-%m-%d %H:%M:%S'), last_revision)
			
			if datetime.strptime( ap[2]['last_modified'].replace("_"," "),'%Y-%m-%d %H:%M:%S') <last_revision:
				continue
			self.FROM_AP_ITERATOR.append(ap)
		print(len(self.FROM_AP_ITERATOR), " new access points")

		# generate list of new information objects
		for information_object in self.dm.information_object_generator():
			#print(information_object)
			#print(information_object['last_modified'], type(information_object['last_modified']))
			if information_object['last_modified'] <last_revision:
				continue
			self.FROM_IO_ITERATOR.append(information_object)
		print(len(self.FROM_IO_ITERATOR), " new information_objects")
		#b=input("---")
		# check all information objects against new ap
		for information_object in self.dm.information_object_generator():
			counter['information_objects']+=1
			result = self.find_access_points_in_text(information_object['cargo'], self.FROM_AP_ITERATOR, g.CULTURE, False, last_revision)
			if result:
				#print(information_object['cargo'][0:40], result)
				counter=self.analyze_ap_result(result, counter, unwanted_relations, information_object)
		
		# check new information object against all ap
		for information_object in self.FROM_IO_ITERATOR:

			counter['information_objects']+=1
			result = self.find_access_points_in_text(information_object['cargo'], ap_iterator, g.CULTURE, False, last_revision)
			if result:
				#print(information_object['cargo'][0:40], result)
				counter=self.analyze_ap_result(result, counter, unwanted_relations, information_object)
		
				
		# save all current relations into file
		sql = "select subject_id as information_object,object_id as ap from relation union select object_id as information_object, term_id as ap \
			from object_term_relation \
			order by information_object;"
		fo.save_data(self.dm.get_mysql(sql, False, False),self.RELATION_STORE_FILE)
		print('\nDone\n', counter)
		print("-----------------------\nDon't forget to rebuild the nested set via >> sudo php symfony propel:build-nested-set\n---------------------------")


		### we still need to write relation to file
	
	
	def rebuild_nested(self):
		pw=getpass.getpass(prompt="\n...\nWell, let's do this now. But we need the sudo password :", stream=None)
		bsh="sudo php /usr/share/nginx/atom/symfony propel:build-nested-set"
		oo.sudo_exec(bsh,pw)
		print("process started")
	
	
	def analyze_ap_result(self,result, counter, unwanted_relations, information_object):
		actor_id = 0
		term_id = 0
		print("\n----------------------\nCounter ---> ",counter)
		for e in result['actors']:
			if not isinstance(e['id'], int):  # using wd keywords the id is a Qxxxx string, so we ned to check if an actor with this id or name is already in atom.actor
				if 'description_identifer' in e and len(e['description_identifier']) > 1:
					sql = "select id from actor where description_identifier='" + e['description_identifier'] + "';"  # does the description identifier match?
					r = self.dm.get_mysql(sql, True, False)
					if r:
						actor_id = r[0]
				if actor_id == 0: 
					sql = 'select a.id,ai.authorized_form_of_name,ai.culture,a.description_identifier \
						from actor_i18n ai join actor a on a.id=ai.id  \
						where authorized_form_of_name ="' + e['label'] + '" or authorized_form_of_name="'+self._reorder_name(e['label'])+'";'
					r = self.dm.get_mysql(sql, True, False)  # Does the name match?
					if r:
						actor_id = r[0]
				if actor_id == 0:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitActor",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, True, True) # Now we know that a new actor needs to be created
					r = self.dm.get_mysql(sql, True, False)
					lft = 0
					rgt =0
					sql = 'insert into actor (id,entity_type_id,description_identifier, parent_id, lft,rgt) value (' + str(last_id) + ',' + str(self._normalize_instance(e['entity_type'])) + ',"' + (e['description_identifier'] or '') + '",' + '3' + ',' + str(lft) + ',' + str(rgt) + ');'
					print(sql)
					r = self.dm.get_mysql(sql, True, True)
					if self._normalize_instance(e['entity_type']) == g.A_ENTITY_TYPES['HUMAN']:
						label = self._reorder_name(e['label'])
					else:
						label = e['label']
					sql = 'insert into slug (object_id,slug,serial_number) value (' + str(last_id) + ',"' + self.dm._create_slug(label) + '",0);'
					r = self.dm.get_mysql(sql, True, True)
					sql = 'insert into actor_i18n (authorized_form_of_name,id,culture) value ("' + label + '",' + str(last_id) + ',"' + g.CULTURE + '");'
					r = self.dm.get_mysql(sql, True, True)
					actor_id = last_id
					counter['actors'] += 1
			else:
				actor_id = e['id']
			relation = next((x for x in unwanted_relations if x[0] == information_object['id'] and x[1] == actor_id), False)
			if not relation:
				sql= 'select object_id, subject_id from relation where object_id='+str(actor_id)+' and subject_id=' +str(information_object['id']) + ';'
				r=self.dm.get_mysql(sql,True,False)
				if not r:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitRelation",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, True, True)
					sql = 'insert into relation (id,subject_id, object_id, type_id, source_culture) value ('+str(last_id)+',' +str(information_object['id']) + ','+str(actor_id)+',' +str(g.A_RELATION_TYPE_NAME_ACCESS_POINTS) +',"' + g.CULTURE +'");'
					r=self.dm.get_mysql(sql,True,True)
					counter['actor_relations']+=1
		for e in self.result_generator(result):
			#if not isinstance(e[0]['id'], int):
			if  isinstance(e[0]['id'], int):
				if 'description_identifer' in e[0] and len(e[0]['description_identifier']) > 1:
					#sql = "select id from term where code='" + e[0]['description_identifier'] + "' and taxonomy_id=" + str(e[1]) + ';'
					sql='select t.id from term t join note n on t.id=n.object_id join note_i18n ni on n.id=ni.id \
						where t.taxonomy_id in ('+str(self.PLACE_AP)+','+str(self.GENRE_AP)+','+str(self.SUBJECT_AP)+') \
						and ni.content="'+ e[0]['description_identifier'] +'";'

					r = self.dm.get_mysql(sql, True, False)
					if r:
						term_id = r[0]
				else:
					pass
					#print("no description_identifier in e: ", e)
				if term_id == 0:
					sql = 'select t.id from term_i18n ti join term t on t.id=ti.id where t.taxonomy_id=' + str(e[1]) + ' and ti.name="' + e[0]['label'] + '" and ti.culture="' + g.CULTURE + '";'
					r = self.dm.get_mysql(sql, True, False)
					if r:
						term_id = r[0]
				if term_id == 0:
					counter['terms'] += 1
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitTerm",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, True, True)
					sql = 'insert into term (id,taxonomy_id,code,parent_id,source_culture) value (' + str(last_id) + ',' + str(e[1]) + ',"' + e[0]['description_identifier'] + '",' + str(110) + ',"' + str(g.CULTURE) + '");'
					r = self.dm.get_mysql(sql, True, True)
					sql = 'insert into term_i18n (id,name,culture) value (' + str(last_id) + ',"' + e[0]['label'] + '","' + g.CULTURE + '");'
					r = self.dm.get_mysql(sql, True, True)
					sql = 'insert into slug (object_id,slug,serial_number) value (' + str(last_id) + ',"' + self.dm._create_slug(e[0]['label']) + '",0);'
					r = self.dm.get_mysql(sql, True, True)
					term_id=last_id
					sql='insert into note (object_id, type_id,source_culture,serial_number) \
						alues ('+str(last_id)+','+str(g.A_SOURCE_NOTE)+',"'+g.CULTURE+'",0);'
					last_id=self.dm.get_mysql(sql,True,True)
					sql='insert into note_i18n (content, id, culture) values ("'+e[0]['description_identifier']+'",'+str(last_id)+',"'+g.CULTURE+'");'
					r=self.dm.get_mysql(sql,True,True)
					
			else:
				#print( e[0]['id'] , "isn't int", type(e[0]['id']), isinstance(e[0]['id'], int))
				
				continue
			relation = next((x for x in unwanted_relations if x[0] == information_object['id'] and x[1] == e[0]['id']), False)
			#print("e", relation, "|",term_id)
			#input ("????")
			if not relation:
				sql= 'select object_id, term_id from object_term_relation where term_id='+str(term_id)+' and object_id=' +str(information_object['id']) + ';'
				r=self.dm.get_mysql(sql,True,False)
				if not r:
					sql = 'insert into object (class_name,created_at,updated_at,serial_number) value ("QubitObjectTermRelation",Now(),Now(),0);'
					last_id = self.dm.get_mysql(sql, True, True)
					sql = 'insert into object_term_relation (id,object_id, term_id) value ('+str(last_id)+','+str(information_object['id']) + ','+str(term_id)+');'
					r=self.dm.get_mysql(sql,True,True)
					counter['object_term_relations']+=1
		return counter
		

	def result_generator(self, result):
		for e in result[self.SUBJECT_AP]:
			yield (e, self.SUBJECT_AP)

		for e in result[self.PLACE_AP]:
			yield (e, self.PLACE_AP)

		for e in result[self.GENRE_AP]:
			yield (e, self.GENRE_AP)

	def find_access_points_in_text(self, text, iterator, culture='de', return_word=True, last_revision=""):
		"""
		Checks a given text contains access point information which is provided by a list
		ap_iterator: generator function where each yield send back a tuple (search_term_name,search_term_instance,search_term_object)
		"""
		if last_revision=="":
			last_revision=datetime.now()
		text = ' ' + so.clean_text(text) + ' '
		tmp = {'actors': [], self.SUBJECT_AP: [], self.PLACE_AP: [], self.GENRE_AP: []}
		i = 0
		for term in iterator:

			i += 1
			if not isinstance(term[0], str):
				print(term[0], " has no instance ", term)
				continue
			if term[0] in '[]':
				print("Term is empty ", term)
				continue
			append = False
			instance = self._normalize_instance(str(term[1]))
			#print(term[0], " instance: ", instance) 
			if instance in g.A_ENTITY_TYPES.values():
				key = 'actors'
			else:

				if instance in ('subject', self.SUBJECT_AP):
					key = self.SUBJECT_AP
				else:
					if instance in ('place', self.PLACE_AP):
						key = self.PLACE_AP
					else:
						if instance in ('genre', self.GENRE_AP):
							key = self.GENRE_AP
						else:
							key = 'other'
							#print(instance, term[1], " instance")
							if 'other' not in tmp:
								tmp['other'] = []
							#print(instance, key, '<<<<<<<<<<<<<<<', term[0])
				if 'exclusions' in term[2]:
					for exclusion in term[2]['exclusions']:
						pattern = re.compile('\\W' + so.clean_text_re(exclusion) + '\\W', re.IGNORECASE)
						if re.search(pattern, ' ' + text + ' '):
							continue

			pattern = re.compile('\\W' + so.clean_text_re(term[0]) + '\\W', re.IGNORECASE)
			#print(pattern)
			if re.search(pattern, ' ' + so.clean_text(text) + ' '):

				if return_word:
					tmp[key].append(term[0])
				else:
					tmp[key].append(term[2])
					continue

		print(i, ' ap checked against ', text[0:40])
		return tmp
		for authority_data_item in authority_data:
			append = False
			if authority_data_item[2]:
				if test_text.find(' ' + authority_data_item[2].lower() + ' ') > -1:
					print('gefunden: nachname, vorname')
					if authority_data_item[1] == g.A_ENTITY_TYPES['HUMAN']:
						a_data_arr = re.findall('[\\w|-]+', authority_data_item[2].lower())
						cn = {}
						if len(a_data_arr) > 1:
							r = [i for i, x in enumerate(test_text_arr) if x == a_data_arr[0] and x + 1 == a_data_arr[1]]
							if r:
								for e in r:
									if e > 0 and test_text_arr[e - 1].lower() not in person_stopwords:
										if not self._is_in_relation(information_object[0], authority_data_item[0], relations, new_relations):
											relations.append((None, information_object[0], authority_data_item[0]))
											print('%s added to id: %d, %s' % (authority_data_item[2], information_object[0], information_object[1]))

							if test_text.find(' ' + self.convert_name(authority_data_item[2].lower()) + ' ') > -1:
								if not self._is_in_relation(information_object[0], authority_data_item[0], relations, new_relations):
									append = True
					else:
						if not self._is_in_relation(information_object[0], authority_data_item[0], relations, new_relations):
							append = True
					if authority_data_item[1] == g.A_ENTITY_TYPES['HUMAN']:
						a_data_arr = re.findall('[\\w|-]+', authority_data_item[2].lower())
						if len(a_data_arr) > 1 and len(a_data_arr[1]) > 2 and a_data_arr[0] not in person_stopwords:
							family_name = a_data_arr[0].lower()
							r = [i for i, x in enumerate(test_text_arr) if x == family_name]
							if len(r) == 0:
								r = [i for i, x in enumerate(test_text_arr) if x == family_name + 's']
							for e in r:
								if e > 0 and test_text_arr[e - 1].lower() in suffixes:
									if not self._is_in_relation(information_object[0], authority_data_item[0], relations, new_relations):
										append = True

					if append:
						d = {}
						d['append'] = True
						d['url'] = self.dm.BASE_URL + information_object[3]
						d['functions'] = authority_data_item[4]
						d['object_id'] = information_object[0]
						d['subject_id'] = authority_data_item[0]
						d['title'] = test_text
						d['authority'] = authority_data_item[2]
						d['description_identifier'] = authority_data_item[3]
						new_relations.append(d)
						print('%s added to id: %d, %s' % (authority_data_item[2], information_object[0], information_object[1]))

	def normalize_authority_data_identifier(self):
		"""
		Changes the base_url of description_identifier in atom.actor to http://wikidata.org/entity/
		Checks all entries in atom.actor
		"""
		sql = ' select id, description_identifier from actor where description_identifier like "%wikidata%" and description_identifier not like "%http://www.wikidata.org/entity%";'
		identifiers = self.dm.get_mysql(sql, False, False)
		if identifiers:
			identifiers = list(identifiers)
			for identifier in identifiers:
				identifier = list(identifier)
				print(identifier)
				identifier[1] = 'http://www.wikidata.org/entity/' + identifier[1][identifier[1].rfind('/') + 1:]
				print('to ---> ' + identifier[1])
				sql = 'update actor set description_identifier = "' + identifier[1] + '" where id= ' + str(identifier[0]) + ';'
				r = self.dm.get_mysql(sql, False, True)

	def remove_duplicate_relations(self):
		"""
		Removes entries in atom.relation where the object_id subject_id pair are identical having start_date and end_date NULL and same source_culture
		"""
		id_to_remove = []
		new_relations = []
		sql = 'select id, subject_id, object_id,start_date,end_date, source_culture from relation;'
		relations = self.dm.get_mysql(sql, False, False)
		for relation in relations:
			e = next((x for x in new_relations if x[1] == relation[1] and x[2] == relation[2] and x[3] == relation[3] and x[4] == relation[4] and x[5] == relation[5]), False)
			if e:
				id_to_remove.append(relation[0])
			else:
				new_relations.append(relation)

		print(len(id_to_remove), ' relations removed')
		for e in id_to_remove:
			sql = 'delete from object where id=' + str(e) + ';'
			x = self.dm.get_mysql(sql, False, True)
			


	def clean_lower_relations(self):
		"""
		delete lines from atom.relation and atom.object_term_relation where the same term is already related to a parent
		"""
		sql="select id,parent_id from information_object;"
		information_objects=self.dm.get_mysql(sql,False,False)
		sql="select r.id,subject_id,object_id,i.parent_id from relation r join information_object i on r.subject_id=i.id where type_id="+str(g.A_RELATION_TYPE_NAME_ACCESS_POINTS)+";"
		#(primary_key, information_object, actor, information_object_parent)
		relations=self.dm.get_mysql(sql,False,False)
		sql="select r.id,object_id,term_id,i.parent_id from object_term_relation r join information_object i on r.object_id=i.id ;"
		#(primary_key, information_object, term, information_object_parent)
		object_term_relations=self.dm.get_mysql(sql,False,False)
		relation_to_delete=[]
		object_term_relation_to_delete=[]
		for relation in relations:
			#print(relation)
			parents=self.dm.parent_information_objects(relation[3],information_objects,[])
			for parent in parents:
				if next((x for x in relations if x[1]==parent and x[2]==relation[2]),False):
					relation_to_delete.append(relation)
		#print(relation_to_delete)			
		for relation in relation_to_delete:
			sql = 'delete from object where id=' + str(relation[0]) + ';'
			x = self.dm.get_mysql(sql, False, True)
		print (len(relation_to_delete), " actor relations deleted")
		
		for relation in object_term_relations:
			parents=self.dm.parent_information_objects(relation[3],information_objects,[])
			for parent in parents:
				if next((x for x in relations if x[1]==parent and x[2]==relation[2]),False):
					object_term_relation_to_delete.append(relation)
		for relation in object_term_relation_to_delete:
			sql = 'delete from object where id=' + str(relation[0]) + ';'
			x = self.dm.get_mysql(sql, False, True)			
		print (len(object_term_relation_to_delete), " actor relations deleted")
	def _is_in_relation(self, subject_id, object_id, relations, new_relations):
		"""
		Returns true if at least one pair of subject_id - object_id is found in relations
		"""
		e = next((x for x in relations if relations[1] == subject_id and relations[2] == object_id), False)
		if e:
			return True
		else:
			ee = next((x for x in new_relations if x['subject_id'] == subject_id and x['object_id'] == object_id), False)
			if ee:
				return True
			return False

	def _add_other_forms_of_name(self, object_id, type_id, name, start_date='', end_date='', note='', dates=''):
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
		sql = 'select name  from other_name o join other_name_i18n oi on o.id=oi.id  where object_id=' + str(object_id) + ' and type_id=' + str(type_id) + ';'
		r = self.dm.get_mysql(sql, False, False)
		if r:
			for e in r:
				if e[0].lower() == name.lower():
					return False

		sql = 'insert into other_name (object_id, type_id, start_date, end_date, source_culture) values (' + str(object_id) + ',' + str(type_id) + ',"' + start_date + '","' + end_date + '","' + g.CULTURE + '");'
		increment = self.dm.get_mysql(sql, False, True)
		if increment:
			sql = 'insert into other_name_i18n (name,note,dates,id,culture) values ("' + name + '","' + note + '","' + dates + '",' + str(increment) + ',"' + g.CULTURE + '");'
			r = self.dm.get_mysql(sql, False, True)
			return True

	def normalize_other_access_points(self):
		"""
		a) write code field to source_note if description identifier
		
		"""
		sql='select t.id, t.taxonomy_id, n.type_id,n.id, ni.content from term t  join note n on t.id=n.object_id join note_i18n ni on n.id=ni.id where t.taxonomy_id in (35,42,78) and type_id='+str(g.A_SOURCE_NOTE)+';'
		existing_notes=self.dm.get_mysql(sql,False,False)
		sql='select id, code, source_culture from term where taxonomy_id in (35,42,78);'
		terms=self.dm.get_mysql(sql,False,False)
		for term in terms:
			if not (term[1] is None):
				if not(next((x for x in existing_notes if x[0]==term[0]),False)):
					sql='insert into note (object_id, type_id,source_culture,serial_number) \
					values ('+str(term[0])+','+str(g.A_SOURCE_NOTE)+',"'+term[2]+'",0);'
					last_id=self.dm.get_mysql(sql,True,True)
					sql='insert into note_i18n (content, id, culture) values ("'+term[1]+'",'+str(last_id)+',"'+g.CULTURE+'");'
					r=self.dm.get_mysql(sql,True,True)
					sql='update term set code = NULL where id='+str(term[0])+';'
					r=self.dm.get_mysql(sql,True,True)
					
		


	def normalize_name_access_points(self):
		"""
		a) checks for name authority data without entity_type:_id
		b) tries to find out if orphan authority data could be a person
		c) ask for the entitiy type of remaining orphan authority data
		d) dedup and merge authority data entries and relations
		e) Reorder the names of humans
		
		"""
		#a)
		sql = 'select a.id, ai.authorized_form_of_name, a.entity_type_id from actor a join actor_i18n ai on a.id=ai.id join relation r on a.id=r.object_id where entity_type_id is NULL and r.type_id=161 and ai.culture="' + g.CULTURE + '" group by a.id;'
		orphan_authority_data = list(self.dm.get_mysql(sql, False, False))
		orphan_authority_data = [list(x) for x in orphan_authority_data]
		print(len(orphan_authority_data), ' orphans found')
		sql = 'select a.id, ai.authorized_form_of_name, a.entity_type_id from actor a join actor_i18n ai on a.id=ai.id  join relation r on a.id=r.object_id where entity_type_id is not NULL and r.type_id=161 and ai.culture="' + g.CULTURE + '" group by a.id;'
		confirmed_authority_data = self.dm.get_mysql(sql, False, False)
		print(len(confirmed_authority_data), ' confirmed name access points found')
		c = 0
		#b)
		for orphan in orphan_authority_data:
			e = next((x for x in confirmed_authority_data if x[1].lower().strip(' ') == self._reorder_name(orphan[1]).lower().strip(' ')), False)
			if not e:
				e = next((x for x in confirmed_authority_data if x[1].lower().strip(' ') == orphan[1].lower().strip(' ')), False)
			if e:
				orphan[2] = e[2]
				print('match found: ', orphan[1], ' --> ', e[1])
				orphan[1] = e[1]
				c += 1
				print(orphan)
				continue

		print(c, ' matches found')
		#c)
		entity_type_labels = [x for x in g.A_ENTITY_TYPES.keys()]
		entity_type_labels.sort()
		c = 0
		for orphan in orphan_authority_data:
			if orphan[2] is None:
				os.system('clear')
				for i, label in enumerate(entity_type_labels):
					print(i, label)

				input_str = input('Please select an entity type for ' + orphan[1] + ': ')
				if input_str.isnumeric():
					if int(input_str) in range(0, len(entity_type_labels) - 1):
						orphan[2] = g.A_ENTITY_TYPES[entity_type_labels[int(input_str)]]
						c += 1
						if entity_type_labels[int(input_str)] == 'HUMAN' and orphan[1].find(',') == -1:
							orphan[1] = self._reorder_name(orphan[1])
				else:
					break
			else:
				print('ok ', orphan)

		print(c, ' items defined')
		print(len([x for x in orphan_authority_data if x[2] is None]), ' name_access_points still to define')
		for orphan in orphan_authority_data:
			if orphan[2]:
				sql = 'update actor set entity_type_id=' + str(orphan[2]) + ' where id= ' + str(orphan[0]) + ';'
				last_id = self.dm.get_mysql(sql, False, True)
			if orphan[1]:
				sql = "update actor_i18n set authorized_form_of_name='" + str(orphan[1]) + "' where id= " + str(orphan[0]) + " and culture='" + g.CULTURE + "';"
				last_id = self.dm.get_mysql(sql, False, True)

		done = []
		# d)
		#sql='select a.id, ai.authorized_form_of_name, a.entity_type_id , a.description_identifier from actor a join actor_i18n ai on a.id=ai.id join relation r on a.id=r.object_id where  r.type_id=161 and ai.culture="'+g.CULTURE+'" group by a.id;'
		#sql='select a.id, ai.authorized_form_of_name, a.entity_type_id , a.description_identifier, ai.dates_of_existence, ai.history from actor a join actor_i18n ai on a.id=ai.id left join relation r on a.id=r.object_id where ( r.type_id is null or r.type_id=161) and ai.culture="'+g.CULTURE+'"  group by a.id order by authorized_form_of_name;'
		sql='select a.id, ai.authorized_form_of_name, a.entity_type_id , a.description_identifier , ai.dates_of_existence, ai.history \
			from actor a join actor_i18n ai on a.id=ai.id left join relation r on a.id=r.object_id \
			where ( r.type_id is null or r.type_id=161) and ai.culture="'+g.CULTURE+'"  and ai.authorized_form_of_name is not null and a.id not in (select id from repository) \
			group by a.id order by authorized_form_of_name;'
		confirmed_authority_data = self.dm.get_mysql(sql, False, False)
		
		for confirmed in confirmed_authority_data:
			
			if not(confirmed[2] is None):
				
				if self._normalize_instance(confirmed[2])==g.A_ENTITY_TYPES['HUMAN']:
					
					if len(confirmed[1].strip(" ").split(" "))>1 and confirmed[1].find(",")==-1:
						print("Changing ", confirmed[1], " to ", self._reorder_name(confirmed[1]))
						sql='update actor_i18n set authorized_form_of_name="'+self._reorder_name(confirmed[1]) +'" where id='+str(confirmed[0])+' and culture="'+g.CULTURE+'";'
						r=self.dm.get_mysql(sql,True, True)
			e = next((x for x in confirmed_authority_data if ((confirmed[1] == x[1] or x[1]==self._reorder_name(confirmed[1]) and not( x[1] is None)) or (not(x[3] is None and confirmed[3] is None) and confirmed[3]==x[3] ) )and confirmed[0] != x[0]), False)
			if e:
				if confirmed[0] in done:
					continue
				else:
					done.append(e[0])
				keep = e[0]
				remove = confirmed[0]
				keep_di=e[3]
				remove_di=confirmed[3]
				keep_de=e[4]
				remove_de=confirmed[4]
				keep_hi=e[5]
				remove_hi=confirmed[5]
				if keep > remove:
					keep, remove = remove, keep
					keep_di,remove_di = remove_di,keep_di
					keep_de,remove_de = remove_de,keep_de
					keep_hi,remove_hi = remove_hi,keep_hi
				if remove_di != keep_di:
					if not (remove_di is None):
						if remove_di.find("wikidata")>-1:
							sql='update actor set description_identifier="'+remove_di+'" where id='+str(keep)+';'
							r= self.dm.get_mysql(sql, False, True)
					if not(remove_de is None) and keep_de !=remove_de:
						sql='update actor set dates_of_existence="'+remove_de+'" where id='+str(keep)+';'
						r= self.dm.get_mysql(sql, False, True)
					if not(remove_hi is None) and keep_hi !=remove_hi:
						sql='update actor set history="'+remove_hi+'" where id='+str(keep)+';'
						r= self.dm.get_mysql(sql, False, True)
									
						
				sql = 'update relation set object_id=' + str(keep) + ' where object_id=' + str(remove) + ';'
				lastId = self.dm.get_mysql(sql, False, True)
				sql = 'update event set actor_id = ' + str(keep) + ' where actor_id=' + str(remove) + ';'
				last_id = self.dm.get_mysql(sql, False, True)
				
				sql ='select id from actor where parent_id='+str(remove)+';'
				if self.dm.get_mysql(sql, True, False) is None:
					sql = 'delete from object where id=' + str(remove) + ';'
					print(confirmed[0], e[0], confirmed[1], e[1], sql)
					lastId = self.dm.get_mysql(sql, False, True)

		self.remove_duplicate_relations()

	def _reorder_name(self, s, wd_type='Q5'):
		if wd_type == 'Q5':
			if s.find(",")>-1:
				return  (s[s.find(",")+1:]+" "+ s[0:s.find(",")]).strip(" ")

			arr = re.findall('\\w+', s)
			if len(arr) > 1:
				out = arr[len(arr) - 1] + ','
				for i in range(0, len(arr) - 1):
					out += ' ' + arr[i]

				return out
			return s
		else:
			return s
