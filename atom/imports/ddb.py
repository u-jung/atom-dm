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
import pprint
from lxml import html
from atom.main import data_manager, g
from atom.config import api_keys
from atom.helpers.helper import fileOps, listOps, stringOps, osOps, funcOps
fo = fileOps()

class Ddb(object):
	
	oauth_consumer_key=api_keys.API_KEYS["DDB"]
	base_url='https://api.deutsche-digitale-bibliothek.de/'
	BASE_URL_FRONTEND="https://www.deutsche-digitale-bibliothek.de/"
	id_list_file='../data/ddb_id_list.json'
	id_all_file='../data/ddb_id_all.json'
	result_file='../data/ddb_results.json'
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
	ID_INDEX_FILE="../data/ddb_id_index.json"
	ID_INDEX={}
	BLOCKED_FILE="../data/ddb_blocked.txt"       # list of items which should ne be retrieved
	BLOCKED=[]
	nofounds=""
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
			'flex_arch_009':'extentAndMedium',
			'flex_arch_013':'extentAndMedium',
			'flex_arch_011':'extentAndMedium',
			'flex_arch_012':'extentAndMedium',
			'flex_arch_014':'repository',
			'flex_arch_013':'archivalHistory',
			'flex_arch_001':'arrangement',
			'flex_arch_007':'languageNote',
			'flex_arch_006':'acquisition',
			'flex_arch_010':'scopeAndContent',
			'flex_arch_024':'archivalHistory',
			'flex_arch_029':'scopeAndContent',
			'flex_arch_032':'scopeAndContent',
			'flex_arch_001':'arrangement',
			'flex_arch_029':'arrangement',
			'flex_arch_017':'languageNote',
			'flex_arch_015':'physicalCharacteristics',
			'flex_arch_016':'genreAccessPoints',
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


	FIELD_MAPPING={
		'identifier':['flex_bibl_013','flex_mus_neu_410'],
		'title':['flex_bibl_001','bin_010','flex_denk_008','flex_mus_10','flex_mus_20.10','flex_mus_30.50', 'flex_mus_neu_010', 'flex_mus_neu_015','label'],
		'scopeAndContent':['description','Objektbeschreibung','subtitle','flex_bibl_015', 'flex_bibl_016','flex_denk_011', 'flex_film_001', 'flex_film_007','flex_mus_20.20'],
		'extentAndMedium':['flex_bibl_009', 'flex_film_009','flex_film_017', 'flex_mus_200.100.10'],
		'archivalHistory':[],
		'arrangement':['bin_009'],
		'accessConditions':[],
		'reproductionConditions':['bin_009','stat_007','stat_010'],
		'language':[],
		'script':[],
		'languageNote':['flex_bibl_008'],
		'physicalCharacteristics':['flex_bibl_003','flex_denk_001','flex_film_006', 'flex_mus_neu_070','flex_mus_neu_060'],
		'findingAids':[],
		'locationOfOriginals':['flex_bibl_014','flex_mus_neu_400'],
		'locationOfCopies':[],
		'relatedUnitsOfDescription':[],
		'publicationNote':[],
		'digitalObjectURI':[],
		'generalNote':['flex_bibl_012', 'flex_mus_neu_050',],
		'subjectAccessPoints':['flex_arch_034','flex_bibl_011','flex_film_015','flex_mus_neu_350'],
		'placeAccessPoints':['flex_denk_007','lex_denk_005','flex_denk_004','flex_denk_003','flex_denk_002','flex_film_014','flex_mus_neu_120'],
		'nameAccessPoints':['flex_bibl_002','flex_denk_012','flex_film_002','flex_film_004','flex_mus_200.10.10'],
		'genreAccessPoints':['flex_denk_013','flex_mus_30.40','flex_mus_neu_020'],
		'descriptionIdentifier':['flex_bibl_013'],
		'institutionIdentifier':[],
		'rules':[],
		'descriptionStatus':[],
		'levelOfDetail':[],
		'revisionHistory':[],
		'languageOfDescription':[],
		'scriptOfDescription':[],
		'sources':[],
		'archivistNote':[],
		'publicationStatus':[],
		'physicalObjectName':[],
		'physicalObjectLocation':[],
		'physicalObjectType':[],
		'alternativeIdentifiers':[],
		'alternativeIdentifierLabels':[],
		'eventDates':['begin', 'end','flex_mus_200.30.30','flex_mus_200.10.30','flex_mus_200.60.30','flex_mus_200.90.30', 'bin_008','flex_bibl_006','flex_denk_010','flex_film_005','flex_mus_110','date', 'flex_mus_neu_130' ],
		'eventTypes':[],
		'eventStartDates':[],
		'eventEndDates':[],
		'eventDescriptions':[],
		'eventActors':['flex_mus_neu_110',"$"]
			
	
	}
	
	
	FIELD_WITHOUT_INTRO={
		"eventDates", "flex_mus_neu_400",'flex_mus_neu_340','label','subtitle', 'title',
		'flex_mus_neu_050','flex_mus_neu_350','flex_mus_neu_020',
		'flex_mus_neu_410','flex_mus_neu_070','flex_mus_neu_120', 'flex_mus_neu_010',
		'flex_mus_neu_015','flex_mus_neu_110','flex_mus_neu_060'
	}
	
	FIELD_NAMES={"bin_001":"[Content-Viewer-Image]",
				"bin_002":"[Thumbnail-Preview]",
				"bin_004":"[Content-Viewer-Großansicht]",
				"bin_006":"[Medientyp]",
				"bin_007":"[Person]",
				"bin_008":"[Datum]",
				"bin_009":"Bestand",
				"bin_009":"Institution",
				"bin_009":"[Rechte am Binärcontent]",
				"bin_010":"[Titel für Bildunterschrift]",
				"flex_arch_001":"Bestand",
				"flex_arch_002":"Online-Findbuch im Angebot des Archivs",
				"flex_arch_004":"Archivaliensignatur",
				"flex_arch_005":"Alt-/Vorsignatur",
				"flex_arch_006":"Provenienz",
				"flex_arch_007":"Vorprovenienz",
				"flex_arch_008":"Laufzeit",
				"flex_arch_009":"Digitalisat im Angebot des Archivs",
				"flex_arch_010":"Enthältvermerke",
				"flex_arch_011":"Umfang",
				"flex_arch_012":"Maße",
				"flex_arch_013":"Material",
				"flex_arch_014":"Urheber",
				"flex_arch_015":"Formalbeschreibung",
				"flex_arch_016":"Archivalientyp",
				"flex_arch_017":"Sprache der Unterlagen",
				"flex_arch_018":"Sonstige Erschließungsangaben",
				"flex_arch_019":"Sonstige Erschließungsangaben",
				"flex_arch_020":"Sonstige Erschließungsangaben",
				"flex_arch_021":"Sonstige Erschließungsangaben",
				"flex_arch_022":"Bemerkungen",
				"flex_arch_024":"Bestandsbeschreibung",
				"flex_arch_025":"Bestandssignatur",
				"flex_arch_026":"Bestandslaufzeit",
				"flex_arch_027":"indexbegriff Person",
				"flex_arch_028":"Verwandte Bestände und Literatur",
				"flex_arch_029":"Kontext",
				"flex_arch_030":"Archivalientitel",
				"flex_arch_031":"Signatur",
				"flex_arch_032":"Beschreibung:",
				"flex_arch_033":"indexbegriff Ort",
				"flex_arch_034":"indexbegriff Sache",
				"flex_bibl_001":"Weitere Titel",
				"flex_bibl_002":"Beteiligte Personen und Organisationen",
				"flex_bibl_003":"Dokumenttyp",
				"flex_bibl_004":"Erschienen in",
				"flex_bibl_005":"Ausgabe",
				"flex_bibl_006":"Erschienen",
				"flex_bibl_007":"Elektronische Ausgabe",
				"flex_bibl_008":"Sprache",
				"flex_bibl_009":"Umfang",
				"flex_bibl_010":"Reihe",
				"flex_bibl_011":"Thema",
				"flex_bibl_012":"Anmerkungen",
				"flex_bibl_013a":"PURL",
				"flex_bibl_013b":"URN",
				"flex_bibl_013c":"ISBN",
				"flex_bibl_013d":"ISSN",
				"flex_bibl_013e":"DOI",
				"flex_bibl_013f":"Handle",
				"flex_bibl_013":"Identifier",
				"flex_bibl_014":"Standort",
				"flex_bibl_015":"Abstract",
				"flex_bibl_016":"Inhaltsverzeichnis",
				"flex_denk_001":"Denkmalart",
				"flex_denk_002":"Land",
				"flex_denk_003":"Kreis",
				"flex_denk_004":"Ort",
				"flex_denk_005":"Ortsteil",
				"flex_denk_006":"Straße und Hausnummer",
				"flex_denk_007":"Lage",
				"flex_denk_008":"Bezeichnung",
				"flex_denk_009":"Denkmaltyp",
				"flex_denk_010":"Datierung",
				"flex_denk_011":"Beschreibung",
				"flex_denk_012":"Beteiligte",
				"flex_denk_013":"Denkmal-Kategorie",
				"flex_denk_013":"Denkmaltyp (fein)",
				"flex_film_001":"Weitere Titel",
				"flex_film_002":"Urheber",
				"flex_film_003":"Herausgeber",
				"flex_film_004":"Mitwirkende",
				"flex_film_005":"Entstanden",
				"flex_film_006":"Medientyp",
				"flex_film_007":"Beschreibung",
				"flex_film_008":"Quelle",
				"flex_film_009":"Format",
				"flex_film_010":"Sprache",
				"flex_film_011":"Objekttyp",
				"flex_film_013":"Zeitlicher Bezug",
				"flex_film_014":"Örtlicher Bezug",
				"flex_film_015":"Bezug",
				"flex_film_016":"Verweis auf Textdokument",
				"flex_film_017":"Länge",
				"flex_mus_100":"Kultur",
				"flex_mus_10":"Objektbezeichnung",
				"flex_mus_110":"Periode",
				"flex_mus_140":"Maße",
				"flex_mus_150":"Signatur",
				"flex_mus_160":"Druckzustand",
				"flex_mus_170":"Auflage",
				"flex_mus_200.100.10":"Typzuweisung",
				"flex_mus_200.100.20":"Typzuweisung (wo)",
				"flex_mus_200.100.30":"Typzuweisung (wann)",
				"flex_mus_200.10.10":"Hergestellt (von wem)",
				"flex_mus_200.10.20":"Hergestellt (wo)",
				"flex_mus_200.10.30":"Hergestellt (wann)",
				"flex_mus_200.110.10":"Restauriert (von wem)",
				"flex_mus_200.110.20":"Restauriert (wo)",
				"flex_mus_200.110.30":"Restauriert (wann)",
				"flex_mus_200.120.10":"Provenienz (von wem)",
				"flex_mus_200.120.20":"Provenienz (wo)",
				"flex_mus_200.120.30":"Provenienz (wann)",
				"flex_mus_200.130.10":"Fall (wo)",
				"flex_mus_200.130.20":"Fall (wann)",
				"flex_mus_200.140.10":"Aufführung (von wem)",
				"flex_mus_200.140.20":"Aufführung (wo)",
				"flex_mus_200.140.30":"Aufführung (wann)",
				"flex_mus_200.20.10":"Wurde genutzt (von wem)",
				"flex_mus_200.20.20":"Wurde genutzt (wo)",
				"flex_mus_200.20.30":"Wurde genutzt (wann)",
				"flex_mus_200.30.10":"Geschaffen (von wem)",
				"flex_mus_200.30.20":"Geschaffen (wo)",
				"flex_mus_200.30.30":"Geschaffen (wann)",
				"flex_mus_200.40.10":"Gefunden (von wem)",
				"flex_mus_200.40.20":"Gefunden (wo)",
				"flex_mus_200.40.30":"Gefunden (wann)",
				"flex_mus_200.50.10":"Auftrag (von wem)",
				"flex_mus_200.50.20":"Auftrag (wo)",
				"flex_mus_200.50.30":"Auftrag (wann)",
				"flex_mus_200.60.10":"Entworfen (von wem)",
				"flex_mus_200.60.20":"Entworfen (wo)",
				"flex_mus_200.60.30":"Entworfen (wann)",
				"flex_mus_200.70.10":"Erworben (von wem)",
				"flex_mus_200.70.20":"Erworben (wo)",
				"flex_mus_200.70.30":"Erworben (wann)",
				"flex_mus_200.80.10":"Erstbeschrieben (von wem)",
				"flex_mus_200.80.20":"Erstbeschrieben (wo)",
				"flex_mus_200.80.30":"Erstbeschrieben (wann)",
				"flex_mus_200.90.10":"Gesammelt (von wem)",
				"flex_mus_200.90.20":"Gesammelt (wo)",
				"flex_mus_200.90.30":"Gesammelt (wann)",
				"flex_mus_20.10":"Originaltitel",
				"flex_mus_20.20":"Alternativer Titel",
				"flex_mus_20":"Titel",
				"flex_mus_30.100":"Schlagwort",
				"flex_mus_30.10":"Sachsystematik",
				"flex_mus_30.130":"Dokumenttyp",
				"flex_mus_30.15":"Sachbegriff",
				"flex_mus_30.200":"Warenart",
				"flex_mus_30.20":"Stil",
				"flex_mus_30.300":"Sprache",
				"flex_mus_30.30":"Funktion",
				"flex_mus_30.400":"Gruppe",
				"flex_mus_30.40":"Genre",
				"flex_mus_30.410":"Teiluntergruppe",
				"flex_mus_30.420":"Untergruppe",
				"flex_mus_30.500":"Taxon",
				"flex_mus_30.50":"Sachtitel",
				"flex_mus_30.510":"Klasse",
				"flex_mus_30.520":"Ordnung",
				"flex_mus_30.530":"Familie",
				"flex_mus_30.540":"Gattung",
				"flex_mus_30.550":"Art",
				"flex_mus_30.5":"Profil",
				"flex_mus_30.600":"Präparationsart",
				"flex_mus_30.60":"Sammlung",
				"flex_mus_30.610":"Stadium",
				"flex_mus_30.620":"Geschlecht",
				"flex_mus_30.630":"Shock",
				"flex_mus_30.640":"Verwitterung",
				"flex_mus_30.650":"Mineral",
				"flex_mus_30.660":"Gesteinstyp",
				"flex_mus_30.670":"Geologische Zeiteinheit",
				"flex_mus_30.680":"Meteorit",
				"flex_mus_30.70":"Sammlungsbereich",
				"flex_mus_30.80":"Sachgebiet",
				"flex_mus_310.10":"Literatur",
				"flex_mus_310.20":"Objekt in Beziehung zu",
				"flex_mus_320.20.10":"Abgebildet (was)",
				"flex_mus_320.40.10":"Abgebildet (Ort)",
				"flex_mus_320.5":"Abgebildet (wer)",
				"flex_mus_450.10.70":"Fotograf",
				"flex_mus_50.5":"Weitere Nummer",
				"flex_mus_50":"Inventarnummer",
				"flex_mus_5":"LIDO Identifikator",
				"flex_mus_60.10":"Standort",
				"flex_mus_70.10":"Objektgeschichte",
				"flex_mus_70.20":"Art/Anzahl/Umfang",
				"flex_mus_70":"Objektbeschreibung",
				"flex_mus_80.10":"Material",
				"flex_mus_80.20":"Technik",
				"flex_mus_80":"Material/Technik",
				"indexmeta_0002":"Titel",
				"indexmeta_0003":"Verfasser",
				"indexmeta_0004":"Illustrator",
				"indexmeta_0008":"Event",
				"indexmeta_0009":"Event-Beschreibung",
				"indexmeta_0010":"Kontext",
				"indexmeta_0014":"Labels Mediathek",
				"indexmeta_0014":"Signatur",
				"stat_001":"Institution",
				"stat_002":"[Link zur KWE]",
				"stat_003":"[Logo KWE]",
				"stat_004":"[Medientyp-Icon]",
				"stat_005":"[Bezeichnung/Titel]",
				"stat_006":"Link auf diese Seite",
				"stat_007":"Rechteinformation",
				"stat_008":"Objekt beim Datengeber anzeigen",
				"stat_009":"Ansehen in: [Viewer-Name]",
				"stat_010":"Rechte",
				"stat_011":"[Ranking]",
				"stat_012":"Vollständiger Titel",
				"stat_013":"Objekt wurde erworben mit Fördermitteln von",
				"stat_014":"Objekt wurde digitalisiert mit Fördermitteln von",
				"stat_015":"Objekt wurde retrokonvertiert mit Fördermitteln von",
				"video_001":"[Content_Viewer_Video]",
				"video_002":"[Poster-Image]"}



	
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
		retrieves data from the German www.deutsche-digitale-bibliothek.de.
		Sends them back to DataManager in batches of {counter} size
		"""
		#args=[counter:, from_term, predefined]
		
		export_list=[]
		counter=kwargs['counter']
		
		#kwargs={'from_term':kwargs['from_term'], 'predefined':kwargs['predefined']}
		if 'facet' in kwargs and 'facet_value' in kwargs:
			facet= kwargs['facet'] + "=" + kwargs['facet_value']
		else:
			facet=""
		
		if 'id' in kwargs:
			yield self.get_item_by_id(kwargs['id'])
			return
	
		for search_term in 	self.dm.search_term_generator(**kwargs):
			self._open_blocked()
			for match in self._search_generator(search_term, facet, kwargs['timespan'],kwargs):
				print("------------------------------------------")
				print(str(len(export_list)))
				print ("Current search term : ", search_term)
				print("------------------------------------------")
				export_item_family=[]
				if len(match)==0:
					self.nofounds+=search_term+"\n"
				#print (match)
				if 'id' in match:

					if not self.dm.is_in_atom(match['id']):  # data still unknown in AtoM
						print(match)
						item_raw=self.get(match['id'],'items','view','json')
						#print("item_raw -> ", item_raw)
						
						#if self._is_archival_description(item_raw) or kwargs['facet']!="":   # check for archival description if no facet where given else let pass everything
						if self._is_archival_description(item_raw) or facet !="" or 'nopost' in kwargs:   # check for archival description if no facet where given else let pass everything
						
							item_dict=self._get_content(item_raw)
							print("item_dict", item_dict)
							#self._write_to_id_index(match['id'],item_dict['descriptionIdentifier']) #obsolete ?
							#if 'eventStartDates' in item_dict:
							if not ('eventStartDates' in item_dict and 'eventEndDates' in item_dict):
								
								item_dict['eventDates']=self._map('eventDates',item_dict,True,"str",True)
								item_dict['eventStartDates']=self.dm.build_eventDates(item_dict['eventDates'])[0]
								item_dict['eventEndDates']=self.dm.build_eventDates(item_dict['eventDates'])[1]
								
							#print('->->---------------\n',item_dict['eventDates'],item_dict['eventStartDates'],item_dict['eventEndDates'])
							if self.dm._is_in_time(item_dict['eventStartDates'],item_dict['eventEndDates']) or not kwargs['timespan'] or 'nopost' in kwargs:
								#pprint.pprint(item_raw)
								#pprint.pprint(item_dict)
								if not (kwargs['predict']) or self.dm.predict_item(self._get_content_str(item_dict)) or 'nopost' in kwargs :
									item_dict['legacyId']=match['id']
									if 'apd_level_of_description' in item_dict:
										item_dict['levelOfDescription']=self.dm._get_level_of_description(item_dict['apd_level_of_description'])

									# adding mapped fields if original fields are not present
									
									for field in self.FIELD_MAPPING.keys():
										
										item_dict[field]=self._map(field,item_dict,True,"str",True)
									print('fA',item_dict['findingAids'])
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
									
									if (match['id'] not in self.CURRENT_ITEMS and match['id'] not in self.dm.LEGACY_IDS 
										and match['id'] not in self.dm.TMP_OUT and match['id'] not in self.dm.DEF_OUT) :
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
										if not ('nopost' in kwargs):
											print('looking for children')
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
					fo.save_data(self.nofounds,self.dm.TMP_RESULTS_PATH+"nofounds_"+time.strftime("%Y%m%d_%H%M%S")+".txt","txt")
					yield export_list.copy()
					export_list=[]
		fo.save_data(self.nofounds,self.dm.TMP_RESULTS_PATH+"nofounds_"+time.strftime("%Y%m%d_%H%M%S")+".txt","txt")
		yield export_list.copy()
	

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
	
	def _map(self,term, item_dict, get_value=False,type="str", add=True):
		term=term.strip('')
		modified=False
		print("....",term)
		if term in item_dict and not add:
			if get_value:
				return item_dict[term]
			else:
				return term
		print("...ist in item_dict.",term)
		for e in self.FIELD_MAPPING[term]:
			#print(term, e)
			if e in item_dict:
				print (e, "(mapping term) is in dict)")
				if get_value:
					if add:
						if e in self.FIELD_NAMES:
							fieldname=self.FIELD_NAMES[e]+": "
							
						else:
							fieldname=e+": "
							
						if term in self.FIELD_WITHOUT_INTRO or e in self.FIELD_WITHOUT_INTRO:
							fieldname=""
						if term in item_dict:
							item_dict[term]+= "|"+fieldname + item_dict[e]
							modified=True
						else:
							item_dict[term]= fieldname + item_dict[e]
							modified=True
					else:
						return item_dict[e]
				else:
					return e
		else:
			if term in item_dict:
				return item_dict[term]
		
		if get_value:
			if modified:
				return item_dict[term]
			else:
				if type=="list":
					return []
				elif type=="bool":
					return False
				else:
					return ""
		
		else:
			return False
		
		
				
				
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
		return self._map('arrangement',item_dict,True) + "; " + self._map('title',item_dict,True) + "; " + self._map('scopeAndContent',item_dict,True) 
		
		#content_str=""
		#if 'arrangement' in item_dict:
			#content_str+=item_dict['arrangement']+"; "
		#if 'title' in item_dict:
			#content_str+=item_dict['title']+"; "
		#if 'scopeAndContent' in item_dict:
			#content_str+=item_dict['scopeAndContent']+"; "
		#print ("Content String" , content_str)
		#return content_str
			
	

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

	"""
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
		"""
			
	def _search_generator(self,search_term, facet,timespan,kwargs):
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
				r=self.get('','search','','json','"'+search_term[0]+'"',number_of_docs, facet, timespan,kwargs)
				#print ("r:",r)
				if 'results' in r:
					results.extend(r['results'][0]['docs'])
					number_of_results=int(r['numberOfResults'])
					number_of_docs+=int(r['results'][0]['numberOfDocs'])
				else:
					
					return []
				
				MAX_RETRIEVAL=200
				if number_of_results>MAX_RETRIEVAL and search_term[1]!=g.WD_COLONY:
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
			print("We have an error on Ddb._search_generator:", sys.exc_info()[0])
			print(e)	
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			
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
		

	def get(self,item_id, method='items', subtype='aip', accept='json', query='', offset=0 , facet="", timespan=True, kwargs={}):
		try:
			
			data={}
			data['oauth_consumer_key']= self.oauth_consumer_key
			service_url=method + '/' + item_id + '/' +subtype +'?'
			if method=="search":
				service_url=method + "?"
				#data['query']=query+" AND (begin_time:[684832 TO 701267] OR end_time:[684832 TO 701267]) AND sector:sec_01"
				if timespan:
					data['query']=query+" AND (begin_time:[684832 TO 701267] OR end_time:[684832 TO 701267])"
				else:
					data['query']=query
				if 'provider' in kwargs:
					data['_']=kwargs['provider']
				if 'identifier' in kwargs:
					data['query']='apd_reference_number:("'+query+'")'
				if 'htype' in kwargs:
					data['query']+=' AND apd_level_of_description:("'+kwargs['htype']+'")'
				data['sort']='ALPHA_ASC'
				data['row']=1000
				data['offset']=offset
				#data['facet']="sector_fct"
				#data['sector_fct']="sec_01"
				if facet!="":
					tmp=facet.split("=")
					#fct=urllib.parse.quote_plus("facetValues[]")+"="+tmp[0]+"="+urllib.parse.quote_plus(tmp[1])
					data['facet']=tmp[0]
					data[tmp[0]]=tmp[1]
					#data['query']+=" AND "+tmp[0]+":"+tmp[1]
					#print(facet)
					#print(tmp[0])
					#print(tmp[1])
					#fct='&facet='+tmp[0]+"&"+tmp[0]+"="+tmp[1]
					
				else:
					fct=""
				
				
			url_values = urllib.parse.urlencode(data)
			
			#full_url=self.base_url+service_url+url_values+fct
			full_url=self.base_url+service_url+url_values
			
			print(full_url)
			if accept=='json':
				
				r= urllib.request.urlopen(full_url, timeout=60).read().decode("utf-8")
				if item_id+"|"+method not in self.REQUESTED:
					self.REQUESTED.append(item_id+"|"+method)
				d= json.loads(r)
				if 'results' in d :
					count_results=len(d['results'][0]['docs'])
					print ("RESULTS : " , count_results) 
					if count_results==0:
						self.nofounds+=query + "\n"
					#stop=input("PAUSE...")
					if not('nopost' in kwargs) and count_results==0 and offset==0:   # we'll now try to search the ID via the html interface
						empty_result={
											"numberOfResults": 0,
											"results": [{
												"name": "single",
												"docs": [],
												"numberOfDocs": 0
											}],
											"facets": [],
											"entities": [],
											"fulltexts": [],
											"correctedQuery": "",
											"highlightedTerms": [],
											"randomSeed": ""
										}
						data={}
						url=self.BASE_URL_FRONTEND+"searchresults?"
						data['query']=query
						data['isThumbnailFiltered']=False
						data['offset']=0
						data['rows']=10000
						if facet!="":
							data['facetValues[]']=facet
						data = urllib.parse.urlencode(data)
						print (url+data)
						the_page= urllib.request.urlopen(url+data, timeout=60).read().decode("utf-8")
						if the_page:
							tree=html.fromstring(the_page)
							print(len(the_page))
							r=tree.xpath('//div[@class="col-sm-12"]//a/@href')
							if r:
								d=empty_result
								r_new=[]
								for e in r:
									ddbid=e[6:38]
									if not (ddbid in r_new) and ddbid==ddbid.upper():
										d['results'][0]['docs'].append({'id':ddbid})
										
									
								return d.copy()
							else:
								return empty_result
						else:
							return empty_result
					
				#return json.loads(r)
				return d
			if accept=="xml":
				r= urllib.request.urlopen(full_url).read().decode("utf-8")
				#d = xmltodict.parse(r)
				return r
			else:
				return []
		except Exception as e:
			self.dm.store_out_files()
			print('Error! Code: {c}, Message, {m}'.format(c = type(e).__name__, m = str(e)))
			
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(exc_type, fname, exc_tb.tb_lineno)
			
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
		iterates recursivly trough the content of an item and store them into a AtoM ready dict
		
		Parameters:
		result: the raw DDB dict
		item, with_children, with_parents, review: depreciated
		
		"""
		d={}
		#print("result -> " , result)
		for e in self._get_key_value(result, ""):
			#print("===================",e)
			for k,v in e.items():
				if k in d:
					d[k]+="|"+v
				else:
					d={**e,**d}
		
		#print("d -> " , d)
		
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



	def OLD_get_key_value(self,j, k_label=""):
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
		d['culture']=g.CULTURE
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
		if d['eventStartDates']<str(g.SEARCH_TO+1):
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
					d['culture']=g.CULTURE
					d['language']=g.CULTURE
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
					item_dict['culture']=g.CULTURE
					item_dict['language']=g.CULTURE
					item_dict['levelOfDescription']=self.dm._get_level_of_description(match['type'])
					item_dict['repository']=repository
					if 'generalNote' in item_dict:
						item_dict['generalNote']+='|{{Data Source}} "Deutsche Digitale Bibliothek":https://www.deutsche-digitale-bibliothek.de/item/'+item_dict['legacyId']
					else:
						item_dict['generalNote']='{{Data Source}} "Deutsche Digitale Bibliothek":https://www.deutsche-digitale-bibliothek.de/item/'+item_dict['legacyId']
	
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
										item_dict['culture']=g.CULTURE
										item_dict['language']=g.CULTURE
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
							item_dict['culture']=g.CULTURE
							item_dict['language']=g.CULTURE
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
					d['culture']=g.CULTURE
					d['language']=g.CULTURE
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
