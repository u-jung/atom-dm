#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  global.py
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


##API_KEY_FILE="/home/kol/api_keys.json"

# This is the main wikidata sparql query which defines the corpus of keywords
# make sure it's working inside https://query.wikidata.org


BASE_URL = 'https://archivfuehrer-kolonialzeit.de/'

WD_ENTITY_QUERY_OLD='SELECT DISTINCT ?item where{\
			{optional{?item wdt:P17/wdt:P279* wd:Q329618 .}} \
			union \
			{optional{?item wdt:P2541/(wdt:P131|wdt:P279) wd:Q329618 .}}\
			union{ optional{?item wdt:P131/wdt:P279* wd:Q329618 .}} \
			union{ optional{?item wdt:P361/wdt:P279* wd:Q329618 .}}\
			union{ optional{?item wdt:P2650/wdt:P279* wd:Q329618 .}}\
			union{ optional{?item wdt:P937/(wdt:P131|wdt:P279)* wd:Q329618 .}}\
			}\
			order by ?item'

WD_ENTITY_QUERY='SELECT DISTINCT ?item ?itemLabel ?itemDescription where \
{ ?item (wdt:P17|wdt:P19|wdt:P20|wdt:P27|wdt:P36|wdt:P119|wdt:P131|wdt:P159|wdt:P180|wdt:P189|wdt:P276|wdt:P279|wdt:P291|wdt:P361|\
wdt:P551|wdt:P740|wdt:P915|wdt:P840|wdt:P921|wdt:P937|\
wdt:P1001|wdt:P1071|wdt:P1269|wdt:P1376|wdt:P1416|\
wdt:P2341|wdt:P2541|wdt:P2647|wdt:P2650)/(wdt:P31*|wdt:P361*|wdt:P131*|wdt:P279*) wd:Q329618 . \
SERVICE wikibase:label { bd:serviceParam wikibase:language "de,fr,en". } } order by ?item'

WD_CORPUS_ROOT_ELEMENT='Q329618'   #here it's "Deutsche Kolonien und Schutzgebiete

WD_RETRIEVE_LABELS_QUERY='SELECT DISTINCT ?item (group_concat(?i;separator="|")  as ?instances)  \
					(group_concat(?d;separator="|")  as ?description)  \
					(group_concat(?short;separator="|")  as ?shortLabel)\
					(group_concat(?de;separator="|")  as ?label_de) \
					(group_concat(?fr;separator="|")  as ?label_fr) \
					(group_concat(?en;separator="|")  as ?label_en) \
					(group_concat(?itemLabel;separator="|")  as ?Label)\
					(group_concat(?itemAltLabel;separator="|")  as ?altLabel) \
					WHERE {\
					  bind(wd:#1# as ?item).\
					  ?item wdt:P31 ?i.\
					  optional{?item rdfs:label ?itemLabel .}\
					  optional{?item schema:description ?d.  FILTER (lang(?d) = "de" )}\
					  optional {?item wdt:P1813 ?short.}\
					  optional{?item skos:altLabel ?itemAltLabel .}\
					  FILTER (lang(?itemLabel) = "de" || lang(?itemLabel) = "en" || lang(?itemLabel) = "fr" ).\
					  bind(if(lang(?itemLabel)="de",?itemLabel,"") as ?de).\
					  bind(if(lang(?itemLabel)="en",?itemLabel,"") as ?en).\
					  bind(if(lang(?itemLabel)="fr",?itemLabel,"") as ?fr).}\
					group by ?item'


# Special Objects for Prediction  (Binary Classification)
PROFESSION_CLASSES= {
			"BEAMTER":["beamter","richter","jurist","verwalter", "assessor","schriftste","regierung", "gouverneur","leiter","zoll"],
			"MILITAER":["offizier","kommandeur","milit", "hauptmann", "leutnant","major", "ober"],
			"MISSIONAR":["angehöerig", "mission", "theolog"],
			"ANGESTELLTER":["heizer", "maurer","schlosser","angestellte", "hilfe","schreiber", "zeichner"],
			"UNTERNEHMER":["unternehmer","kaufmann","händler", "pionier"],
			"MEDIZINER":["arzt", "mediziner", "bakterio"],
			"WISSENSCHAFTLER":["geograph","reisende","geologe","ethnologe","wissenschaft", "backterio","forsch", "archäolog","biolog","zoolog", "ethnograph"]
			}

PREDICTION_THRESHOLD=300  #Minumun score to classify an item as True


# CONST WIKIDATA Instances
WD_HUMAN="Q5"	
WD_MISSION="Q20746389"
WD_ENTERPRISE="Q4830453"
WD_AUTHORITY="Q327333"
WD_MILITARY="Q45295908"
WD_SETTLEMENT="Q486972"
WD_EVENT="Q15815670"
WD_COLONY="Q133156"
WD_GEOGRAFICAL="Q618123"
WD_SUBJECT="Q82550"
WD_RELIGION=""
WD_ASSOCIATION=""
WD_SCIENCE=""





# AtoM CONST   
# To check your entity_type_ids, use SQL = 
# SELECT entity_type_id, name  FROM actor a JOIN term_i18n ti on a.entity_type_id=ti.id WHERE culture="de" GROUP BY entity_type_id ;

A_ENTITY_TYPES={
"HUMAN":132,
"ORGANISATION":131,
"BUSINESS":150495,
"AUTHORITY":150496,
"ASSOCIATION":150497,
"MILITARY":150498,
"SCIENCE":150499,
"RELIGION":150558,
"SOCIAL_GROUP":930069}

ENTITY_TYPES={
"HUMAN":(132,"Q5"),
"ORGANISATION":(131,("Q43229")),
"BUSINESS":(150495,("Q4830453")),
"AUTHORITY":(150496,("Q327333")),
"ASSOCIATION":(150497,("Q48204")),
"MILITARY":(150498,("Q45295908")),
"SCIENCE":(150499,("Q2385804")),
"RELIGION":(150558,("Q9174","Q20746389")),
"SOCIAL_GROUP":(930069,("Q2472587")),
"PLACE":("place",("Q618123","Q133156","Q486972")),
"GENRE":("genre",("Q483394")),
"SUBJECT":("subject",("Q82550","Q15815670"))
}


A_TYPE_OTHER_FORM_OF_NAME=149
A_TYPE_PARALLEL_FORM_OF_NAME=148

A_RELATION_TYPE_HAS_PHYSICAL_OBJECT=147
A_RELATION_TYPE_HIERARCHICAL=150
A_RELATION_TYPE_NAME_ACCESS_POINTS=161
A_RELATION_TYPE_CONVERSE_TERM=177
A_RELATION_TYPE_IS_SUPERIOR_OF=189
A_RELATION_TYPE_CONTROLS=191
A_RELATION_TYPE_IS_CONTROLLED_BY=192
A_LANGUAGE_NOTE_ID=174

A_SOURCE_NOTE=121

# depreciated
A_LEVELS_OF_DESCRIPTION={
	57124:((),"Plan of records groups"),
	1080400:((57124),"Record group"),
	227:((1080400,57124),"Fonds"),
	228:((227),"Subfonds"),
	229:((1080400,57124),'Collection'),
	450:((227,229),'Class'),
	230:((450,229,227),'Series'),
	231:((230),'Subseries'),
	232:((230,231,227,229),'File'),
	233:((232,229,290),'Object'),
	290:((232,233),'Part')
}


A_LOD=[
  {
    "id": 1080400,
    "de": "Tektonik",
    "en": "Record Group",
    "fr": "Classement"
  },
  {
    "id": 57124,
    "de": "Archivtektonik",
    "en": "Plan of record groups",
    "fr": "Classement des fonds"
  },
  {
    "id": 450,
    "de": "Gliederung",
    "en": "Class",
    "fr": "Gliederung"
  },
  {
    "id": 227,
    "de": "Bestand",
    "en": "Fonds",
    "fr": "Fonds"
  },
  {
    "id": 232,
    "de": "Akt(e)",
    "en": "File",
    "fr": "Dossier"
  },
  {
    "id": 290,
    "de": "Teil",
    "en": "Part",
    "fr": "Partie"
  },
  {
    "id": 228,
    "de": "Teilbestand",
    "en": "Subfonds",
    "fr": "Sous-fonds"
  },
  {
    "id": 233,
    "de": "Objekt",
    "en": "Item",
    "fr": "Pièce"
  },
  {
    "id": 229,
    "de": "Sammlung",
    "en": "Collection",
    "fr": "Collection"
  },
  {
    "id": 230,
    "de": "Serie",
    "en": "Series",
    "fr": "Série"
  },
  {
    "id": 231,
    "de": "Teilserie",
    "en": "Subseries",
    "fr": "Sous-série"
  }
]


ACCESS_POINTS={
		WD_HUMAN:"nameAccessPoints",
		WD_MISSION:"nameAccessPoints",
		WD_ENTERPRISE:"nameAccessPoints",
		WD_AUTHORITY:"nameAccessPoints",
		WD_MILITARY:"nameAccessPoints",
		WD_SETTLEMENT:"placeAccessPoints",
		WD_COLONY:"placeAccessPoints",
		WD_EVENT:"subjectAccessPoints"	
		}

MONTHS={"januar":"01",
		"februar":"02",
		"märz":"03",
		"april":"04",
		"mai":"05",
		"juni":"06",
		"juli":"07",
		"august":"08",
		"september":"09",
		"oktober":"10",
		"novembeR":"11",
		"dezember":"12",
		"jan":"01",
		"feb":"02",
		"febr":"02",
		"apr":"04",
		"jun":"06",
		"jul":"07",
		"aug":"08",
		"sept":"09",
		"okt":"10",
		"nov":"11",
		"dez":"12",
		"jänner":"01"}
		
# Other Consts
CULTURE="de"        # AtoM needs the ISO 639-1 value of a known language

SATZZEICHEN=".,;:-)(/\?!_–{}„…“»«›‹"   # A string of non wanted characters in stemming

MIN_EXPORT_LIST_LEN=100 #size of chunks for data imports from external sites.

HELP_STRING="USAGE:  atom-dm [OPTION] ... [SOURCE] [DEST] \
			\nMANDATORY ELEMENTS:"

BASE_DIR="/home/kol/kol/atomdm/"


SEARCH_TO='1945'
SEARCH_FROM='1850'

PREDEFINED_SEARCH_TERMS=[
	"koloni",
	"kolonial",
	"koloniale",
	"kolonialer",
	"koloniales",
	"kolonialen",
	"reichskolonialamt",
	"kolonialgesellschaft",
	"afri",
	"schutztruppe",
	"schutztruppen",
	"kamerun",
	"deutsch-ostafrika",
	"deutsch südwest-afrika",
	"deutsch-südwestafrika",
	"mahinland",
	"witu",
	"wituland",
	"togo",
	"neuguinea",
	"samoa",
	"karolinen",
	"marianen",
	"bismarck-archipel",
	"tsingtau",
	"kiautschou",
"kolonialverein",
"deutsche schutzgebiete",
"deutsches schutzgebiet",
"marshall-inseln",
"jaluit",
"saipan",
"bismarckarchipel",
"bismarck-archipel",
"duala",
"jaunde",
"buea",
"daressalam",
"tanga",
"windhuk",
"lüderitzbucht",
"swakopmund",
"palau",
"bourgainville",
"kaiser-wilhelmsland",
"kaiser-wilhelm-land",
"urindi",
"ruanda",
"anecho"

	
	
	
]


BLOCKED_BY_DESCRIPTION=[
	 'eisenbahnverwaltung', 'schreiber', 'heizer', 'polizei', 'polizist'
]



#search terms which will return too much noise if used in queries
NOISING_SEARCH_TERMS=[
	"hauses","marianne", "engelberg", "sb", 'erster weltkrieg', 'hausa', 'Hotel Kaiserhof', 'maria barbara', "fleischkonservenfabrik","mofa", "maurer", "bergwerks ag",
	"caprivi","adventisten","bekanntmachung","zentralbüro","reiterdenkmal","chefarzt","betriebsdirektor","kassenbeamter","maria margaretha",
	"maria christina","bohrgesellschaft","standard bank","oblaten","schenk"
]


RED = '\033[91m'
OLIV = '\033[32m'
CEND = '\033[0m'


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


LEVEL_OF_DESCRIPTION_INDEX= {'Section':'htype_001', 
 'Appendix':'htype_002', 
 'Contained work':'htype_003', 
 'Annotation':'htype_004', 
 'Address':'htype_005', 
 'Article':'htype_006', 
 'Volume':'htype_007', 
 'Additional':'htype_008', 
 'Intro':'htype_009', 
 'Entry':'htype_010', 
 'Fascicle':'htype_011', 
 'Fragment':'htype_012', 
 'Manuscript':'htype_013', 
 'Issue':'htype_014', 
 'Illustration':'htype_015', 
 'Index':'htype_016', 
 'Table of contents':'htype_017', 
 'Chapter':'htype_018', 
 'Map':'htype_019', 
 'Multivolume work':'htype_020', 
 'Monograph':'htype_021', 
 'Music':'htype_022', 
 'Serial':'htype_023', 
 'Privilege':'htype_024', 
 'Review':'htype_025', 
 'Text':'htype_026', 
 'Verse':'htype_027', 
 'Preface':'htype_028', 
 'Dedication':'htype_029', 
 'Fonds':'htype_030', 
 'Class':'htype_031', 
 'Series':'htype_032', 
 'Subseries':'htype_033', 
 'File':'htype_034', 
 'Item':'htype_035', 
 'Collection':'htype_036', 
 '?':'htype_037', 
 'Tektonik_collection':'tektonik_collection', 
 'Collection':'repository', 
'Institution': 'Institution'}

LEVELS_OF_DESCRIPTION = {"en":
		{'htype_001': 'Section', 
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
	 'institution': 'Institution'},
	"de":
		{'htype_001': 'Section', 
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
	 'htype_030': 'Bestand', 
	 'htype_031': 'Gliederung', 
	 'htype_032': 'Serie', 
	 'htype_033': 'Teilserie', 
	 'htype_034': 'Akt(e)', 
	 'htype_035': 'Object', 
	 'htype_036': 'Sammlung', 
	 'htype_037': '?', 
	 'htype_038': '?', 
	 'htype_039': '?', 
	 'tektonik_collection': 'Tektonik', 
	 'repository': 'Archivtektonik', 
	 'institution': 'Archivtektonik'},
	 "fr":
		{'htype_001': 'Section', 
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
	 'institution': 'Institution'}	 }


PHYSICAL_OBJECT_LEVELS=(
		'File',
		'Akt(e)',
		'Dossier',
		'Object',
		'Objekt',
		'Pièce',
		'Item',
		'Part',
		'Teil'
		)

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


jump_fonds = [
	 'BArch R 1001', 'BArch R 1002', 'BArch R 1003', 'BArch R 8023', 'BArch R 8024', 'BArch R 1003', 'BArch R 8124', 'BArch RW 51', 'BArch RH 88', 'BArch RW 51']

KLASSEN=[]


START_TIME_ISO="1850-01-01"
END_TIME_ISO="1919-12-31"
