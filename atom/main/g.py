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

WD_ENTITY_QUERY='SELECT DISTINCT ?item where{\
			{optional{?item wdt:P17/wdt:P279* wd:Q329618 .}} \
			union \
			{optional{?item wdt:P2541/(wdt:P131|wdt:P279) wd:Q329618 .}}\
			union{ optional{?item wdt:P131/wdt:P279* wd:Q329618 .}} \
			union{ optional{?item wdt:P361/wdt:P279* wd:Q329618 .}}\
			union{ optional{?item wdt:P2650/wdt:P279* wd:Q329618 .}}\
			union{ optional{?item wdt:P937/(wdt:P131|wdt:P279)* wd:Q329618 .}}\
			}\
			order by ?item'


#A cookie is necessary to retrieve data from findbuch net
PHP_SESSION_COOKIE ='PHPSESSID=56570757349f28e903a9ce6894a474a5'

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
	"afri",
	"schutztruppe",
	"kamerun",
	"deutsch-ostafrika",
	"deutsch südwest-afrika",
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
	"kiautschou"
	
]
