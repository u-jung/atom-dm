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

# CONST WIKIDATA Instances
WD_HUMAN="Q5"	
WD_MISSION="Q20746389"
WD_ENTERPRISE="Q4830453"
WD_AUTHORITY="Q327333"
WD_MILITARY="Q45295908"
WD_SETTLEMENT="Q486972"
WD_EVENT="Q15815670"
WD_COLONY="Q133156"

WD_MIN_EXPORT_LIST_LEN=100 



# AtoM CONST   
# To check your entity_type_ids, use SQL = 
# SELECT entity_type_id, name  FROM actor a JOIN term_i18n ti on a.entity_type_id=ti.id WHERE culture="de" GROUP BY entity_type_id ;

A_ENTITY_TYPE_HUMAN=132
A_ENTITY_TYPE_ORGANISATION=131
A_ENTITY_TYPE_BUSINESS=150495
A_ENTITY_TYPE_AUTHORITY=150496
A_ENTITY_TYPE_ASSOCIATION=150497
A_ENTITY_TYPE_MILITARY=150498
A_ENTITY_TYPE_SCIENCE=150499
A_ENTITY_TYPE_RELIGION=150558


A_TYPE_OTHER_FORM_OF_NAME=149
A_TYPE_PARALLEL_FORM_OF_NAME=148

A_RELATION_TYPE_HAS_PHYSICAL_OBJECT=147
A_RELATION_TYPE_HIERARCHICAL=150
A_RELATION_TYPE_NAME_ACCESS_POINTS=161
A_RELATION_TYPE_CONVERSE_TERM=177
A_RELATION_TYPE_IS_SUPERIOR_OF=189
A_RELATION_TYPE_CONTROLS=191
A_RELATION_TYPE_IS_CONTROLLED_BY=192




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

HELP_STRING="USAGE:  atom-dm [OPTION] ... [SOURCE] [DEST] \
			\nMANDATORY ELEMENTS:"
