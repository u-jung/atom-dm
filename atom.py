#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#  atom.py
#  
#  Copyright 2017 FH Potsdam FB Informationswissenschaften PR Kolonialismus <kol@fhp-kol-1>
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






"""
Handles the import of data into the candidate database from external sources as well of their export to AtoM csv.


External sources could be:
- EAD2002-XML files
- Archival Description from www.archiveportal-europe.net
- Archival Descriptions and institunional data from www.deutsche-digitale-bibliothek.de 
- Meta data from www.deutsche-biographie.de

"""



import sys
import platform

import logging
import subprocess
import pexpect
import pathlib
import logging
import pycurl
import xmltodict 
import os
import time

from datetime import datetime

import operator
import glob

from collections import Counter

import xml.etree.ElementTree as ET
from slugify import slugify

from atom.main import g, data_manager
from atom.helpers import ai, linguistic, location,deepl


def main(args):
	
	print (args)
	err=0
	if len(args)>1:
		if args[1] in ("--import", "-i"):
			if len(args)>2:
				dm=data_manager.DataManager()				
				if args[2]=="ddb":
					dm.imports("DDB","archival_description","","",True,"".join(args[3:4]))
				if args[2] in ("fbn","FBN"):
					predefined=[]
					if len(args)>5:
						if args[5].lower() in ("true", "predefined"):
							predefined=g.PREDEFINED_SEARCH_TERMS
					print(predefined)
					dm.imports("FBN","archival_description","","",True,"","",args[3],args[4],predefined)
				elif args[2]=="kal":
					dm.imports("KAL")
				elif args[2]=="eadxml":
					#./atom-dm -i eadxml {import file} 
					if len(args)>3 and os.path.isfile(args[3]):
						if len(args)>4:
							source_str=args[4]
						else:
							source_str=""
						dm.imports("EADXML","archival_description",args[3],"","","",source_str)
					else:
						print("file not found")
			else:
				err=1
		elif args[1] in ("--maintainance", "-m"):
			if len(args)>2:
				if args[2] == "publish":
					dm=data_manager.DataManager()
					dm.publish()
				if args[2] in ("join-tmp-csv","join_tmp_csv"):
					dm=data_manager.DataManager()
					dm.join_csv()
				if args[2] in ("reduce-csv"):
					dm=data_manager.DataManager()
					dm.reduce_csv(True)	
				if args[2] in ("merge-csv"):
					dm=data_manager.DataManager()
					dm.merge_csv(args[3],args[4])					
				if args[2] in ("update-access-points-list"):
					ap=data_manager.access_points()
					ap.update_access_points_list()
				if args[2] in ("normalize-name-access-points"):
					ap=data_manager.access_points()
					ap.normalize_name_access_points()	
				if args[2] in ("normalize-other-access-points"):
					ap=data_manager.access_points()
					ap.normalize_other_access_points()		
				if args[2] in ("find-name-access-points"):
					ap=data_manager.access_points()
					ap.find_name_access_points(True)
				if args[2] in ("find-other-access-points"):#old
					ap=data_manager.access_points()
					ap.find_other_access_points(True)	
				if args[2] in ("translate-information-objects"):
					tr=deepl.deepl()
					if args[3] in ("en","EN"):
						tr.translate_information_objects("EN")
					if args[3] in ("fr","FR"):
						tr.translate_information_objects("FR")					
									
				if args[2] in ("find-access-points-in-atom", "find-ap"):
					ap=data_manager.access_points()
					if args[3] in ("Wikidata", "wd","WD"):
						print("open Wikidata corpus")
						dm=data_manager.DataManager()
						ap.find_access_points_in_atom("wd",args[4] or "")	
					else:
						print ("open AtoM corpus")
						ap.find_access_points_in_atom("atom",args[4] or "")
					print ("normalize access points")
					ap.normalize_name_access_points()
					print ('normalize other access_points')
					ap.normalize_other_access_points()	
					#ap.find_other_access_points(True)
					print ('clean up lower relations')
					ap.clean_lower_relations()
					ap.rebuild_nested()
				if args[2] in ("index-wd-keywords","index-wd-corpus"):
					dm=data_manager.DataManager()
					dm._index_keywords()									
					
		elif args[1] in ('--create_access_points', "-a"):
			pass
		elif args[1] in ("--location", "-p"):
			pass
		elif args[1] in ("--linguistic", "-l"):
			pass
		elif args[1] in ("--keywords", "-k"):
			dm=data_manager.DataManager()	
			dm._index_keywords()
		elif args[1] in ("--build-upper-levels"):
			dm=data_manager.DataManager()	
			pa=dm.TMP_RESULTS_PATH+time.strftime("%Y%m%d_%H%M%S")
			pathlib.Path(pa).mkdir(parents=True, exist_ok=True) 
			l=dm.build_upper_levels(None,None,None)
			dm._post_import(l,pa)
			dm.write_csv(l,pa+"/import.csv","archival_description")
		elif args[1] in ("--clean", "-c"):
			dm=data_manager.DataManager()
			if len(args)>2:
				if args[2] in ("de","en","fr"):
					dm.strip_tags(args[2])
				else:
					dm.strip_tags("de")
			else:
				dm.strip_tags("de")
		elif args[1] in ("--ai", "-ai"):
			ki=ai.ai()
			if len(args)>2:
				if args[2] == "add_data":
					if len(args)>3:
						data=None
						index=None
						target=None
						for arg in args[3:]:
							if arg[0:2].lower()=="-d":
								data=arg[3:]
							if arg[0:2].lower()=="-t":
								target=arg[3:]
							if arg[0:2].lower()=="-i":
								index=arg[3:]	
						ki.add2data(data,target,index)	
				if args[2] == "add-data-from-file-list":
					
					if len(args)>3:
						ki.add2data_from_file_list(args[3])
					else:
						ki.add2data_from_file_list(None)	
				if args[2] =="add-data-from-atom":
					ki.add2data_from_atom()	
				if args[2] =="word2ix":
					ki.index_words()	
				if args[2] in ("create_sets","create-sets"):
					print("Creating files ", ki.TRAIN_DATA_TEXT_FILE , " and " , ki.TEST_DATA_TEXT_FILE , " with ratio " , ki.RATIO)
					ki.create_sets(ki.RATIO)
				if args[2] =="train":
					ki.training()
				if args[2] =="test":
					ki.testing()
				if args[2] =="predict":
					
					if len(args)>4:
						print(ki.predict(args[3]))
		elif args[1] in ("--help", "-h"):
			print(g.HELP_STRING)
		

	if err==0:
		return 0
	else:
		print ("Not enough arguments. See ./atomdm -h or --help for more information")
		return 1


if __name__ == '__main__':
	import sys    

sys.exit(main(sys.argv))




