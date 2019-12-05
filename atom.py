#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#  atom.py
#  
#  Copyright 2017 FH Potsdam FB Informationswissenschaften PR Kolonialismus projekt-kolonialzeit@fh-potsdam.de>
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
#import pexpect
import pathlib
import logging
#import pycurl
#import xmltodict 
import os
import time
from datetime import datetime
import operator
#import glob
from collections import Counter
import xml.etree.ElementTree as ET
from slugify import slugify
from atom.main import g, data_manager
from atom.helpers import  linguistic, location,deepl,helper
fuo = helper.funcOps()


def main(args):
	

	d_args={"ops":[]}
	#print (args)
	last_key="ops"
	for arg in args:
		kv=arg.split("=")
		if len(kv)==2:
			a=kv[1].split(",")
			d_args[kv[0].replace("-","_")]=kv[1] if len(a)==1 else a
			if kv[1].lower()=="false":
				d_args[kv[0].replace("-","_")]=False
			if kv[1].lower()=="true":
				d_args[kv[0].replace("-","_")]=True			
			
			last_key=kv[0].replace("-","_")
		else:
			if kv[0][0:1]=="-":
				d_args["ops"].append(kv[0])
			else:
				a=kv[0].split(",")
				if isinstance(d_args[last_key],list):
					if len(a)>1:
						d_args[last_key].extend(a)
					else:
						d_args[last_key].append(a[0])
				else:
					d_args[last_key]+=" " + a[0]
					
	
	default_args={"timespan":True, 'predict':True,'predefined':[]}
	d_args=fuo.get_args(d_args,default_args)
		
	print (d_args)		
			
	
	err=0
	if len(args)>1:
		
		if  "-i" in d_args["ops"]:
			if 'source' in d_args:
				dm=data_manager.DataManager()				
				if d_args['source'].lower() in ("ddb","fbn","kal","eadxml","ddbhtml","ape","nad","sca"):
					dm.imports(**d_args)
				else: 
					print ("unknown import source")

			else:
				err=1
		elif "-h" in d_args['ops']:
			f=open("README.md","r")
			helptext=f.read()
			f.close()
			print(helptext)
			
		elif   "-m" in d_args["ops"]:
			if 'action' in d_args:
				if d_args['action'] =="create-sitemap":
					dm=data_manager.DataManager()
					dm.create_sitemaps()
				if d_args['action'] == "publish":
					dm=data_manager.DataManager()
					dm.publish()
				if d_args['action'] =="save-database":
					os=helper.stringOps()
					pass
				if d_args['action'] =="create-location-index":
					print("creating location index")
					loc=location.Location()
					loc.create_location_index()
				if d_args['action']=="make-arrangements":
					dm=data_manager.DataManager()
					repository=""
					if 'repository' in d_args:
						repository=int(d_args['repository'])
					dm.fill_repository_id()
					dm.create_arrangements(repository)
					
				if d_args['action'] =="join-tmp-csv":
					dm=data_manager.DataManager()
					dm.join_csv()
				if d_args['action']=="fill-repository-id":
					dm=data_manager.DataManager()
					dm.fill_repository_id()						
				if d_args['action'] =="reduce-csv":
					dm=data_manager.DataManager()
					dm.reduce_csv(True)	
				if d_args['action'] =="sort-import":
					dm=data_manager.DataManager()
					dm.sort_import()
				if d_args['action'] =="merge-csv":
					dm=data_manager.DataManager()
					dm.merge_csv(args[3],args[4])					
				if d_args['action'] =="update-access-points-list":
					ap=data_manager.access_points()
					ap.update_access_points_list()
				if d_args['action'] =="normalize-name-access-points":
					ap=data_manager.access_points()
					ap.normalize_name_access_points()	
				if d_args['action'] =="add-wd-identifier2actor":
					ap=data_manager.access_points()
					ap.add_wd_identifier2actor()
				if d_args['action'] =="add-wd-identifier2term":
					ap=data_manager.access_points()
					ap.add_wd_identifier2term()
				if d_args['action'] =="normalize-other-access-points":
					ap=data_manager.access_points()
					ap.normalize_other_access_points()		
				if d_args['action'] =="find-name-access-points":
					ap=data_manager.access_points()
					ap.find_name_access_points(True)
				if d_args['action'] =="find-other-access-points":#old
					ap=data_manager.access_points()
					ap.find_other_access_points(True)	
				if d_args['action'] == "add-other-names":
					ap=data_manager.access_points()
					ap.add_other_names()
				if d_args['action'] =="translate-information-objects":
					tr=deepl.deepl()
					if d_args['lang'] in ("en","EN"):
						tr.translate_information_objects("EN")
					if d_args['lang'] in ("fr","FR"):
						tr.translate_information_objects("FR")	
				if d_args['action'] == 'change-taxonomy':
					taxonomy_from=int(d_args['from'])
					taxonomy_to=int(d_args['to'])
					ap=data_manager.access_points()
					ap.change_taxonomy(taxonomy_from, taxonomy_to)				
				
				if d_args['action'] =="add-creator":
					object_slug=d_args['oslug']
					actor_slug=d_args['aslug']
					dm=data_manager.DataManager()
					dm.add_creator(object_slug,actor_slug)
				if 	d_args['action'] =="replace":			
					dm=data_manager.DataManager()	
					search_term=input("search term ? ")
					replace_term=input ('replace_term ? ')	

					culture=input('culture ? ').lower()
					if culture=="":
						culture="de"
					fields=input('fields in information_object_i18n to search for (separated by comma) ? \n (Just Enter for "title, scope_and_content,archival_history)": ')
					if input("ignore case ? y/(n) ") in ( "yes","Yes","y","Y"):
						ignore_case=True
					else:
						ignore_case=False
					if input("words only ? y/(n) ")  in ( "yes","Yes","y","Y"):
						words_only=True
					else:
						words_only=False	
					q="So, you want to replace occurencies of '"+search_term+"' by '" + replace_term +"' in culture='"+culture+"' ?  y/(n)"
					if input(q) not in ("yYjJ"):
						sys.exit() 
					print ("\n\n\nPLEASE MAKE SURE YOU HAVE A BACKUP COPY OF YOUR DATABASE!\n\n\n")
					dm.replace(	search_term,replace_term,culture,fields, words_only,ignore_case)	
				if d_args['action'] =="find-access-points-in-atom":
					ap=data_manager.access_points()
					if 'last_revision' in d_args:
						last_revision=d_args['last_revision']
					else:
						last_revision=""		
					print("lr",last_revision)	
					if d_args['type'] in ("Wikidata", "wd","WD"):
						print("open Wikidata corpus")
						dm=data_manager.DataManager()

						ap.find_access_points_in_atom("wd",last_revision)	
					else:
						print ("open AtoM corpus")
						ap.find_access_points_in_atom("atom",last_revision)
					print ("normalize access points")
					ap.normalize_name_access_points()
					print ('normalize other access_points')
					ap.normalize_other_access_points()	
					#ap.find_other_access_points(True)
					print ('clean up lower relations')
					ap.clean_lower_relations()
					ap.rebuild_nested()
				if d_args['action'] =="index-wd-keywords":
					
					dm=data_manager.DataManager()
					dm._index_keywords()									
				if d_args['action'] =="create-keyword-list":
					dm=data_manager.DataManager()
					dm.create_keyword_list()									
					
		elif d_args['action']=="build-eventDates":
			dm=data_manager.DataManager()
			print("testing: build_EventDates")
			print(dm.build_eventDates(d_args['param']))
		elif args[1] in ('--create_access_points', "-a"):
			pass
		elif args[1] in ("--location", "-p"):
			pass
		elif args[1] in ("--linguistic", "-l"):
			pass
		elif args[1] in ("--keywords", "-k", "--index-keywords"):
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
					from atom.helpers import ai
					if len(args)>4:
						print(ki.predict(args[3]))
		elif args[1] in ("--help", "-h"):
			print(g.HELP_STRING)
		

	if err==0:
		return 0
	else:
		print ("Not enough arguments. See ./atom-dm -h or --help for more information")
		return 1


if __name__ == '__main__':
	import sys    

sys.exit(main(sys.argv))




