#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  access_points.py
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

from atom.main import data_manager, g
import re
from atom.helpers.helper import fileOps, listOps, stringOps, osOps
fo=fileOps()
lo=listOps()
so=stringOps()
oo=osOps()



class access_points(object):
	
	ACCESS_POINTS_LIST=[]
	ACCESS_POINTS_LIST_FILE="atom/data/access_points.json"
	dm=data_manager.DataManager()
	SUBJECT_AP=35
	PLACE_AP=42
	GENRE_AP=78
	
	def __init__(self):
		
		self.ACCESS_POINTS_LIST= fo.load_data(self.ACCESS_POINTS_LIST_FILE)
		self.update_access_points_list()
		
	
	
	
	def update_access_points_list(self):
		sql='select t.id, t.parent_id,ti.name,ti.culture, t.taxonomy_id from term_i18n ti join term t on ti.id=t.id  where t.taxonomy_id in (35,42,78) and ti.culture in ("en","fr","de") order by ti.name;'
		r=self.dm.get_mysql(sql,False)
		sql='select group_concat(name) as term from other_name o join other_name_i18n oi on o.id=oi.id where object_id='+str(r[0][0])+' group by object_id;'
		#print(sql)
		alt_names=self.dm.get_mysql(sql,True)
		for e in r:
			item=next((x for x in self.ACCESS_POINTS_LIST if x['id']==e[0]),False)
			if item:
				item['culture_'+e[3]]=e[2]
				item['parent_id']=e[1]
				item['taxonomy_id']=e[4]
				
				if alt_names:
					if 'inidcators' in e:
						item['indicators']=list(set(item['indicators']).union(set(alt_names[0].split(","))))
					else:
						item['indicators']=alt_names[0].split(",")
			
			else:
				d={}
				d['culture_'+e[3]]=e[2]
				d['parent_id']=e[1]
				d['type']=e[4]
				d['id']=e[0]
				
				if alt_names:
					d['indicators']=alt_names[0].split(",")
				self.ACCESS_POINTS_LIST.append(d)
		fo.save_data(self.ACCESS_POINTS_LIST,self.ACCESS_POINTS_LIST_FILE)


	def lookup(self, text, culture="de", return_text=True):
		"""
		Checks if one or mor access_points could be connected to a given text,
		If return_text==False, term_ids will be returned instead
		"""
		text=text.strip(" ")
		tmp={SUBJECT_AP:[],PLACE_AP:[],GENRE_AP:[]}
		placeAccessPoints=[]
		genreAccessPoints=[]
		for ap in self.ACCESS_POINTS_LIST:
			test=False
			if 'exclusions' in ap:
				for ex in ap['exclusions']:
					pattern=re.compile(ex,re.IGNORECASE)
					if re.search(p,text):
						continue
			if 'culture_'+culture in ap:
				pattern=re.compile(ap['culture_'+culture],re.IGNORECASE)
				if re.search(p,text):
					test=True
					
			if 'indicators' in ap:
				for ind in ap['indicators']:
					pattern=re.compile(ind,re.IGNORECASE)
					if re.search(p,text):
						test=True
			if test:	
				if return_text:
					if 'culture_'+culture in ap:
						tmp[ap['type']].append(ap['culture_'+culture])
					elif 'culture_de' in ap:
						tmp[ap['type']].append(ap['culture_de'])
					else:
						tmp[ap['type']].append(ap['id'])
				else:
					tmp[ap['type']].append(ap['id'])
		return tmp
		
