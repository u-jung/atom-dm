#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  location.py
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


class Location(object):
	location_file="/home/kol/kol/data/atom_geo.csv"
	
	def __init__(self):
		self.dm=DataManager()
	
	def __del__(self):
		pass
		
	def create_location_index(self):
		self.dm.es.indices.delete(index='geo')		
		self.dm.es.indices.create(index='geo')

		f=open(self.location_file,"r")

		for line in f:
			
			line=line[:-1]
			array=line.split("\t")
			print(array[0],array[1])
			geo_line={"geonames_id":array[0],
					"loc":array[1],
					"loc_ascii":array[2],
					"loc_alt":array[3],
					"lon":array[4],
					"lat":array[5],
					"feature_class":array[6],
					"feature_code":array[7],
					"country":array[8],
					"map":array[9],
					"error":array[10]}
			self.dm.es.index(index="geo", doc_type="geo",  body=geo_line)
			
		f.close()
	
