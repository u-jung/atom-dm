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


class Linguistic(object):
	
	corpus_file="/tmp/corpus.csv"
	
	def __init__(self):
		self.dm=data_manager()
	
	def __del__(self):
		pass
		
		
	def create_corpus(self):
		"""
		atom database dump an corpus creation
		"""
		if os.path.isfile(API_KEY_FILE):
				with open(API_KEY_FILE, 'r') as file:
					keys = json.load(file)
				file.close()
				key=keys['sudo']
		self.es = Elasticsearch()
		#logging.basicConfig(level=logging.DEBUG)
		if os.path.isfile(self.corpus_file):
			sudo_exec('sudo rm ' + self.corpus_file, key)
			
		sql='select ifnull(title,""), ifnull(alternate_title,""), ifnull(scope_and_content,"") from information_object_i18n  into outfile "'+self.corpus_file+'";'
		result=self.dm.get_mysql(sql,False,False)
		
		
			
		bashCommand= "cat "+self.corpus_file+" | sed 's|<[^>]*>|\ |g' | sed 's|([0-9])([A-ZÄÖÜ])>|\1\ \2|g'  | sed -E -e 's/,|;|:|\)|\(|\t|\\N|\\|\"|\"|<|>|\/|„|“|\[|\]|\\\|,//g' | tr ' ' '\n'| tr '.' '\n'| sort | uniq"
		#work around
		bashCommand= "cat data/words.txt"
		print(bashCommand)
		output=subprocess.check_output(['bash','-c', bashCommand]).decode("utf-8")

		self.dm.es.indices.delete(index='words')		
		self.dm.es.indices.create(index='words')
		
		out_arr=output.split("\n")
		for e in out_arr:
			if len(e)>1:
				self.dm.es.index(index="words", doc_type="words",  body={"word": e})

