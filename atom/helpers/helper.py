#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  helper.py
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




import csv
import os
import json
import re
import platform
import pexpect

class fileOps(object):
	"""
	different methods of using files
	"""
	
	def __init__(self):
		return
	
	def load_data(self,source_file, data_type=None, list_output=False):
		"""
		Opens a file and returns a text or a json. The returned data_type will be determinated by the file name or the list_output parameter
		"""
		if data_type is None:
			data_type=source_file[-4:].lower()
			data_type=data_type.strip(".")		
		if os.path.isfile(source_file):
			f=open(source_file, 'r')
			if data_type== "txt":
				if list_output:
					data=f.read().split('\n')
					data.pop(-1)
				else:
					data=f.read()
			elif data_type=="csv":
				reader = csv.DictReader(f)
				data = [row for row in reader]
			elif data_type=="json":
				data=json.load(f)
			f.close()
			if isinstance(data, str):
				return data
			else:
				return data.copy()
		else:
			
			print("File ", source_file, " not found!")
			if data_type=="txt":
				return ""
			elif data_type in ("json","csv") or list_output:
				print(data_type)
				return []
			else:
				return None
		
	
		
	def load_data_once(self,data_var,source_file,data_type=None):
		"""
		Opens a file and returns a text or a json. The returned data_type will be determinated by the file name or the list_output parameter.
		Before opening the file, this method checks if the target object is empty and need to be filled
		"""
		
		if isinstance(data_var,str):
			if data_var=="":
				obj= self.load_data(source_file,data_type,True)
				return obj
		elif isinstance(data_var, list) or isinstance (data_var, tuple):
			if len(data_var)==0 :
				obj= self.load_data(source_file,data_type,True)
				return obj
		elif isinstance(data_var,dict):
			if data_var=={}:
				onj= self.load_data(source_file,data_type,True)
				return obj
		return data_var
	
		
	
	def load_once(self,target,source_file,data_type=None):
		"""
		wrapps load_data_once
		"""
		return self.load_data_once(target, source_file, data_type)
		
		
	
	def save_data(self,data,destination_file, data_type=None, fieldnames=None):
		"""
		Stores data into a file depending on the data_type
		"""
		if not data_type:
				data_type=destination_file[-4:].lower()
				data_type=data_type.strip(".")
		f=open(destination_file, 'w')
		if data_type == "txt":
			if isinstance(data,str):
				f.write(str(data))
			else:
				print("data needs to be a string or a number but not a list, tuple or dict!")
				return 0
		elif data_type =="csv":
			if fieldnames:
				if isinstance(fields,list):
					writer = csv.DictWriter(f, fieldnames=fieldnames,extrasaction='ignore', delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_ALL)
					writer.writeheader()
					for e in data:
						if instance(e,data):
							writer.writerow(e)
						else:
							print("data must be a list of dicts")
							return 0
				else:
					print("data must be a list ")
					return 0
		elif data_type=="json":
			if isinstance(data, list) or isinstance (data, tuple) or isinstance(data,dict):
				f.write(json.dumps(data.copy(), sort_keys=True, indent=2, ensure_ascii=False))
			else:
				print("data must be a json like object (list, tuple or dict")
				return 0
		f.close()
		return 1
			
		
	def add_data(self, new_data, destination_file, data_type=None, list_output=False):
		"""
		Adds data to a file
		"""
		existing_data=self.load_data(destination_file,data_type,list_output)
		
		if existing_data:
			if isinstance(existing_data,str):
				if isinstance(new_data,str):
					data=existing_data+new_data
			elif isinstance(existing_data,list):
				if isinstance(new_data,list) or isinstance(new_data,tuple):
					data=list(existing_data) + list(new_data)
			elif isinstance(existing_data,dict):
				if isinstance(new_data,dict):
					new_data.update(existing_data)
					data=new_data
			elif isinstance(existing_data,tuple):
				if isinstance(new_data,list) or isinstance(new_data,tuple):
					data=tuple(list(existing_data) + list(new_data))
			if self.store_data(data,destination_file,data_type, list_output):
				print("data added")
			else:
				print("there was an error on adding data")
		else:
			print("Could not find existing data")
		
	
	
class listOps(object):
	
	def __init__(self):
		pass
		
	def dedup(self, l):
		"""
		deduplicates a list
		"""
		return list(self.uniq(l))

	def uniq(self,l):
		last = object()
		for item in l:
			if item == last:
				continue
			yield item
			last = item

	def remove_duplicate_followers(self,text):
		new_l=[]
		words=text.split(" ")
		for i in range(0,len(words)-1):
			if words[i]!=words[i+1]:
				new_l.append(words[i])
		return " ".join(new_l)
		


class stringOps(object):
	
	SATZZEICHEN=".,;:)(/\?!_{}„…“»«›‹"
	KURZZEICHEN="§$%&#~*|"
	ANFUHRUNGSZEICHEN='"'
	LQGQ="<>"
	STRICHE="–-"
	
	def __init__(self):
		pass
		
	def replaceChars(self,text,string_of_chars,replace_to):
		"""
		Replaces all characters inside string_of_chars by replace_to
		"""
		for c in string_of_chars:
			text=text.replace(c,replace_to)
		
		return text
	
	
	
	def dedupSpaces(self,text):
		while text.find("  ")>-1:
			text=text.replace("  "," ")
		return text
	
	def stripHtml(self, text,replacment=""):
		tag_re = re.compile(r'(<!--.*?-->|<[^>]*>)')
		return tag_re.sub('', text)

	def stripBr(self, text):
		text=text.replace("<br>","\n")
		text=text.replace("<br />"," \n")
		return text



class osOps(object):
		
		def __init__(self):
			pass
	
		def sudo_exec(self,cmdline, passwd):
			"""
			Executes a sudo command in bash
			"""
			osname = platform.system() # 1
			if osname == 'Linux':
				prompt = r'\[sudo\] password for %s: ' % os.environ['USER']
			elif osname == 'Darwin':
				prompt = 'Password:'
			else:
				assert False, osname

			child = pexpect.spawn(cmdline)
			idx = child.expect([prompt, pexpect.EOF], 3) # 2
			if idx == 0: # if prompted for the sudo password
				self.log.debug('sudo password was asked.')
				child.sendline(passwd)
				child.expect(pexpect.EOF)
			return child.before

		def remove_all(self, pathname):
			"""
			removes all files and directories recursivly from a given path name
			"""
			fcount=0
			dcount=0
			for root, dirs, files in os.walk(pathname):
				for f in files:
					fcount+=1
					os.remove(os.path.join(root,f))
				for d in dirs:
					dcount+=1
					os.rmdir(os.path.join(root,d))
			return (dcount,fcount)

