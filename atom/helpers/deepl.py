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
from atom.config import api_keys
import urllib.parse, urllib.request, urllib
import json

class deepl(object):
	CORRECTION=[]
	
	def __init__(self):
		self.CORRECTION={"en":{"protected area":"protectorate",
								"Conservation area": "protectorate",
								"protected areas":"protectorates",
								"Conversation areas":"protectorates",
								"stock creation": "holdings"
								}}

	def translate_information_objects(self, target_lang,from_lang="DE"):
		dm=data_manager.DataManager()
		done=[]

		sql='select i.id,i.repository_id,io.title,io.scope_and_content,io.archival_history,io.culture from information_object i join information_object_i18n io on i.id=io.id ;'
		information_objects=dm.get_mysql(sql,False, False)
		#sql='select n.id,ni.content,ni.culture from note n  join note_i18n ni on n.id=ni.id where n.type_id='+str(g.A_LANGUAGE_NOTE_ID)+';'
		sql='select n.id,ni.content,ni.culture, n.type_id, n.object_id from note n  join note_i18n ni on n.id=ni.id ;'
		notes=dm.get_mysql(sql,False, False)
		# create a corpus of existing translations
		for e in information_objects:
			#print(len(done))
			if e[5].upper()==target_lang:
				de_item=next((x for x in information_objects if x[0]==e[0] and x[5]=="de"),False)
				if de_item:
					for i in range(2,5):
						if not(de_item[i] is None):
							exist=next((x for x in done if x[1]  and x[0]==de_item[i]),False)
							if not exist:
								done.append((de_item[i],e[i]))
		for e in notes:
			if e[3]!=g.A_LANGUAGE_NOTE_ID:
				if e[2].upper()==target_lang:
					de_item=next((x for x in notes if x[0]==e[0] and x[2]=="de"),False)
					if de_item:
						if not(de_item[1] is None):
							exist=next((x for x in done if x[1]  and x[0]==de_item[1]),False)
							if not exist:
								done.append((de_item[1],e[1]))
		
		#print(len(done), done)
		##a=input("STOP")
		# start iterating
		k=0
		for e in information_objects:
			print("---- ",k ,"/" , len(information_objects) )
			if e[1]==124547:
				continue
			title=""
			scope_and_content=""
			archival_history=""
			add_lang_note=False
			new_content=False
			if e[5].lower()=="de":
				if not(e[2]is None):
					r=next((x for x in done if x[0]==e[2]),False)
					if r:
						title=r[1]
						print("known ", title[0:40])
					else:
						print("unknown ", e[2][0:40])
						title=so.escapeQuotes(self.translate_once(e[2],target_lang))
						if title is None:
							title=""
						else:
							new_content=True
					if title!="":
						done.append((e[2],title))
				if not(e[3] is None):
					r=next((x for x in done if x[0]==e[3]),False)
					if r:
						scope_and_content=r[1]
						print("known ", scope_and_content[0:40])
					else:
						print("unknown ", e[3][0:40])
						scope_and_content=so.escapeQuotes(self.translate_once(e[3],target_lang))
						if scope_and_content is None:
							scope_and_content=""
						else:
							new_content=True
					if scope_and_content!="":
						done.append((e[3],scope_and_content))
				if not(e[4] is None):
					r=next((x for x in done if x[0]==e[4]),False)
					if r:
						archival_history=r[1]
						print("known ", archival_history[0:40])
					else:
						print("unknown ", e[4][0:40])
						archival_history=so.escapeQuotes(self.translate_once(e[4],target_lang))
						if archival_history is None:
							archival_history=""
						else:
							new_content=True
					if archival_history!="":
						done.append((e[4],archival_history))
				
				if title+scope_and_content+archival_history=="":
					continue

				if new_content:	
					
					r=next((x for x in information_objects if x[0]==e[0] and x[5]==target_lang.lower()),False)
					if r:
						sql='update information_object_i18n set title="'+title+'", scope_and_content="'+scope_and_content+'", archival_history="'+archival_history+'" where id='+str(e[0])+' and culture ="'+target_lang.lower()+'";'
					else:
						sql='insert into information_object_i18n (id,title,scope_and_content,archival_history, culture) values ('+str(e[0])+',"'+title+'","'+scope_and_content+'","'+archival_history+'","'+target_lang.lower()+'");'
					print(sql)
					r=dm.get_mysql(sql,True,True)
					k+=1
					if k>4000:
						break
				else:
					continue
				add_lang_note=True
				individual_notes=[x for x in notes if x[4]==e[0] and x[2]=="de"]
				language_note_id=0
				if individual_notes:
					
					for note in individual_notes:
						if note[3]!=g.A_LANGUAGE_NOTE_ID:
							r=next((x for x in done if x[0]==note[1]),False)
							if r:
								content_translated=r[1]
							else:
								content_translated=self.translate_once(note[1],target_lang)
						
							r=next((x for x in notes if x[0]==note[0] and x[2]==target_lang.lower()),False)
							if r:
								if r[1]!=content_translated:
									sql='update note_i18n set content="'++'" where id='+str(note[0])+';'
									r=dm.get_mysql(sql,True,True)
							else:
								sql='insert into note_i18n (content,id,culture) values ("'+content_translated+'",'+str(note[0])+',"'+target_lang.lower()+'");'
								r=dm.get_mysql(sql,True,True)
							if archival_history!="":
								done.append((note[1],content_translated))
						else:
							language_note_id=note[0]
								
				# add language note
				if add_lang_note:
					if target_lang=="EN":
						message= "This description was automatically translated with the help of www.DeepL.com. Translation errors are possible. Please note that the document itself has not been translated."
					else:
						message="Cette description a été automatiquement traduite à l'aide de www.DeepL.com. Des erreurs de traduction sont possibles. Veuillez noter que le document lui-même n'a pas été traduit."
					
					if language_note_id ==0:
						r=next((x for x in notes if x[4]==e[0] and x[3]==g.A_LANGUAGE_NOTE_ID),False)
						if r:
							language_note_id=r[0]
						else:
							sql='insert into note (object_id,type_id, source_culture) values ('+str(e[0])+','+str(g.A_LANGUAGE_NOTE_ID)+',"'+target_lang.lower()+'");'
							language_note_id=dm.get_mysql(sql,True,True)
							sql='insert into note_i18n (content,id,culture) values ("deutsch",'+str(language_note_id)+',"de");'
							r=dm.get_mysql(sql,True,True)

					
					if language_note_id >0:
						r=next((x for x in notes if x[4]==e[0] and x[2]==target_lang.lower()),False)
						if r:
							sql= 'update note_i18n set content="'+message+'" where id='+str(language_note_id)+' and culture="'+target_lang.lower()+'";'
						else:
							sql='insert into note_i18n (content,id,culture) values ("'+message+'",'+str(language_note_id)+',"'+target_lang.lower()+'");'
						#print(sql)
						r=dm.get_mysql(sql,True,True)
		#print(done)


	def translate_once(self, text, target_lang, from_lang="DE"):
		if text is None:
			return ""
		quota=True
		try:
			url = 'https://api.deepl.com/v1/translate'
			data="auth_key="+api_keys.API_KEYS['DeepL']+"&target_lang="+target_lang+"&text="+text
			data = data.encode('utf-8')
			headers = {}
			headers["Content-Type"]= "application/x-www-form-urlencoded"
			req = urllib.request.Request(url, data, headers = headers)
			resp = urllib.request.urlopen(req)
			respData = resp.read()
			r=respData.decode('utf-8')
			if r.find("uota exeeded")>-1:
				quota=False
				raise ValueError('A very specific bad thing happened')
			r=json.loads(r)
			print("translated ", r['translations'][0]['text'][0:30])
			return self.correct(r['translations'][0]['text'],target_lang)
			
		except Exception as e:
			if not quota:
				raise ValueError('Not enough deepl quota!!!!')
			print ("Error")
			print(str(e))
			return ""


	def correct(self,text,target_lang):
		for key in self.CORRECTION[target_lang.lower()].keys():
			if text.find(key)>-1:
				text.replace(key,self.CORRECTION[key])
		return text
