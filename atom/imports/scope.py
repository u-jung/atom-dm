#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  ape.py
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
"""
####  How to extract the the links to the records?
####  Using jQuery console
####  a) liste=[]
####  b) make a manual search in APE
####  c) liste.push.apply(liste,jQuery('#searchresultsList .unittitle'))
####  After a while...
####  d) for(i=0;i<liste.length;i++){console.log(liste[i].href + " || " + liste[i].title)}
####  e) copy the liste and create a json file with a list of dicts where each dict contains "url" and "title" tags.
####
"""

import urllib
import json
import sys
from lxml import html
import re

from atom.main import data_manager, g
#from http.cookiejar import CookieJar
from atom.helpers.helper import fileOps, listOps, stringOps, osOps
fo = fileOps()
lo = listOps()
so = stringOps()
oo = osOps()

dm=data_manager.DataManager()

class ScopeArchiv():
	"""
	Lookup for data in a single record inside Archive Portal Europe
	"""
	RESULTS_DICT={}
	MAPPINGS=(
		('Scope and content',"scopeAndContent"),
		("Records creator's history","archivalHistory"),
		("Source of acquisition","acquisition"),
		("Conditions governing access","accessConditions"),
		("Conditions governing reproduction","reproductionConditions"),
		("Other finding aids","findingAids"),
		("Existence and location of copies","locationOfCopies"),
		("Condition of the material","physicalCharacteristics"),
		("Extent","extentAndMedium"),
		("General remarks","generalNote"),
		("Other descriptive information","archivistNote"),
		("Language of the material","languageNote"),
		("Records creator","eventActors"),
		("Content provider","repository"),
		("Publication note","publicationNote"),
	)

	#only KAB
	LEVELS={"17":"Tektonik","14":"Tektonik","24":"Tektonik","16":"Bestand","21":"Gliederung","7":"Akt(e)",
	"15":"15","18":"Gliederung","19":"19","20":"20","22":"22","23":"23",
	"1":"Objekt","6":"Akt(e)","11":"Gliederung","5":"Sammlung", "3":"Objekt","10":"","8":"","12":""}
	
	
	


	def __init__(self,filename):
		#filename="/tmp/data (Kopie).json"
		self.RESULTS_DICT=fo.load_data(filename)
		print(len(self.RESULTS_DICT))
		#self.RESULTS_DICT=self.calculate_urls()
		#print(json.dumps(self.RESULTS_DICT, indent=2))
		fo.save_data(self.RESULTS_DICT,"/tmp/result.json")
		print(len(self.RESULTS_DICT))
		#a=input("stop")
		pass
	
	
	
	def export(self,**kwargs):
		print ("Start export from scopeArchiv")
		nofounds=[]
		r=[]
		fo.save_data(r,"/tmp/raw_export.json","json")
		for counter,result in enumerate(self.RESULTS_DICT):
			d={}
			#url="https://"+kwargs['base']+".scopearchiv.ch/"+result['url']
			url=result['url']
			print (url)
			d['culture']="de"
			d['legacyId']="DE-"+kwargs['base']+"_"+result['url'].split("=")[1]
			d['url']=url
			if url=="":
				continue
			#result['findingAids']=url+ "   "
			print(counter, url)
			raw_data=self.get(url)
			if raw_data.find('The document you have requested does not exist.')>-1:
				nofounds.append({'url':url})
				print("Fehler: ", len(nofounds))
				continue
			if raw_data=="":
				nofounds.append(  {'url':url})
				print("Fehler: ", len(nofounds))
				continue
			tree=html.fromstring(raw_data)
			#print(raw_data)
			labels=tree.xpath('//td[@class="veXDetailAttributLabel"]/text()')
			values=tree.xpath('//td[@class="veXDetailAttributValue"]/text()')
			rows=tree.xpath('//td[@class="veXDetailAttributLabel"]/text()|//td[@class="veXDetailAttributValue"]/text()|//td[@class="veDetailHighlightAttributValue"]/text()')
			levels=tree.xpath('//div[@class="rtBot"]//a/text() | //div[@class="rtBot"]//a/@href | //div[@class="rtBot"]//img/@src ')
			levelsText=tree.xpath('//div[@class="rtBot"]//a/text()')
			levelsLink=tree.xpath('//div[@class="rtBot"]//a/@href')
			parents=[]
			for i,e in enumerate(levels):
				dp={"culture":g.CULTURE}
				if e[0:1]!="/":
					continue
				else:
					level=e.split("/")[-1:][0].split(".")[0]
					print("level ", level)
					if level=="7":
						continue
					dp['levelOfDescription']=self.LEVELS[level]
					dp['legacyId']="DE-"+kwargs["base"]+"_"+levels[i+1].split("=")[1]
					dp['Titel:']=levels[i+2]
					if i >2:
						dp['parentId']="DE-"+kwargs["base"]+"_"+levels[i-2].split("=")[1]
					else: 
						dp['parentId']="DE-"+kwargs["base"]+"_1"
					parents.append(dp.copy())
			for e in parents:
				print(e)
			print("----------")
			k="dummy"
			v=""
			for e in rows:
				#print("row", e, e[-1:])
				if e[-1:]!=":":
					v+=e+"; "
				else:
					v=v.strip(" ")
					d[k]=v.strip(";")
					if k.find("index")>-1:
						d[k]=d[k].replace(";","|")
					v=""
					k=e
			print(parents)
			d['parentId']=parents[len(parents)-1]['legacyId']
			for e in parents:
				if not(next((x for x in r if x['legacyId']==e['legacyId']),False)):
					r.append(e.copy())
			"""
			print(len(labels),len(values))
			for i,e in enumerate(values):
				d[labels[i]]=e.strip(" ")
				if labels[i].find("index")>-1:
					d[labels[i]]=e.replace(";","|")
				print(labels[i],e)
			"""
			r.append(d.copy())	
			
			"""	
			a=input("Stop")
			
			result['repository']=e[0]
			e=tree.xpath('//div[@class="eadid"]/text()')
			print(e)
			if len(e)>0:
				result['identifier'] = e[0]
			else:
				result['identifier'] = ""
			e=tree.xpath('//div[@class="subtitle"]/text()')
			if len(e)>0:
				result['eventDates'] = e[0]	
			else:
				result['eventDates']=""
			e=tree.xpath('//h1[@class="titleproper"]/text()')    
			if not len(e) >0:
				e=tree.xpath('//{http://www.w3.org/1999/xlink}h1[@class="titleproper"]/text()') 
			if len(e)>0:
				result['title'] = e[0]	
			else:
				if not 'title' in result:
					result['title']=""
			e=tree.xpath('//div[@id="eadcontent"]//h2/text()')
			f=tree.xpath('//div[@id="eadcontent"]//div[@class="ead-content"]')
			print(len(e),len(f))

			result['eventStartDates']=dm.build_eventDates(result['eventDates'])[0]
			result['eventEndDates']=dm.build_eventDates(result['eventDates'])[1]
			#result['levelOfDescription']="Objekt"
			
			
			for i,s in enumerate(f):
				field=next((x[1] for x in self.MAPPINGS  if x[0]==e[i]),None)
				
				if not field is None:
					if not field in result:
						result[field]=""
					result[field]+= re.sub('<[^>]*>', '', html.tostring(s).decode("utf-8"))+ " "
					result[field]=result[field][0:10000] 
				else:
					
					c=re.sub('<[^>]*>', '', html.tostring(s).decode("utf-8")).strip() + " "
					pos= c.find("Subjects:")
					if pos>-1:
						c=c.replace("Subjects:","#subjectAccessPoints#")
					
					pos= c.find("Geographic names:")
					if pos>-1:
						c=c.replace("Geographic names:","#placeAccessPoints#")
					pos= c.find("Personal names:")
					if pos>-1:
						c=c.replace("Personal names:","#nameAccessPoints#")
					print(c)
					a=c.split("#")
					if len(a)>1:
						for ii, ss in enumerate(a):
							if ss in ("subjectAccessPoints","placeAccessPoints","nameAccessPoints"):
								result[ss]=a[ii+1].replace(",","|")
				if 'parentId' in result:
					parent=next((x for x in self.RESULTS_DICT if x['legacyId']==result['parentId'] and x['levelOfDescription']=="Archivtektonik"),False)
					if parent:
						if not 'title' in parent or parent['title']=="":
							parent['title']=result['repository']

			print(result)
		
		#print(json.dumps(nofounds, indent=2))
		print("RESULTS: ", len(self.RESULTS_DICT), len([x for x in self.RESULTS_DICT if 'legacyId' in x]))
		a=input("stop")
		yield [x for x in self.RESULTS_DICT if 'legacyId' in x]
		"""
		
		r=self.mapping(r,kwargs['base'])
		dm.write_csv(r,"/tmp/import.csv","archival_description")
		r=dm.hierarchy_sort(r)[0]
		dm.write_csv(r,"/tmp/import.csv","archival_description")
		fo.save_data(r,"/tmp/raw_export.json","json")
		yield r.copy()
		a=input("stop")

	def get(self,url):
		try:
			headers = {}
			r = urllib.request.Request(url, None, headers)
			with urllib.request.urlopen(r) as (response):
				the_page = response.read().decode('utf-8')
				#print (the_page)
				
				return the_page
		except:
			print("echec")
			return ""
		

	def mapping(self,l, base):
		for e in l:
			if 'Titel:' in e:
				e['title']=e['Titel:']
				if 'Band:' in e:
					e['title']+=", Band " + e['Band:']
			if 'Entstehungszeitraum:' in e:
				e['eventDates']=e['Entstehungszeitraum:']
			if 'Enthält:' in e:
				e['scopeAndContent']=e['Enthält:']
			else:
				e['scopeAndContent']=""
			if 'Klassifikation:' in e:
				e['archivalHistory']=e['Klassifikation:']
			if 'Bestandsbeschreibung:' in e:
				e['scopeAndContent']+="; "+ e['Bestandsbeschreibung:']
			if 'Personenindex:' in e:
				#e['nameAccessPoints']=e['Personenindex:']
				pass
			if 'Ortsindex:' in e:
				e['placeAccessPoints']=e['Ortsindex:'].replace(",","|")
			if 'Aktenbildner-/Provenienzname:' in e:
				e['eventActors']=e['Aktenbildner-/Provenienzname:']
			if 'Archivalienart:' in e:
				e['genreAccessPoints']=e['Archivalienart:'].replace("/","|")
			if 'Physische Benützbarkeit:' in e:
				e['conditionOfAccess']=e['Physische Benützbarkeit:']
			if 'Signatur:' in e:
				e['identifier']=e['Signatur:']
			if 'Stufe:' in e:
				if e['Stufe:']=="Verzeichnungseinheit":
					e['levelOfDescription']="Akt(e)"
			if 'Beschreibung:' in e:
				e['scopeAndContent']+="; "+ e['Beschreibung:']
			if 'Autor/Fotograf/Künstler:' in e:
				e['scopeAndContent']+="| Autor/Fotograf: "+ e['Autor/Fotograf/Künstler:']
			if 'Geschichte der Institution mit Archivbeständen:' in e:
				e['scopeAndContent']+="; "+ e['Geschichte der Institution mit Archivbeständen:']
			if 'Bilderschließung:' in e:
				e['scopeAndContent']+="; "+ e['Bilderschließung:']
				
	
			url="http://"+base+".scopearchiv.ch/detail.aspx?ID="+e['legacyId'][7:]
			e['generalNote']='{{Data Source}} "Kirchliches Archivzentrum Berlin":' + url
			e['findingsAids']=url
			e['repository']='Kirchliches Archivzentrum Berlin'
			if 'Format (H x B x T) in cm:' in e:
				e['extentAndMedium']=e['Format (H x B x T) in cm:']
			if 'eventDates' in e:
				e['eventStartDates']=dm.build_eventDates(e['eventDates'])[0]
				e['eventEndDates']=dm.build_eventDates(e['eventDates'])[1]
			else:
				e['eventStartDates']=""
				e['eventEndDates']=""
		return [x for x in l if x['eventStartDates']<"1946"]
