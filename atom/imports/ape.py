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

class APE():
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

	def __init__(self,filename):
		self.RESULTS_DICT=fo.load_data(filename)
		print(len(self.RESULTS_DICT))
		self.RESULTS_DICT=self.calculate_urls()
		#print(json.dumps(self.RESULTS_DICT, indent=2))
		fo.save_data(self.RESULTS_DICT,"/tmp/result.json")
		print(len(self.RESULTS_DICT))
		a=input("stop")
		pass
	
	def calculate_urls(self):
		"""
		- calculates upper levels of the item
		- cuts non essential parts of the url
		Note: APE uses a system of aicode, id and unitid inside the url where:
		- aicode refers to the institution
		- id refers to the fonds
		- unitid refers to the class and the object
		Inside the unidid different levels are seperated by _SLASH or _COLON.
		The first part of unitid represents once again the id but is misleading to a broken link
		"""
		new_results=[]

		for result in self.RESULTS_DICT:
			new_result=[]
			#shorten the url
			#print(result['url'])
			if result['url'].find("/search/")>-1:
				result['url']=result['url'][0:result['url'].find("/search/")]
			url_arr=result['url'].split("/")
			aicode=""
			fonds=""
			unitid=""
			for i,s in enumerate(url_arr):

				if s=="aicode":
					aicode=url_arr[i+1]
				if s=="id":
					fonds=url_arr[i+1]
				if s=='unitid':
					unitid=url_arr[i+1]			
			
			if aicode[0:2] in ("PL"):
				continue
				
			
			if unitid=="" :
				#we have a fonds
				new_result.append({
					'legacyId':fonds,
					'parentId':aicode,
					'levelOfDescription':"Bestand",
					'url':result['url'],
					'findingAids':result['url'],
					'title':result['title']
				})
				
				#result['legacyId']=fonds
				#result['parentId']=aicode
				#result['levelOfDescription']="Bestand"
				#continue
			else:
				#we have a unit or classes + unit
				#first we write the ai record 
				
				new_result.append({
					'legacyId':aicode,
					'levelOfDescription':'Archivtektonik',
					'parentId':"",
					'findingAids':'',
					'url':""
						
				})
				# and then the Fonds
				new_result.append({
					'legacyId':aicode+'_'+fonds,
					'levelOfDescription':'Bestand',
					'parentId':aicode,
					'findingAids':'https://www.archivesportaleurope.net/ead-display/-/ead/pl/aicode/'+aicode+'/type/fa/id/'+fonds,
					'url':'https://www.archivesportaleurope.net/ead-display/-/ead/pl/aicode/'+aicode+'/type/fa/id/'+fonds,
				})				
				# and now we analyze the unitid
				tmp=unitid
				level="Objekt"
				a=[]
				a.append(tmp)
				while len(re.split("_SLASH_|_COLON_",tmp))>1:
					if tmp.rfind("_SLASH_")> tmp.rfind("_COLON_"):
						tmp=tmp[0:tmp.rfind("_SLASH_")]
					else:
						tmp=tmp[0:tmp.rfind("_COLON_")]
					a.append(tmp)
				a.sort(key = len)
				#print(a)
				#z=input("stop")
				for i,e in enumerate(a):
					d={}
					d['legacyId']=aicode+'_'+fonds+'_'+e
					d['levelOfDescription']='Gliederung'
					d['findingAids']='https://www.archivesportaleurope.net/ead-display/-/ead/pl/aicode/'+aicode+'/type/fa/id/'+fonds+'/unitid/'+e
					d['url']='https://www.archivesportaleurope.net/ead-display/-/ead/pl/aicode/'+aicode+'/type/fa/id/'+fonds+'/unitid/'+e
					if i==0:
						if e!=fonds:
							d['parentId']=aicode+'_'+fonds
					elif i==1:
						d['parentId']=aicode+'_'+fonds
					else:
						d['parentId']=aicode+'_'+fonds+'_'+a[i-1]
					if i==len(a)-1:
						d['levelOfDescription']='Objekt'
					new_result.append(d)
					
			new_results.extend(new_result)
		l_arr=list(set([x['legacyId'] for x in new_results]))
		print ("uniq :", len(l_arr))
		new_uniq_results=[]
		#new_uniq_results=new_results
		for e in new_results:
			if not next((x for x in new_uniq_results if x['legacyId']==e['legacyId']),False):
				new_uniq_results.append(e)
		self.RESULTS_DICT=new_uniq_results
		return self.RESULTS_DICT
	
	def export(self,**kwargs):
		print ("Start export from ape")
		nofounds=[]
		for counter,result in enumerate(self.RESULTS_DICT):
			url=result['url']
			result['culture']="de"
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
			e=tree.xpath('//h3[@id="contextInformation"]/a/text()')
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
		
		print(json.dumps(nofounds, indent=2))
		print("RESULTS: ", len(self.RESULTS_DICT), len([x for x in self.RESULTS_DICT if 'legacyId' in x]))
		a=input("stop")
		yield [x for x in self.RESULTS_DICT if 'legacyId' in x]


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
		

