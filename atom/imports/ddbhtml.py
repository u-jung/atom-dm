#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  findbuch.py
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



import urllib
import json
import sys
import html as htmllib
from lxml import html


from atom.main import data_manager, g
#from http.cookiejar import CookieJar
from atom.helpers.helper import fileOps, listOps, stringOps, osOps, funcOps
fo = fileOps()
lo = listOps()
so = stringOps()
oo = osOps()
fuo =funcOps()

dm=data_manager.DataManager()

class Ddbhtml():
	"""
	Lookup for data inside findbuch.net representation of a specific archive
	Retrieve them
	How to get the archive_id ?
	1. Go to the entry page of the archives online database at www.{archive_url}.findbuch.net
	2. Open the debug console of your browser 
	3. enter:  ar_id
	
	"""
	
	RESULTS_DICT={}
	
	MAX_RESULTS=200
	ARCHIVE_URL=""
	ARCHIVE_ID=""
	BASE_URL="https://www.deutsche-digitale-bibliothek.de/"
	CONVERTED_LEVEL={
					"Fonds":"be_e",
					"File":"ve_e",
					"Class":"k",
					"Tektonik":"t"
					}
	
	FIELD_MAPPING={
	"title":['Titel', 'Band', "Dokumenttitel", "Inhalt","Angabe des Objekts"],
	"scopeAndContent":['Enthält', "Darin", 'Verweis', "Beschreibung","Enthält auch", 'Enthält', 'Personenindex',"Institutionenindex"],
	"eventDates":["Dat. => Findbuch", "Datierung","Datierung von","Datierung bis","Dat. - Findbuch"],
	"extendAndMedium":['Umfang',"Format / Umfang", "Medium","Maßstab"],
	'relatedUnitsOfDescription':['Quelle'],
	'findingAids':['findingAids'],
	"repository":[],
	"eventActors":[],
	"physicalCharacteristics":['Erhaltung', 'Fototyp', 'Dokumenttyp',  "Dokumentart", "Format"],
	"script":[],
	"languageNote":[],
	"generalNote":["Abbruchdatum","Vermerk","Originalbeschriftung","Straßenumbenennung","Sicherung","Provenienz","Provenienz/Quelle","Edition", "Entnommen", "Reservefeld", "Verzeichnungsprotokoll","Bemerkungen","Bemerkung", "Bereich"],
	"identifier":["l. Num."],
	"alternativeIdentifiers":["alte Archiv-Sign.", "Spruchakte", "l. Num.", "Signatur", "PADUA", "alte Archiv-Sign."],   #always accompagned by d['alternativeIdentifierLabels']='Former call number'
	"relatedUnitsOfDescription":["Microfilm/-fiche", "Digital?"],
	"archivalHistory":["Überlieferungsgeschichte"],
	"accessConditions":["Rechte", "gesperrt bis","gesperrt für","Besondere Sperrfrist"],
	"reproductionConditions":["Rechte"],
	"language":[],
	"locationOfOriginals":[],
	"locationOfCopies":[],
	"publicationNote":[],
	"subjectAccessPoints":["Sachindex"],
	"placeAccessPoints":["Land", "Gerichtsort 1","Gerichtsort 2", "Handlungsort 1", "Handlungsort 2"],
	"nameAccessPoints":["Fotograf/Autor","Fotograf / Verlag","Personenindex"],
	}
	
	PREFIXES =(
	"Rechte",
	"Personenindex",
	"Institutionenindex",
	"l. Num.",
	"Vermerk",
	"Originalbeschriftung",
	"Format",
	"Maßstab",
	"Quelle",
	"Abbruchdatum",
	"Dokumentart",
	"Format / Umfang",
	"Medium",
	"Bemerkungen",
	"Sicherung",
	"Besondere Sperrfrist",
	'Dokumenttyp',
	"PADUA",
	"gesperrt bis",
	"gesperrt für",
	"Erhaltung",
	"Fototyp",
	"Entnommen",
	"Spruchakte",
	"Darin",
	"Enthält",
	"Verweis",
	"Beschreibung",
	"Provinienz",
	"Provenienz/Quelle",
	"Edition",
	"Microfilm/-fiche",
	"Verzeichnungsprotokoll",
	"Bemerkung",
	"Enthält auch",
	"Digital?",
	"Bereich"
	)
	
	def __init__(self):
		pass
		
	
	
	def export(self,**kwargs):
		"""
		retrieves data from the German from {archive}.findbuch.net.
		Sends them back to DataManager in batches of {counter} size
		"""
		
		counter=kwargs['counter']
		if 'facet' in kwargs and 'facet_value' in kwargs:
			facet= kwargs['facet'] + "=" + kwargs['facet_value']
		else:
			facet=""
		
		
		export_list=[]
		not_to_retrieve=['ferienkolonie','gartenkolonie','wohnkolonie', 'künstlerkolonie', 'villenkolonie','arbeiterkolonie']
		for term in dm.search_term_generator(**kwargs):
			search_term=term[0]
			for match in self._search_generator(search_term, facet, kwargs['timespan']):
				#print (export_list)
				e=next((x for x in export_list if x["fb_id"]==match['fb_id']),False)
				tmp=[str(v) for k,v in match['props'].items()] + [str(x) for x in match['top']]
				for test in not_to_retrieve:
					if " ".join(tmp).lower().find(test)>-1:
						continue
				print("Export list len : ", len(export_list))
				if not e:
					for k,v in match['props'].items():
						if k in self.PREFIXES:
							prefix=k+": "
						else:
							prefix=""
						for field_k, field_v in self.FIELD_MAPPING.items():
							if k in field_v:
								if field_k in match:
									match[field_k]+=" | "+prefix + v 
								else:
									match[field_k]=prefix + v
					match['language']="de"
					match['languageNote']="deutsch"
					
					match['repository']=match['top'][0]
					match['culture']="de"
					match['languageOfDescription']="de"

					if 'eventDates' in match:
						match['eventStartDates']= dm.build_eventDates(match['eventDates'])[0]
						match['eventEndDates']= dm.build_eventDates(match['eventDates'])[1]
					if 'reproductionConditions' in match:
						match['reproductionConditions']+=" | Das Recht zur Weiternutzung der Dokumente unterliegt den Bestimmungen des Archivs. Sie ist gegebenenfalls nur nach Zustimmung des Archivs gestattet."
					else:
						match['reproductionConditions']="Das Recht zur Weiternutzung der Dokumente unterliegt den Bestimmungen des Archivs. Sie ist gegebenenfalls nur nach Zustimmung des Archivs gestattet."

					if not ('title' in match):
						match['title']=match['top'][len(match['top'])-1]
					#export_list.append(match.copy())
					hierarchy_titles=match['top'].copy()
					
					if not (next((x for x in export_list if x['legacyId']==self.ARCHIVE_ID+"_"+self.ARCHIVE_URL),False)):
						d={}
						d['legacyId']=self.ARCHIVE_ID+"_"+self.ARCHIVE_URL
						d['title']=match['repository']
						d['levelOfDescription']='Institution'
						d['repository']=match['repository']
						d['culture']=match['culture']
						d['fb_id']=self.ARCHIVE_ID
						export_list.append(d.copy())
						print("added to list:" , d['title'] if 'title' in d else "NO TITLE", " for: " , search_term)
					selected=['title','scopeAndContent','placeAccessPoints','subjectAccessPoints','nameAccessPoints']
					item_str=" "
					for s in selected:
						if s in match:
							item_str+=match[s] + " "
					print("item str:", item_str[0:300], d)
					if 'arrangement' in d:
						arrangement=d['arrangement']
					else:
						arrangement=""
					if predefined==[]:
						prediction=dm.predict_item(item_str, arrangement, False, True,False)
						if prediction:
							match['prediction']=prediction
							print(prediction)
						else:
							continue
					else:
						print("Prediction passed")
					
					for i in range (0,len(match['top'])+1):
						if len(match['hierarchy'])>i:
							elem=match['hierarchy'][i]
							if elem[0:2] in ("k_","b_","t_"):
								e=next((x for x in export_list if x["fb_id"]==elem),False)
								if not e:
									d={}
									d['legacyId']=self.ARCHIVE_ID+"_"+elem
									if i+1 < len(match['hierarchy']):
										d['parentId']=self.ARCHIVE_ID+"_"+match['hierarchy'][i+1]
									else:
										d['parentId']=self.ARCHIVE_ID+"_"+self.ARCHIVE_URL
									d['title']=hierarchy_titles.pop()
									if elem[0:2] in ("b_"):
										d['scopeAndContent']=self.get("","props",elem[2:],"Fonds")
									d['language']=match['language']
									d['repository']=match['repository']
									d['culture']=match['culture']
									d['languageOfDescription']=match['languageOfDescription']
									d['levelOfDescription']="Fonds" if elem[0:1]=="b" else "Class" if elem[0:1]=="k" else "Tektonik_collection" if elem[0:1]=="t" else "Institution"
									d['fb_id']=elem
									export_list.append(d.copy())
									print("added to list:" , d['title'] if 'title' in d else "NO TITLE")
						else:
							print("broken hierarchy")
					del match['top']
					del match['props']
					del match['hierarchy']
					match={k:so.dedupSpaces(v) for k,v in match.items()}
		
					export_list.append(match.copy())
					print("added to list:" , match['title'] if 'title' in match else "NO TITLE")
		yield export_list
		#print(json.dumps(export_list, indent=2))
									
									
							
							
			
			 
			
	
	def _search_generator(self,search_term, facet,timespan):
		r=self.get(search_term,"search",facet)
		if r:
			for e in r:
				item=self.get(e,"item",facet)
				print (e)
				print (item)
				print ("----------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
			
			
			items=r[0].split('**+~+*')
			if len(items)==self.MAX_RESULTS:
				print("To much results for : ", search_term, "                                                                                                  \n")
				return
			for item in items:
				item_elem=item.split('*')
				d={'levelOfDescription':"File"}
				#print([x for x in item_elem if x!="+#+"])
				d['hierarchy']=[x for x in item_elem if x!="+#+"]
				if d['hierarchy'][0][0:2]=="ve":
					d['identifier']=d['hierarchy'][0][2:]+ " "+d['hierarchy'][1].split("_")[0]
					d['identifier']=d['identifier'][1:] if d['identifier'][0:1]=="_" else d['identifier']
					d['levelOfDescription']="File"
					d['fb_id']=d['hierarchy'][1][d['hierarchy'][1].rfind("_")+1:]

					d['legacyId']=self.ARCHIVE_ID+"_"+"ve_"+d['fb_id']
					d['parentId']=self.ARCHIVE_ID+"_" + d['hierarchy'][2]
				if d['hierarchy'][0][0:2]=="be":
					d['level0fDescription']="Fonds"
					d['fb_id']=d['hierarchy'][0][d['hierarchy'][0].rfind("_")+1:]
					d['legacyId']=self.ARCHIVE_ID+"_"+"b_"+d['fb_id']
					d['parentId']=self.ARCHIVE_ID+"_" + d['hierarchy'][2]
				top=self.get(search_term,"top",d['fb_id'],d['levelOfDescription'])
				d['top']=[x for x in top if x!=""]
				d['arrangement']=' >> '.join(d['top'])
				d['props']=self.get(search_term,"props",d['fb_id'],d['levelOfDescription'])
				d['search_term']=search_term
				print(d)
				yield d.copy()
			print(len(items), " raw results found for ", search_term,"                                                                                                  \n")
		else:
			sys.stdout.write("\033[F")
			print("No results for : ", search_term, "                                                                                                                     ")


		
	
	def get(self, search_term, method='search', facet="", level_of_description=""):

		if method=='top':
			url="https://www."+self.ARCHIVE_URL+"."+self.BASE_URL + "top.php?ar_id="+str(self.ARCHIVE_ID)+"&kind="+self.CONVERTED_LEVEL[level_of_description]+"&id="+str(fb_id)+"&source=rech"
			if url in self.RESULTS_DICT:
				return self.RESULTS_DICT[url]
			the_page= urllib.request.urlopen(url, timeout=30).read().decode("utf-8")
			if the_page:
				the_page=the_page.replace("<b></b>","<b>Archiv</b>")
				tree=html.fromstring(the_page)
				e=tree.xpath('//span[@class="p"]/text()')
				tmp= [x.strip() for x in e]
				e=tree.xpath('//b/text()')
				if e[0]!="Archiv":
					tmp.insert(0,self.ARCHIVE_URL)
				self.RESULTS_DICT[url]=tmp
				return tmp
			else:
				self.RESULTS_DICT[url]=[]
				return []
		if method=='props':
			
			url="https://www."+self.ARCHIVE_URL+"."+self.BASE_URL + "props.php?ar_id="+str(self.ARCHIVE_ID)+"&kind="+self.CONVERTED_LEVEL[level_of_description]+"&id="+str(fb_id)+"&source=rech"
			the_page= urllib.request.urlopen(url, timeout=30).read().decode("utf-8")
			if the_page:
				d={}
				d['findingAids']="https://www."+self.ARCHIVE_URL+"."+self.BASE_URL + "main.php?ar_id="+str(self.ARCHIVE_ID)+"#"+the_page.split('*')[0]
				
				the_page=so.replaceChars(the_page,['<br />']," ")
				tree=html.fromstring(the_page)
				if level_of_description=="File":
					k=tree.xpath('//td[@class="ve_fn"]/text()')
					v=tree.xpath('//span[@class="ve_fi"]/text()')
					if k:
						for i in range(0,len(k)):
							if len(v)>i:
								d[k[i][0:len(k[i])-1]]=v[i]
						self.RESULTS_DICT[url]=d

						return d
					else:
						self.RESULTS_DICT[url]={}
						return {}
				if level_of_description=="Fonds":
					t=tree.xpath('//span/text()')
					tmp=  " ".join(t)
					self.RESULTS_DICT[url]=tmp
					return tmp
			else:
				self.RESULTS_DICT[url]={}
				return {}
			
				
		if method=="item":
			data={}
			headers={}
			url=self.BASE_URL+"item/" + search_term		
			the_page= urllib.request.urlopen(url, timeout=60).read().decode("utf-8")		
			if the_page:
				d={'legacyId':search_term}
				the_page=so.replaceChars(the_page,"\n","") 	
				tree=html.fromstring(the_page)	
				t=tree.xpath('//div[@class="col-sm-6 item-description"]')
				if len(t)>0:
					tree=t[0]
				else:
					t=tree.xpath('//div[@class="col-sm-12 item-description"]')
					if len(t)>0:
						tree=t[0]
					else:
						return d.copy()
				
				
				d['title']=tree.xpath("//div/h1/text()")[0].strip()
				d['findingAids']=tree.find('.//div[@class="row item-detail"]//a').get("href")

				field_elem=tree.findall('.//div[@class="row"]')
				
				for e in field_elem:
					if len(e.findall(".//a"))>0:
						#print(html.tostring(tree))
						link=e.find(".//a").get("href")
						text=htmllib.unescape(e.find(".//a").text_content().strip())
						# print(text,"|",link,html.tostring(e))
						if not link is None:
							d[e.find(".//strong").text.strip()[:-1]]=text + " ("+ link + ")"
						else:
							d[e.find(".//strong").text.strip()[:-1]]=htmllib.unescape(e.findall(".//div")[1].text.strip())
					elif e.findall(".//div")[1].text is None:
						d[e.find(".//strong").text.strip()[:-1]]=htmllib.unescape(e.findall(".//span")[1].text)
					else:
						d[e.find(".//strong").text.strip()[:-1]]=htmllib.unescape(e.findall(".//div")[1].text.strip())
				return d.copy()


		if method=='search':
			data={}
			headers = {}
			url=self.BASE_URL+"searchresults?"
			data['query']=search_term
			data['isThumbnailFiltered']=False
			data['offset']=0
			data['rows']=1000
			if facet!="":
				data['facetValues[]']=facet
			data = urllib.parse.urlencode(data)
			#data = data.encode('ascii') # data should be bytes
			print (url+data)
			the_page= urllib.request.urlopen(url+data, timeout=60).read().decode("utf-8")
			if the_page:
				tree=html.fromstring(the_page)
				print(len(the_page))
				r=tree.xpath('//div[@class="col-sm-12"]//a/@href')
				#r=tree.xpath('//div[@class="summary-main"]')
				#for e in r:
					#print (e.xpath('//a/@href'),e.xpath('//div[@subtitle]/text()'))
				#f=input()
				if r:
					r_new=[]
					for e in r:
						ddbid=e[6:38]
						if not (ddbid in r_new) and ddbid==ddbid.upper():
							r_new.append(ddbid)
					print(r_new)	
					return r_new
				else:
					return []
			else:
				return []
			

		"""
		---- Top
		curl "https://www.uniarchiv-rostock.findbuch.net/php/top.php?ar_id=3739&kind=ve_e&id=332175&be_id=21&source=rech"
		---  DEtails
		curl "https://www.uniarchiv-rostock.findbuch.net/php/props.php?ar_id=3739&kind=ve_e&id=332175&be_id=21&source=rech&high="
		"""
