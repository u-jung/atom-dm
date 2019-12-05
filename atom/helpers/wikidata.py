#!/usr/bin/python3
# -*- coding: utf-8 -*-
#

import requests
import json
import urllib.parse
import urllib.request
import csv
import sys
import os	
import datetime
import re
from atom.main import data_manager
from atom.helpers import helper
so=helper.stringOps()
fo=helper.fileOps

#https://www.wikidata.org/w/api.php?action=query&list=search&format=json&srsearch=Meyer&srlimit=500&sroffset=500


class wikidata(object):
	wd_terms=[]
	properties=[]
	properties_file="atom/data/wd_prop.json"
	#wd_terms_file="atom/data/wd_terms.json"
	#wfile="statements.txt"
	#origin_file="origin.csv"
	statements=[]
	origin=[]
	result=""
	#result_file="results.txt"
	API_ENDPOINT = "https://www.wikidata.org/w/api.php"
	SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
	P31=[]
	#p31_file="p31.json"
	temp_terms={}
	last_item=""
	
	def __init__(self):
		#self.read_wd_terms()
		with open(self.properties_file, 'r') as file:
			self.properties = json.load(file)
		file.close()		
		return
		
	def __del__(self):
		#self.store_wd_terms()
		#print(self.result)
		pass

		
	def read_wd_terms(self):
		#populates the id_list from file
		#print(os.path.getsize(self.wd_terms_file))
		if os.path.getsize(self.wd_terms_file) > 1:
			with open(self.wd_terms_file, 'r') as file:
				self.wd_terms = json.load(file)
			file.close()	
		#print (json.dumps(self.wd_terms, indent=4, sort_keys=True))	
		with open(self.p31_file, 'r') as file:
			self.P31 = json.load(file)
		file.close()
		self.clean_wd_terms()
		
	def store_wd_terms(self):
		#stores the id_list to file
		with open(self.wd_terms_file, 'w', encoding='UTF-8') as file:
			file.write(json.dumps(self.wd_terms, sort_keys=True, indent=4, ensure_ascii=False))
		file.close()
		print ('stored')
	
	def clean_wd_terms(self):
		new=[i for n, i in enumerate(self.wd_terms) if i not in self.wd_terms[n + 1:]]
		self.wd_terms.clear()
		for e in new:
			if len(e['id'])<15:
				e['id']="http://www.wikidata.org/entity/"+e['id']
			self.wd_terms.append(e.copy())
		
		
	def proc_csv(self,filename):
		t={}
		line=""
		"""
		Transforms an input csv into an csv which is conform to the Quick Statements2 format
		"""
		self.get_origin(filename)
		print("origin")
		print(self.origin)
		#print (json.dumps(self.origin[0], indent=4, sort_keys=True))
		props=[] # 0=p, 1=pLabel, 2=type, 3=parent, 4=origin.label, 5 =datatype
		if len(self.origin)==0:
			return ""
		for k in self.origin[0].keys():
			ar=k.split(".")
			props_item=[]
			props_item=self.ask_property(ar[len(ar)-1])
			if len(ar)==2:
				props_item[3]=self.ask_property(ar[0])[0]
				if props_item[2]=="prop":
					props_item[2]="qualifier"
			props_item[4]=k
			props_item.append(self.get_property_label(props_item[0],True))
			if props_item[1]=='item':
				props_item[5]="WikibaseItem"
			props.append(props_item)
		#print (json.dumps(props, indent=4, sort_keys=True))
				
		for e in self.origin:
			statements_line={}
			statements_line['i']={}
			statements_line['p']={}
			statements_line['q']={}
			statements_line['s']={}
			last_item=""
			#print(e)
			for key, value in e.items():
				last_item=value
				#print("last - " + self.last_item)
				if value=='':
					continue
				p_ar=next((x for x in props if x[4]==key),None)
					
				# Could it be either a Label or a WDID?
				if p_ar[5]=="WikibaseItem":
					if not self.is_wdid(value):   #The case of beeing a label
						if p_ar[1]=="item": 
							if self.last_item==value:   #Could it be the same label as in the line before?
								value="LAST"
								#t=self.ask_instance(line, e[key])
								#self.result+=t["line"]
								#if self.is_wdid(t['wdid']):
									#value=t['wdid']
							else:

								self.last_item=""
								value=self.check_duplicates(value)
						else:
							value=self.check_duplicates(value)
						
						if value=="":
							self.last_item=last_item
							line='CREATE\nLAST\tLde\t"'+e[key] +'"\n'
							value="LAST"
							
							if 'Dde' in e:
								dde=e['Dde']
								instance=e['P31']
							else:
								dde=""
								instance=""
							t=self.ask_instance(line, e[key],dde, instance)
							if self.is_wdid(t["wdid"]):
								value=t["wdid"]
							else:
								self.result+=t["line"]	


											
				if p_ar[5]=="String":
					value='"'+value+'"'
				
				if p_ar[5]=="Time":
					value= self.get_date(value)		
				if p_ar[1]=="item":
					statements_line["i"][p_ar[0]]=value
					
				elif p_ar[2]=="prop":
					statements_line["p"][p_ar[0]]=value
				elif p_ar[2]=="qualifier":	
					statements_line["q"][p_ar[0]]=(value,p_ar[3])	
				elif p_ar[2]=="source":	
					statements_line["s"][p_ar[0]]=(value,p_ar[3])
			print(statements_line)		
			item=statements_line['i']['item']
			p=statements_line['p'].copy()
			q=statements_line['q'].copy()
			s=statements_line['s'].copy()
			for key,value in statements_line['p'].items():

				print(key,value)
				line= item + "\t" + key +"\t" + p[key]
				for k,v in q.items():
					if q[k][1]==key:
						line +="\t" + k +"\t" + v[0]
				for k,v in s.items():
					line +="\t" + k +"\t" + v[0]				
				
				#print(line)	
				self.result+=line+'\n'
				print(self.result)
				statements_line.clear()
			
			self.store_results()	
			
				
			#self.statements.append(statements_line)
		#print (json.dumps(self.statements, indent=4, sort_keys=True))	

	def store_results(self):
		os.remove(self.result_file)
		f= open(self.result_file,"w") 
		f.write(self.result)
		f.close()
		
	def ask_instance(self, line, ekey, dde, instance):
		if dde !="":
			line+='LAST\tDde\t"'+dde +'"\n'
			line+='LAST\tP31\t'+instance +'\n'
			wdid="LAST"
			return {"wdid":wdid,"line":line}
		print("\n\n/////////////////////////////////////////////////////////")
		for i in range(0,len(self.P31)):
			print(str(i)+":  "+self.P31[i][1])
		a=input("-----------------\n"+ekey+ " is a instance of what? (or wdid if found)  ")
		#print (a)
		#print (int(a) in range(0,len(self.P31)))
		if self.is_wdid(a):   # an Wdid was given
			wdid=a
		elif a.isdigit():
			if int(a) in range(0,len(self.P31)):
				a =int(a)
				line+='LAST\tDde\t"'+self.P31[a][1] +'"\n'
				line+='LAST\tP31\t'+self.P31[a][0] +'\n'
				line+='LAST\tDen\t"'+self.P31[a][2] +'"\n'
				wdid="LAST"
			else:
				wdid=""
		else:
	
			wdid=""
		return {"wdid":wdid,"line":line}
		
		
	def is_wdid(self,value):
		if value[0:1]=="Q" and value[1:].isdigit():
			return True
		else:
			return False
	
	def get_date(self,value):
		if value=="":
			return ""
		if len(value)==4:
			return "+"+value +"-00-00T00:00:00Z/9"
		if len(value)==7:
			return "+"+value[3:]+"-"+value[0:2] +"-00T00:00:00Z/10"
		if len(value)==10:
			return "+"+value[6:]+"-"+value[3:5] + "-"+value[0:2]+"T00:00:00Z/11"
		return ""
		
	
		
	def ask_property(self,test,uri = False):
		#option for test: Pxxx, Lxx, Axx, Dxxx, item, Sxxx, value, {other}
		#print(test)
		r=[]
		if test[1:].isdigit():
			if test[0:1].lower()=="p":
				return [test[0:1].upper()+test[1:].lower(),self.get_property_label(test),"prop","",""]
			elif test[0:1].lower()=="s":
				return [test[0:1].upper()+test[1:].lower(),self.get_property_label("P" +test[1:]),"source","",""]
		elif (test[0:1].lower() in ("l","a","d")) and (len(test)==3):
			return [test[0:1].upper()+test[1:].lower(),"","label","",""]
		elif test.lower()=="item":
			return ["item","item","item","",""]
		elif test.lower()=="value":	
			return ["value","value","source","",""]
		else:
			p=self.get_property_id(test, uri)
			if p=="":
				a=" "
				while not ((a[0:1].lower()=="p" and a[1:].isdigit()) or a== ""):
					a=input(test + " is not known to be a property. Please specify the property ID: ")
					pLabel=self.get_property_label(a)
					if pLabel=="":
						a=" "
					else:
						print ("Correct label is: " + pLabel)
						return [a, test,"P",""]
			else:
				return[p,test,"prop","",""]
		
	def lookup(self,liste):
		for e in liste:
			self.check_duplicates(e)
		print (json.dumps(self.wd_terms, indent=4, sort_keys=True))
		print (self.wd_terms)

	def search_query(self,search_term):
		#query=API_ENDPOINT+'/w/api.php?action=query&list=search&format=json&callback=?&srsearch='+urllib.parse.urlencode(search_term)+'&srlimit=100&sroffset=0';
		objects=[]
		
		params = {
			'action': 'wbsearchentities',
			'format': 'json',
			'language':'de',
			'uselang':'de',
			'limit':10,
			'origin':'*',
			'search': search_term
		}
		#https://www.wikidata.org/w/api.php?format=json&action=wbsearchentities&search=bongor&limit=50&language=de&uselang=de&origin=*
		url=self.API_ENDPOINT +"?"+ urllib.parse.urlencode(params)
		headers={}
		headers['accept']='application/sparql-results+json'
		
		#print(url)
		#stop=input("stop")
		req = urllib.request.Request(url, None, headers)
		with urllib.request.urlopen(req) as response:
			the_page = response.read().decode("utf-8")
		#print(type(the_page))
		#rjson =json.loads(the_page)			
		#print(rjson)
		l= json.loads(the_page)
		items=[]
		if not 'search' in l:
			return []
		for e in l['search']:
			items.append([e['title'], e['concepturi'], e['label'], e['description'] if 'description' in e else ""])
		
		return items 
		
		
		r=requests.get(url)

		 # because of ???
		rjson=json.loads(r.text[5:len(r.text)-1])
		print(r)
		stop=input("stop")		
		
		#print (json.dumps(rjson, indent=4, sort_keys=True))
		if 'query' in rjson:
			for e in rjson["query"]["search"]:
				objects.append(e['title'])
			return objects
		else:
			return []


	

	def get_from_WDQ(self,query):
		params={
				'format':"json",
				'query':query
				}
		url=self.SPARQL_ENDPOINT +"?query="+ urllib.parse.urlencode(params)
		headers={}
		headers['accept']='application/sparql-results+json'
		
		r = urllib.request.Request(url, None, headers)
		with urllib.request.urlopen(r) as response:
			the_page = response.read().decode("utf-8")
			
			return json.loads(the_page)
		
	def get_from_wd(self, url_values,accept='json'):
		data={}
		headers={}
		headers['accept']='application/sparql-results+json'
		url=self.SPARQL_ENDPOINT  + '?' + urllib.parse.urlencode({"query": url_values})
		#print(url)
		req = urllib.request.Request(url, None, headers)
		with urllib.request.urlopen(req) as response:
			the_page = response.read().decode("utf-8")
			return the_page
	
	
	
	def check_duplicates(self,search_term):
		wdid=self.get_local_wdid(search_term)
		if wdid !="":
			return wdid
		objects=self.search_query(search_term)
		query_values=""

		for e in objects:
			query_values+="(wd:"+ e +")"
		
		query='select ?item ?itemLabel ?itemAltLabel ?itemDescription ?yob ?yod \
			where {\
			VALUES (?item) {'+ query_values+'} \
			OPTIONAL { ?item wdt:P569 ?yob. } \
			OPTIONAL { ?item wdt:P570 ?yod. } \
			SERVICE wikibase:label {bd:serviceParam wikibase:language "de,en,fr" . }} '
		
		#data=self.get_from_WDQ(query)
		data=json.loads(self.get_from_wd(query))
		print("Duplicates for :" + search_term)
		print("-------------------------------------")
		i=0
		uStr=""
		for e in data['results']['bindings']:
			
			uStr=str(i)+":   " 
			
			for k in e.keys():
				uStr+=e[k]['value'] + " | "
			
			print(uStr)
			i+=1
		laenge= len(data['results']['bindings'])
		if laenge>0:
			a=input("Choice : ("+ search_term+")")

			if a[0:1].upper()=="Q":
				self.wd_terms.append({'id':'http://www.wikidata.org/entity/'+a,'label':self.get_label_from_wd(a,data['results']['bindings'],search_term)})
				self.temp_terms[search_term]=a
				return a
			elif a=="":
				return ""
			elif int(a) in range(0,len(data['results']['bindings'])):
				
				d=data['results']['bindings'][int(a)]
				self.temp_terms[search_term]=self.get(d,'item','value')[self.get(d,'item','value').rfind("/")+1:] 
				self.wd_terms.append({'id':self.get(d,'item','value'),'label':self.get(d,'itemLabel','value'),'description':self.get(d,'itemDescription','value'),'alt':self.get(d,'itemAltLabel','value')})
				return data['results']['bindings'][int(a)]['item']['value'][data['results']['bindings'][int(a)]['item']['value'].rfind("/")+1:]
				
			else:
				return ""
		else:
			print("Kein Ergebnis")
			return ""
	
	def get(self,d,k,v):
		if k in d:
			return d[k][v]
		else:
			return ""

		
	def get_local_wdid(self,search_term):
		"""
		Checks if a wdid is locally available
		"""
		#print(self.temp_terms)
		print (search_term)
		if search_term in self.temp_terms:
			return self.temp_terms[search_term]
			
		for e in self.wd_terms:
			if 'label' in e:
				if e['label'].lower()==search_term.lower():
					return e['id'][e['id'].rfind("/")+1:]
			if 'alt' in e:		
					ar=e['alt'].split(",")
					for ee in ar:
						if ee.lower() == search_term.lower():
							return e['id'][e['id'].rfind("/")+1:]
		return ""

	def get_local_label(self,wdid):

		element=next((x for x in self.wd_terms if x['item']==wdid),None)
		
		if element:

				return element['itemLabel']
		else:
			return ""
			
	def get_label_from_wd(self,wdid, data, search_term):
		wdiduri="http://www.wikidata.org/entity/"+wdid.upper()
		for e in data:
			if e['item']['value']==wdiduri:
				return e['itemLabel']['value']
		query='select ?item ?itemLabel  \
			where {bind (wd:'+wdid+' as ?item) .\
			SERVICE wikibase:label {bd:serviceParam wikibase:language "de,en,fr" . }} '
		
		#data=self.get_from_WDQ(query)
		data=json.loads(self.get_from_wd(query))
		if len(data['results']['bindings'])>0:
			return data['results']['bindings'][0]['itemLabel']['value']
		else:
			return search_term
		

	def get_property_id(self,pLabel,get_uri=False):
		element=next((x for x in self.properties if x['pLabel'].lower()==pLabel.lower()),None)
		if element:
			if get_uri:
				return element['p']
			else:
				return element['p'][element['p'].rfind("/")+1:]
		
		else:
			return ""
	def get_property_label(self,p, datatype=False):
		print(p)
		if p[0:1].upper()=="S":
			p="P"+p[1:]
		print(p)
		p_uri="http://www.wikidata.org/entity/"+p.upper()
		element=next((x for x in self.properties if x['p']==p_uri),None)
		if element:
			if datatype:
				return element["pt"][element["pt"].rfind("#")+1:]
			else:
				return element['pLabel']
		else:
			return ""
	
	def store_csv(self,wfile):
		with open(wfile,'w') as wf:
			fieldnames = self.statements[0].keys()			
			for e in self.statements:
					for ee in e.keys():
						if not ee in fieldnames:
							fieldnames.extend(ee)
					

			writer = csv.DictWriter(wf, fieldnames=fieldnames,extrasaction='ignore', delimiter='\t',
                            quotechar='"', quoting=csv.QUOTE_NONE)
			writer.writeheader()
			for e in l:
				writer.writerow(e)
		wf.close()
		
	def get_origin(self,filename):
		l=[]
		if filename=="":
			filename=self.origin_file
		self.origin=[]
		with open(filename) as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				row['item']=row['item'].strip()
				if row['item']!="":
					l.append(row)
		
		self.origin = sorted(l, key=lambda k: k['item']) 
