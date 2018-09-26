#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  data_manager.py
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
import torch.nn as nn
from torch.autograd import Variable
import torch
import random
import torch.nn.functional as F
import json
from nltk.stem.snowball import GermanStemmer
stemmer =GermanStemmer()
import sys
import numpy as np
import subprocess

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from atom.main import data_manager, g
from atom.helpers.helper import fileOps, stringOps, listOps
from atom.imports import eadxml
fo=fileOps()
so=stringOps()
lo=listOps()
from time import gmtime, strftime

class ai(object):
	
	TARGET_THEMATIC=1
	TARGET_NON_THEMATIC=0	
	FIELDS=["title","scopeAndContent","arrangement"]
	TRAIN_DATA=[]
	TEST_DATA=[]
	TRAIN_DATA_FILE="atom/data/train.json"
	TEST_DATA_FILE="atom/data/test.json"
	TRAIN_DATA_TEXT_FILE="atom/data/train.txt"
	TEST_DATA_TEXT_FILE="atom/data/test.txt"
	TRAIN_DATA_STR_FILE="atom/data/train_raw.txt"
	STOPPWORD_FILE="atom/data/stopp.txt"
	RATIO=0.7
	STOPWORTE=[]
	STOPWORTE_FILE="atom/data/stopworte.txt"
	TRAIN_FILE_LIST_FILE="atom/data/ai_train_files.txt"
	WORD2IX={}
	WORD2IX_FILE="atom/data/word2ix.json"
	WORD_STAT=[]
	WORD_STAT_FILE='atom/data/word_stats.txt'
	LEARNING_RATE=-0.005
	BOW_MODEL_FILE='atom/data/bow.pt'
	MODEL=object()
	BOW=[]
	EPOCHS=20
	
	dm=data_manager.DataManager()
	
	def __init__(self):
		self.TRAIN_DATA=[]
		self.TEST_DATA=[]
		
	
	def add2data_from_file_list(self,list_file=None):
		print("start")
		
		if list_file:
			file_list=fo.load_data(list_file,None,True)
		else:
			file_list=fo.load_data(self.TRAIN_FILE_LIST_FILE,None,True)
		#print(file_list)
		file_list=[x.split(",") for x in file_list]
		for fname,target in file_list:
			if fname[0:1]=="#":
				continue
			print("-----------------")
			print("file: ", fname, "  target:", target)
			self.add2data(fname,target,[0,1])
		print("Finish")
	
	def add2data_from_atom(self):
		sql="select concat(IFNULL(title, ''), IFNULL(arrangement, ''),IFNULL(scope_and_content, '')) as text from information_object_i18n ii join information_object i on ii.id=i.id where  i.level_of_description_id=461 and culture ='de';"
		l=self.dm.get_mysql(sql,False)
		print(len(l), " records found in AtoM database")
		new_l=[str(x[0]) for x in l]
		self.add2data(new_l,"1")
		
	
	def add2data(self,data,target,index_arr=[]):
		
		"""
		Adds content and target to a batch of data and add them to the data.
		Shuffels test_data and train_data once again
		"""
		if isinstance(data,str):
			if os.path.isfile(data) and data[-4:].lower()==".xml":
				l=self.addEADXML2data(data,target)
			else:
				l=self.prepare_data(data,index_arr)
		else:
			l=self.prepare_data(data,index_arr)
		print(len(l),"---")
		l=self.add_target(l,target)
		if len(l)>0:
			print(len(l), " records collected. Having target =", l[0][1] )
		else:
			print ("No records collected")
		l=self.clean_list_data(l,True,True)
		print(len(l), "Records cleaned.")
		#print(l)
		#return
		#if self.TRAIN_DATA is None:
		#	self.TRAIN_DATA	= fo.load_data_once(self.TRAIN_DATA,self.TRAIN_DATA_FILE)
		#print("Train data loaded. ",self.stat(self.TRAIN_DATA))
		#if self.TEST_DATA is None:
		#	self.TEST_DATA=fo.load_data_once(self.TEST_DATA,self.TEST_DATA_FILE)
		#print("Test data loaded. ", self.stat(self.TEST_DATA))
		#self.TRAIN_DATA = self.TRAIN_DATA.copy() + l.copy()
		#print("Data has been merged", self.TRAIN_DATA[len(self.TRAIN_DATA)-1][1])
		#self.stat(self.TRAIN_DATA)
		#self.store_data(self.RATIO)
		self.add_data2_store(l,self.RATIO)
		print("Data has been (shuffled and) stored")

	def prepare_data(self,data, index_arr=[]):
		"""
		Gets a EAD-CSV file or a list (of strings, list or tuples), extends
		\n the target to each element and returns a list of tupels
		data - the list or filename
		target - the target value for the nn
		index - the index of the values to use if list element is list or tuple
		"""
		rlist=[]

		if isinstance(data,list):
			l=data
		elif isinstance(data,str):		
			if os.path.isfile(data):
				l=fo.load_data(data)
				#print(len(l)," data len file", type(l), index)
				if isinstance(l,list):
					if isinstance(l[0],dict):
						l=self.read_data_from_csv(l,self.FIELDS,{"levelOfDescription":"File"})
					elif isinstance(l[0],list) or isinstance(l[0],tuple) and len(index)>0:
						#new_l=[x[index] for x in l]
						new_l=[x[index_arr[0]]+" "+x[index_arr[1]] for x in l]
						l=new_l
					elif isinstance(l[0],str):
						l=l
				#print(len(l)," data len file", type(l), index)	
			else:
				l=data.split("\n")
		return l.copy()
	
	def add_target(self,l,target):
		print("target",target)
		rlist=[]
		print(type(l[0]))
		for e in l:
			if isinstance(e,str):
				rlist.append((e,target))
			elif isinstance(e,list) or isinstance(e,tuple):
				if len(e)>1:
					rlist.append((e[0],e[1]))
				elif len(e)==1:
					rlist.append((e[0],target))
		
		return rlist.copy()
	
	def clean_list_data(self,l, stemm=True, stopp=True):
		if stopp:
			stopw=fo.load_data(self.STOPPWORD_FILE,None,True)
		rlist=[]
		l=lo.dedup(l)
		i=0
		total=len(l)
		current=0
		for e in l:
			item=e[0]
			item=so.replaceChars(item,so.SATZZEICHEN,"")
			item=so.replaceChars(item,so.LQGQ,"")
			item=so.replaceChars(item,so.ANFUHRUNGSZEICHEN,"")
			item=so.replaceChars(item,so.STRICHE," ")
			item=so.replaceChars(item,['\n']," ")
			item=so.replaceChars(item,[']n'],' ')
			item=so.replaceChars(item,['\t',"b'",'¤',"‧"],' ')
			item=so.replaceChars(item,"'°%§[]*"," ")
			item=so.replaceChars(item,"0123456789","")
			item=so.replaceChars(item,['xc3xa4','xc3x84'],'a')
			item=so.replaceChars(item,['xc3xb6','xc3x96'],'o')
			item=so.replaceChars(item,['xc3xbc','xc3x9c'],'u')
			item=so.replaceChars(item,['xc3x9f'],'ss')
			
			item=so.dedupSpaces(item)
			if stemm:
				item=stemmer.stem(item)
			if stopp:
				item=" ".join([x for x in item.split(" ") if x not in stopw])
			
			if int(i/total*100)>current:
				current=int(i/total*100)
				sys.stdout.write("\033[F")
				print(current+1, "% ")
				
			i+=1
			if item!="" and item not in [str(x) for x in range(0,10)]:
				rlist.append((item,e[1]))	
		return rlist
			
	def clean_text(self,item,stemm=True, stopp=True):
		if stopp:
			stopw=fo.load_data(self.STOPPWORD_FILE,None,True)
		item=so.replaceChars(item,so.SATZZEICHEN,"")
		item=so.replaceChars(item,so.LQGQ,"")
		item=so.replaceChars(item,so.ANFUHRUNGSZEICHEN,"")
		item=so.replaceChars(item,so.STRICHE," ")
		item=so.replaceChars(item,['\n']," ")
		item=so.replaceChars(item,[']n'],' ')
		item=so.replaceChars(item,['\t',"b'",'¤',"‧"],' ')
		item=so.replaceChars(item,"'°%§[]*"," ")
		item=so.replaceChars(item,"0123456789","")
		item=so.replaceChars(item,['xc3xa4','xc3x84'],'a')
		item=so.replaceChars(item,['xc3xb6','xc3x96'],'o')
		item=so.replaceChars(item,['xc3xbc','xc3x9c'],'u')
		item=so.replaceChars(item,['xc3x9f'],'ss')
		item=so.dedupSpaces(item)
		if stemm:
			item=stemmer.stem(item)
		if stopp:
			item=" ".join([x for x in item.split(" ") if x not in stopw])
		return item
	
	def addEADXML2data(self,sourcefile,target):
		eadx=eadxml.eadxml(sourcefile,"")
		l=eadx.transform()
		fo.save_data(l,"/tmp/test.json")
		rlist=[]
		for e in l:
			if "levelOfDescription" in e:
				item=""
				if "title" in e:
					
					item=e["title"]+" "
				if "scopeAndContent" in e:
					item+=e["scopeAndContent"] +" "
				if "arrangement" in e:
					item+=e["arrangement"]
				if item != "":
					rlist.append(item)
		rlist=[(x,target) for x in rlist]
		return rlist
		

	def stat(self,l):
		if l:
			t0=[x for x in l if int(x[1])==0]
			t1=[x for x in l if int(x[1])==1]
			return("Len: ", len(l)," T 0 = ", len(t0), " T 1 = ", len(t1))
		else:
			return("list to count does not exist")

	def read_data_from_csv(self,sourcefile,fields, restrictions):
		"""
		Opens returns a list where elements are string concat of requiered fields
		"""
		
		rlist=[]
		

		if l:
			for e in l:
				item=""
				if restrictions:
					for k,v in enumerate(restrictions):
						if item[k]!=v:
							continue
				for k in e.keys():
					if k in fields:
						item += e[k]+" "
				rlist.append(item)
			return rlist
		else:
			print("Could see list of dicts")
			return []
				
						
		
	"""
	def open_train_data(self):
		self.TRAIN_DATA=fileOps.load_data_once(self.TRAIN_DATA,self.TRAIN_DATA_FILE)	
	
	def open_test_data(self):
		self.TEST_DATA=fileOps.load_data_once(self.TEST_DATA,self.TEST_DATA_FILE)
	"""
	
	def store_data_json(self, ratio):
		
		print(len(self.TRAIN_DATA), "Train len")
		new_data=[]
		new_train_data=[]
		new_test_data=[]
		if self.TEST_DATA:
			tt_data=self.TEST_DATA+self.TRAIN_DATA
		else:
			tt_data=self.TRAIN_DATA
		tt_data=lo.dedup(tt_data)
		count=len(tt_data)
		for i in range(0,len(tt_data)):
			item=random.choice(tt_data)
			if i>(count*ratio):
				new_test_data.append(item)
			else:
				new_train_data.append(item)
			tt_data.remove(item)
		
		#fo.save_data(new_test_data, self.TEST_DATA_FILE)
		#fo.save_data(new_train_data, self.TRAIN_DATA_FILE)
		
	def add_data2_store(self,data,ration):
		train_str=fo.load_data(self.TRAIN_DATA_STR_FILE,None,False)
		print(len(train_str)," Train String Len")
		add_str=""
		dup_c=0
		for e in data:
			add_str+=e[0]+"|"+e[1]+"\n"
			"""
			if train_str.find(e[0])<0 and add_str.find(e[0])<0:
			#if True:
				add_str+=e[0]+"|"+e[1]+"\n"
			else:
				dup_c+=1
			"""
		data=[]
		#print(dup_c, " duplicates removed")
		train_str=train_str+add_str
		fo.save_data(train_str,self.TRAIN_DATA_STR_FILE)
		if os.path.isfile(self.TRAIN_DATA_STR_FILE):
			subprocess.Popen(["cp",self.TRAIN_DATA_STR_FILE,self.TRAIN_DATA_STR_FILE + ".bak"  ] )
		#subprocess.Popen("perl -i.bak -ne 'print if ! $x{$_}++' "+ self.TRAIN_DATA_STR_FILE)
		return
		
	def index_words(self):
		i=0
		print(" ")
		with open(self.TRAIN_DATA_STR_FILE) as f:
			for line in f:
				i+=1
				sys.stdout.write("\033[F")
				print(i)
				items=line.split("|")
				text=items[0].split(" ")
				for k,word in enumerate(text):
					if not word.isnumeric():
						if word not in self.WORD2IX:
							self.WORD2IX[word] = len(self.WORD2IX)
							"""
							if len(items)>0:
								if len(items[1])>0:
									if items[1][0]=="0":
										self.WORD_STAT.append([word,len(self.WORD2IX),1,0])
									else:
										self.WORD_STAT.append([word,len(self.WORD2IX),0,1])
							"""
						"""
						else:
							e=next((x for x in self.WORD_STAT if x[0]== word),False)
							if e:
								if len(items)>0:
									if len(items[1])>0:
										if items[1][0]=="0":
											e[2]+=1
										else:
											e[3]+=1
						"""
						"""
						if k< len(text)-1:
							j=k +1
							n2gram=word+text[j]
							if n2gram not in self.WORD2IX:
								#print(n2gram)
								self.WORD2IX[n2gram] = len(self.WORD2IX)
						"""
		#print(self.WORD2IX, len(self.WORD2IX))
		fo.save_data(self.WORD2IX,self.WORD2IX_FILE)
		"""
		text=""
		for e in self.WORD_STAT:
			text+="|".join([str(x) for x in e])+"\n"
		fo.save_data(text,self.WORD_STAT_FILE)
		"""
		return len(self.WORD2IX)
	
	
	def word_stats(self):
		frequencies=[]
		if len(self.WORD_STAT)==0:
			l=fo.load_data(self.WORD_STAT_FILE)
			
	
	def create_sets(self,ratio):
		#self.TRAIN_DATA=[]
		#self.TEST_DATA=[]
		train_data=""
		test_data=""
		f=open(self.TRAIN_DATA_STR_FILE ,"r")
		lines=f.readlines()
		count=len(lines)
		err=0
		print("")
		for i in range(0,len(lines)):
			sys.stdout.write("\033[F")
			print(i)
			line=random.choice(lines)
			item=line.strip("\n").split("|")
			mod_text=lo.remove_duplicate_followers(item[0])
			if len(item)>1:
				if i>(count*ratio):
					test_data+=mod_text+"|"+item[1]+"\n"
				else:
					train_data+=mod_text+"|"+item[1]+"\n"

			lines.remove(line)
		
		fo.save_data(test_data, self.TEST_DATA_TEXT_FILE)
		test_data=""
		fo.save_data(train_data, self.TRAIN_DATA_TEXT_FILE)
				
	
	

		
		




	def catFromOutput(self,out):
		_,i = out.data.topk(1)
		return i

	#def charToIndex(self,char):
		#return letters.find()
		
	"""
	def wordToTensor(self,word):
		ret=torch.zeros(1,len(self.WORD2IX))  #ret.size= (1,len(letters))
		ret=[0][self.WORD2IX[word]]=1
		return ret
	"""
	""""
	def textToTensor(self,text):
		ret=torch.zeros(len(text),1,len(self.WORD2IX))
		for i,word in enumerate(text):
			ret[i][0][self.WORD2IX[word]]=1
		return ret
	"""	

		
	def getTrainData(self,test=False):
		if test:
			file_name=self.TEST_DATA_TEXT_FILE
		else:
			file_name=self.TRAIN_DATA_TEXT_FILE	
		with open(file_name,"r") as f:
			for line in f:
				#print(line)
				text=line[:-2]
				cat=line[-2:-1]
				#print(cat)
				#print(text)
				if cat in ("0","1"):
					cat=int(cat)
					#text_tensor=Variable(self.textToTensor(text))
					#cat_tensor=Variable(torch.LongTensor([cat]))
					
					#yield cat,text,cat_tensor,text_tensor
					yield text.split(),cat
		
	def make_bow_vector(self,sentence):
		# create a vector of zeros of vocab size = len(word_to_idx)
		vec = torch.zeros(len(self.WORD2IX))
		for i,word in enumerate(sentence):
			if word not in self.WORD2IX:
				#raise ValueError('houston we have a problem')
				pass
			else:
				vec[self.WORD2IX[word]]+=1
			"""
			if i+1< len(sentence):
				if word+sentence[i] in self.WORD2IX:
					vec[self.WORD2IX[word+" "+sentence[i]]]+=1
			"""
		return vec.view(1, -1)

	def make_target(self,value):
		return torch.LongTensor([value])
	
	def training(self):
		if len(self.WORD2IX)==0:
			self.WORD2IX=fo.load_data(self.WORD2IX_FILE)
		if isinstance(self.BOW,list):	
			self.BOW = BOWClassifier(2, len(self.WORD2IX))
		if os.path.isfile(self.BOW_MODEL_FILE):
			self.BOW=torch.load(self.BOW_MODEL_FILE	)
			self.BOW.eval()
		#bow_vector = torch.autograd.Variable(self.make_bow_vector(sample_data, self.WORD2IX))
		#logprobs = bow(bow_vector)
		# define a loss function and an optimizer
		loss_function = nn.NLLLoss()
		#opt = torch.optim.SGD(bow.parameters(), lr = 0.05)
		opt = torch.optim.SGD(self.BOW.parameters(), lr = 0.05)

		# the training loop
		print(" T:" ,strftime("%Y-%m-%d %H:%M:%S", gmtime()) )
		for epoch in range(self.EPOCHS):
			for instance, label in self.getTrainData():
				# get the training data
				self.BOW.zero_grad()
				bow_vec = Variable(self.make_bow_vector(instance))
				label = Variable(self.make_target(label))
				probs = self.BOW(bow_vec) # forward pass
				loss = loss_function(probs, label)
				loss.backward()
				#sys.stdout.write("\033[F")
				#print('CURRENT LOSS: {}'.format(loss.data))
				opt.step()
			#sys.stdout.write("\033[F")
			print('Epoch:', epoch,'  CURRENT LOSS: {}'.format(loss.data), " T:" ,strftime("%Y-%m-%d %H:%M:%S", gmtime()) )
			torch.save(self.BOW,self.BOW_MODEL_FILE)	
	
	def testing(self):	
		if len(self.WORD2IX)==0:
			self.WORD2IX=fo.load_data(self.WORD2IX_FILE)
		if isinstance(self.BOW,list):	
			self.BOW = BOWClassifier(2, len(self.WORD2IX))
			self.BOW=torch.load(self.BOW_MODEL_FILE)
			self.BOW.eval()
		i=0
		c=0
		print("")
		for instance, label in self.getTrainData(True):
			i+=1
			bow_vec = Variable(self.make_bow_vector(instance))
			logprobs = self.BOW(bow_vec)
			#print(logprobs)
			
			pred = np.argmax(logprobs.data.numpy())
			if pred==int(label):
				c+=1
			else:
				#print(instance[0:60])
				pass
		sys.stdout.write("\033[F")
		print("i= ", i , " Correct: " , 100* c/i )
		
	
	def predict(self,text):
		if len(self.WORD2IX)==0:
			self.WORD2IX=fo.load_data(self.WORD2IX_FILE)
		if isinstance(self.BOW,list):	
			self.BOW = BOWClassifier(2, len(self.WORD2IX))
			self.BOW=torch.load('atom/data/bow.pt')
			self.BOW.eval()
		text=self.clean_text(text)
		bow_vec = Variable(self.make_bow_vector(text.split()))
		logprobs = self.BOW(bow_vec)
		pred = np.argmax(logprobs.data.numpy())
		return pred
"""
	def predict(self,text):
		# function for evaluating user input sentence
		# print("input=",sentence)
		output = evaluate(Variable(self.make_bow_vector(text)))
		top_v,top_i = output.data.topk(1)
		output_index = top_i[0][0]
		return all_categories[output_index],top_v[0][0],output_index

	def evaluate(self,line_tensor,ann):
		# output evaluating function
		output = ann(line_tensor)
		return output
		"""
"""
	def train(self,cat_tensor,text_tensor):	
		criterion =nn.NLLLoss()
		#hidden=self.MODEL.initHidden()
		self.MODEL.zero_grad()
		for i in range(text_tensor.size()[0]):
			output=self.MODEL(text_tensor[i])
		loss= criterion(output,cat_tensor)
		loss.backward()
		for i in self.MODEL.parameters():
			i.data.add_(self.LEARNING_RATE,i.grad.data)#

		return output, loss
		
	def training(self):
		if len(self.WORD2IX)==0:
			self.WORD2IX=fo.load_data(self.WORD2IX_FILE)
		self.MODEL = Netz(len(self.WORD2IX),128,2)
		sum=0
		avg=[]
		i=0
		for cat,name,cat_tensor,text_tensor in self.getTrainData():
			i+=1
			output,loss=self.train(cat_tensor,text_tensor)
			sum=sum+loss.data[0]
			if i%100 ==0:
				avg.append(sum/100)
				sum=0
				print(i/100, "% done.")
		self.plot(avg)		
	
	def plot(self,avg):
		plt.figure()
		plt.plot(avg)
		plt.show()


	def textToTensor(self,text):
		# input tensor initialized with zeros
		tensor=torch.zeros(1,len(self.WORD2IX))
		for word in text.split(" "):
			if word not in self.WORD2IX:
				# to deal with words not in dataset in evaluation stage
				continue
			tensor[0][self.WORD2IX[word]] = 1  # making found word's position 1
		return tensor
"""
"""
class Netz(nn.Module):
	def __init__(self,input,hiddens,output):
		super(Netz,self).__init__()
		self.hiddens=hiddens
		self.hid = nn.Linear(input + hiddens, hiddens)
		self.out = nn.Linear(input + hiddens, output)
		self.logsoftmax = nn.LogSoftmax(dim=1)
		
	def forward(self,x, hidden):

		x= torch.cat((x,hidden),1)
		new_hidden = self.hid(x)
		output = self.logsoftmax(self.out(x))
		return output, new_hidden
		
	def initHidden(self):
		return Variable(torch.zeros(1,self.hiddens))
	
"""




class Netz(nn.Module):

	def __init__(self,input_size,hidden_size,output_size):
		super(Netz,self).__init__()
		self.i2h = nn.Linear(input_size,hidden_size)
		self.h2o = nn.Linear(hidden_size,output_size)
		self.softmax = nn.LogSoftmax()

	def forward(self, input):
		# forward pass of the network
		hidden = self.i2h(input)   # input to hidden layer
		output = self.h2o(hidden)  # hidden to output layer
		output = self.softmax(output)   # softmax layer
		return output

##################################



class BOWClassifier(nn.Module):
    def __init__(self, num_labels, vocab_size):
        super(BOWClassifier, self).__init__()
        self.lin = nn.Linear(vocab_size, num_labels)
    def forward(self, x):
        return F.softmax(self.lin(x))
