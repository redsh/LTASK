'''
LTASK
Minimal distributed task queue, written in Python.
Workers get tasks from a server using HTTP requests.
A task is identified by a set of parameters.
The server acts only as task parameters provider.

https://github.com/redsh/LTASK/tree/master/ltask

Copyright (C) 2013 Francesco "redsh" Rossi
License: MIT
'''

import os,sys
import itertools,copy
#parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#sys.path.insert(0,parentdir) 
#from push import *
#import jafar

			
class zerodb():
	pass
	

class scanstep():
	def __init__(self, what, vals):
		self.what = what
		self.values = vals
		
class pset(): #parameter set
	def __init__(self, what=None, vals=[]):
		self.config = [[(what,v)] for v in vals]
		
	def __or__(self,b):
		for i,a in enumerate(self.config):
			b.config[i] += self.config[i]
		return b

	def __mul__(self,B):
		ret = pset()
		for a in self.config:
			for b in B.config:
				a1 = copy.deepcopy(a)
				b1 = copy.deepcopy(b)
				
				for b11 in b1:
					a1.append(b11)	
				ret.config.append(a1)
					
		return ret
	
	def __add__(self,b):
		ret = copy.deepcopy(self)
		for c in b.config:
			ret.config.append(copy.deepcopy(c))
		return ret

def ls(ps):
	ls = []
	for p in ps.config:
		l = dict()
		for pp in p:
			l[pp[0]] = pp[1]
			x = pp[1]
			if type(pp[1]) == type([]):
				x = ''
				for a in pp[1]:
					x = x+'%s-'%a
			#l['name'] = '%s_%s%s'%(l['name'],pp[0],x)
		#print l['name']
		ls += [l]
	return ls
		
def parameter_scan_3(ps,funz):
	for p in ps.config:
		l = dict()
		for pp in p:
			l[pp[0]] = pp[1]
			x = pp[1]
			if type(pp[1]) == type([]):
				x = ''
				for a in pp[1]:
					x = x+'%s-'%a
			#l['name'] = '%s_%s%s'%(l['name'],pp[0],x)
		#print l['name']
		funz(l)

def parameter_scan_2(funz, l0, ps):
	for p in ps.config:
		l = copy.deepcopy(l0)
		for pp in p:
			l[pp[0]] = pp[1]
			x = pp[1]
			if type(pp[1]) == type([]):
				x = ''
				for a in pp[1]:
					x = x+'%s-'%a
			l['name'] = '%s_%s%s'%(l['name'],pp[0],x)
		#print l['name']
		funz(l)

def parameter_scan(funz, l0, scans):
	W = [s.what for s in scans]
	V = [s.values for s in scans]
	S = list(itertools.product(*[s.values for s in scans]))
	
	for s in S:
		l = copy.deepcopy(l0)	
		
		i = 0
		for w in W:
			l[w] = s[i]
			x = s[i]
			if type(s[i]) == type([]):
				x = ''
				for a in s[i]:
					x = x+'%s-'%a
			l['name'] = '%s_%s%s'%(l['name'],w,x)
			i+=1
			
		print l['name']
		#funz(l)
		#bfadsfs
		#print l
		
		
	
def launch_parameter_scan(tnsa,C,L,scans):
	args = pre_main()
	sim = 0
	c   = 0
	
	if len(args ) > 0:
		simname = args[0]
		sim = 0
		for l in L:
			if l['name'] == simname:
				break
			sim = sim+1
		
		simname = args[1]
		c = 0
		for l in C:
			if l['name'] == simname:
				break
			c = c+1
		
	for a in args[2:]:
		parameter_scan_2(lambda x: tnsa([x],C[c]),
				L[sim],scans[a])
	
	run_and_watch()

	exit(0)
	