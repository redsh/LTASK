'''
Tasks-In-A-Bottle

Minimal distributed task queue, written in Python.
Workers get tasks from a server using HTTP requests.
A task is identified by a set of parameters.
The server only provides task parameters, sending them as messages in a bottle.

https://github.com/redsh/LTASK/tree/master/ltask

Copyright (C) 2013 Francesco "redsh" Rossi
License: MIT
'''

import os,sys,hashlib

from parameter_scan import pset

class Task:

	def __init__(self,params):
		self.params = params
	
	def transform_params(self,p2):
		return p2
	#return p2

	def processed_params(self):
		hash = hashlib.md5(	).hexdigest()

		p2 = self.params.copy()
		p2 = self.transform_params(p2)
		
		p2['output_dir'] = self.out_dir().rstrip('/')+'/'+hash

		try:
			os.mkdir(p2['output_dir'])
		except:
			pass
	    	
		return p2

