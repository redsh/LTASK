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

