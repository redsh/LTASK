import ltask,json
from ltask import pset

class Task(ltask.Task):
    #def __init__(self,params):
        #super(ltask.Task,self).__init__(self,params)

    def transform_params(self,p): #called in ltask.Task processed_params
    	return p
  
    def out_dir(self):
        return './examples/out/'
		
    def kill(self):
  		pass
  		
    def run(self):
    	p = self.processed_params()
    	
    	open(p['output_dir']+'/params.json','w').write(json.dumps(self.params,indent=5))
    		
    	print(p)
     

def scan():
	
	P = pset('a',[1,2,3,4,5,6,7])*pset('b',['a','b','c','d','e']) + pset('x',[1000,10001])
	P.name = 'dummy1'
	
	return P
