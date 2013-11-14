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

import os,sys,json,hashlib,math,time
from parameter_scan import parameter_scan_3,ls

def run_task(task):
    task.run() 

def write_scan_header(P, cls, out_dir):
    
    try:
    	os.mkdir(out_dir)
    except:
    	pass
	write_scan_header.header = []
	def write_header(params):
		p2 = cls(params).processed_params()
		write_scan_header.header += [p2]
	
	parameter_scan_3(P,write_header)
	
	header = write_scan_header.header
	header_fn = hashlib.md5(json.dumps(header,indent=4)).hexdigest()
	header_fn = out_dir+'scan-'+ P.name + '-' +header_fn+'.json'
	#print header_fn
	
	open(header_fn,'w').write(json.dumps(header,indent=4))
	
	return header_fn


def my_import(clspath):
    clspath = clspath.split('.')
    #print '.'.join(clspath[:-1])
    
    Task = __import__('.'.join(clspath[:-1]))
    
    for a in clspath[1:]:
    	#print Task.__name__
	    Task = getattr(Task,a)
    return Task


class AbstractLauncher(object):
    def __init__(self,clspath,header_fn,text_arguments=''):
        self.TaskCls = my_import(clspath+'.Task')
        self.clspath = clspath
        self.header_fn = header_fn
        
        #print '{%s}'%text_arguments
        self.args = {}
        for ap in text_arguments.split(','):
            if len(ap)<=1: continue
            k = ap.split(':')[0]
            v = ap.split(':')[1]
            try:
                self.args[k]=int(v)
            except:
                try:
                     self.args[k]=float(v)
                except:
                    self.args[k]=v
                    
        #self.args      = json.loads('{%s}'%text_arguments)
    def map(self,x):
    	pass
    def join(self):
    	pass
        
#def init_worker():
#	import signal#
#	signal.signal(signal.SIGINT, signal.SIG_IGN)
    
class PoolLauncher(AbstractLauncher):
    
    def __init__(self,cls,header_fn,arguments=''):
        super(PoolLauncher,self).__init__(cls,header_fn,arguments)
        from multiprocessing import Pool
        self.pool = Pool(self.args['n'])
        
    def map(self,tasks):
        #print tasks
        self.pool.map_async(run_task, tasks)
    
    def join(self):	
    	self.pool.close()
    	self.pool.join()
    	
    	

class QueueLauncher(AbstractLauncher):

    def __init__(self,cls,header_fn,arguments=''):
        super(QueueLauncher,self).__init__(cls,header_fn,arguments)
        
        self.tasks_per_job = self.args['tasks_per_job']
        self.tasks_count   = 0
        
    def map(self,cmds):
        self.tasks_count += 1
        
    def join(self):
        n_jobs = int(math.ceil(  float(self.tasks_count)/self.tasks_per_job ))
    
        s = 0
        e = self.tasks_per_job
        
        for i in range(n_jobs):
            if e >= self.tasks_count: e = self.tasks_count
            
            self.create_job_with_task_range(i, s, e)
            
            s+=n_jobs
            e+=n_jobs

    def create_job_with_task_range(self,job_id,start,end):
        print 'impl'
    


class SSHLauncher(AbstractLauncher):
    
    def __init__(self,cls,header_fn,arguments=''):
        
        #print cls
        super(SSHLauncher,self).__init__(cls,header_fn,arguments)
        
        fn = 'hosts.json'
        if self.args.has_key('fn'):
            fn = self.args['fn']
            
        self.hosts = json.loads(open(fn,'r').read())['hosts']
        
        import paramiko
        
        self.sshs   = []
        self.stdin  = []
        self.stdout = []
        self.stderr = []
        self.pid    = {}
        
        self.tasks_count = 0
        
        do_deploy=False
        if self.args.has_key('deploy'):
	        do_deploy = self.args['deploy']
        
        if do_deploy:
            os.system('hg add %s'%header_fn)
            os.system('hg ci -m "autodeploy" -uredsh')
        
        for h in self.hosts:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(h['hostname'],username=h['username'],password=h['password'])
            
            stdin, stdout, stderr = ssh.exec_command("uptime && killall -9 QS_bubble && killall -9 QS_bubble")
            print( stdout.read() )
            
            self.sshs   += [ ssh    ]
            self.stdin  += [ stdin  ]
            self.stdout += [ stdout ]
            self.stderr += [ stderr ]
            
            #deploying
            if h['hg_dir'] != '' and do_deploy:
                os.system('hg push ssh://%s/%s'%(h['hostname'],h['hg_dir']))
                ssh.exec_command('cd %s && hg up'%(h['hg_dir']))
                
    	time.sleep(2)
    
    def map(self,cmds):
       
        self.tasks_count += 1
        
        
    def join(self):
        
        task_head = 0
        
        try:
        
			while task_head < self.tasks_count:
				
				for host_i,f in enumerate(self.stdout):
	
					#print  f.channel.exit_status_ready()
					
					if not f.channel.exit_status_ready():
						sys.stdout.write('%d >> %s'% (host_i,f.readline()))
						pass
					else:
						self.launch_job_at_host(host_i, task_head, self.hosts[host_i]['cores'])
						task_head += self.hosts[host_i]['cores']
						#time.sleep(0.5)
		
        except (KeyboardInterrupt, EOFError):
			print 'killing..'
			for host_i,f in enumerate(self.stdout):
				#print('kill %d %s %s'% (self.pid[host_i],self.hosts[host_i]['hostname'],self.sshs[host_i].exec_command('kill -9 %d'%(self.pid[host_i]))[1].readline() ))
        
        		#TODO
				print('kill2 %s %s'%  (self.hosts[host_i]['hostname'],self.sshs[host_i].exec_command('killall -9 QS_bubble')[1].readline() ))
        		
        		#self.sshs[host_i].close()
        		
			#time.sleep(10)
        

    def launch_job_at_host(self,host_id,s,cores):
        e = s+cores
        
        host = self.hosts[host_id]
        ssh  = self.sshs [host_id]
        
        job = ''
        job += "cd '%s' && echo $$; exec "%(  host['dir'] )
        job += 'python bubble/launch.py -l %s -q %s %d %d\n\n'%(host['launcher'],self.header_fn,s,e) #TODO
        #job  += 'sleep 2s'
        
        print ( '>>> launching %d-%d on host %s'%(s,e,host['hostname']) )
        
        #job  = ' ' + job
        print job
        #return
        #job = 'ls'
        #job = 'sleep 2m'
        #job = 'sleep 4s && echo $$; exec hostname'
        
        stdin, stdout, stderr = ssh.exec_command(job)
        
        self.pid[host_id] = int(stdout.readline())
        print 'pid %d'%self.pid[host_id]
        
        self.stdin [host_id] = stdin
        self.stdout[host_id] = stdout
        self.stderr[host_id] = stderr
        

        
class SlurmQueueLauncher(QueueLauncher):
    def __init__(self,cls,header_fn,args=''):
        super(SlurmQueueLauncher,self).__init__(cls,header_fn,tasks_per_job,args)
        
    def create_job_with_task_range(self,job_id,s,e):
        clspath = self.TaskCls
        cname = clspath.__module__+'.'+str(clspath.__name__)
    
        #exit(0)
        
        job =  '#!/bin/bash\n'
        job += "cd '%s'\n"%(os.path.abspath(os.getcwd()))
        job += 'python bubble/launch.py -q %s %s %d %d\n\n'%(cname,self.header_fn,s,e) #TODO
        
        jobname = 'tmp_job.sh'
        open(jobname,'w').write(job)
        os.system('chmod +x %s'%(jobname))
        #print job
        os.system('sbatch -n 1 -p run %s'%(jobname))

def params2task2map(params):
    tx = Task(params)
    launcher.map([tx])
     
def get_js_from_params(options,args):
    if not options.r:
        clspath = '.'.join(options.params.split('.')[:-1])
    else:
        clspath = options.r 
        
   
    largs = ''
    if options.a: largs = options.a
    
    Task = my_import(clspath+'.Task')
    
    if options.q:
        fn    = args[0]
        print clspath
        
        js = json.loads(open(fn,'r').read())
        
        return js
        
    else:
        if options.params:
            P = my_import(options.params)()
        else:
            P = my_import(clspath+'.Params')()
         
        out_dir = Task(P).out_dir()
        
        header_fn = write_scan_header(P, Task, out_dir)
    
        #launcher = launcherCLS(clspath,header_fn, largs)
        
        js = ls(P)
        
    return js,clspath+'.Task'
  
    
if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-q", "--queuestep",action='store_true',default=False,dest='q')
    parser.add_option("-l", "--launcher",action='store',default=None,dest='l')
    parser.add_option("-r", "--run",action='store',default=None,dest='r')
    parser.add_option("-a", "--args",action='store',default=None,dest='a')
    parser.add_option("-p", "--params",action='store',default=None,dest='params')
    
    (options, args) = parser.parse_args()
    print options,args
    
    if not options.r:
        clspath = 'run'
    else:
        clspath = options.r 
           
        
    launcherCLS = WebLauncherServer #PoolLauncher
    
    if options.l:
        try:
            launcherCLS = my_import(__name__+'.'+options.l) 
        except:
            launcherCLS = my_import(options.l)       

    largs = ''
    if options.a: largs = options.a
    
    Task = my_import(clspath+'.Task')
    
    if options.q:
        fn    = args[0]
        print clspath
        
        js = json.loads(open(fn,'r').read())
        
        if len(args) >= 3:
            start = int(args[1])
            end   = int(args[2])
        else:
            start = 0
            end   = len(js)  
        
        launcher = launcherCLS(clspath,fn,largs)
        
        for i in range(start,end):
            p = js[i]
            #p = js[k]
            
            t = Task(p)
            #print [t]
            launcher.map([t])
        
        launcher.join()
    else:
        if options.params:
            P = my_import(options.params)()
        else:
            P = my_import(clspath+'.Params')()
            
        
        #out_dir = my_import(clspath+'.out_dir')
        out_dir = Task(P).out_dir()
        
        header_fn = write_scan_header(P, Task, out_dir)
    
        launcher = launcherCLS(clspath,header_fn, largs)
        parameter_scan_3(P,params2task2map)
        launcher.join()
        
        


