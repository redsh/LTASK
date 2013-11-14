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

import os,sys,json,hashlib,math,time,requests,traceback
from parameter_scan import parameter_scan_3
import bottle
from launch import get_js_from_params,my_import,write_scan_header
import threading

def run_server(params,clspath,upload_path):

	bottle.tasks = [dict(params=p,class_path=clspath) for p in params]

	bottle.current_task = 0
	
	@bottle.route('/alltasks')
	def alltasks():
		return json.dumps(bottle.tasks,indent=4)

	@bottle.route('/tasks',method='GET')
	def remtasks():
		if bottle.current_task >= len(bottle.tasks): return json.dumps([],indent=4)
		return json.dumps(bottle.tasks[bottle.current_task:],indent=4)

	@bottle.route('/tasks',method='POST')
	def posttasks():
		bottle.tasks += json.loads(bottle.request.POST['data'])
		
	@bottle.route('/task')
	def task():
		
		if bottle.current_task >= len(bottle.tasks): return json.dumps([],indent=4)
		ct = bottle.current_task
		
		print('Assigning task %d/%d to %s'%(ct,len(bottle.tasks),bottle.request.remote_addr))
		
		bottle.current_task += 1
		
		return json.dumps(bottle.tasks[ct],indent=4)

	@bottle.route('/upload')
	def task():	
		return json.dumps(dict(url=upload_path),indent=4)

	bottle.run(host='0.0.0.0', port=1988)

	
def run_client(server_url,options):

	#open('client.pid','w').write('%d\n'%os.getpid())

	while True:
	
		try:
			js = json.loads(requests.get(server_url+'/task').content)
		except requests.exceptions.ConnectionError,e:
			print (e)
			#traceback.print_exc()
			js = []
		except Exception,e2:
			print (e2)
			traceback.print_exc()
			break
			
		if len(js) == 0:
			if options.retry_if_empty:
				print('no tasks from %s, retrying..'%server_url)
				time.sleep(0.5) #! throttling if no work
				continue	
			else:
				print('done!')
				return
				
		C = js['class_path']
		P = js['params']
		
		Task = my_import(C)
		t = Task(P)
		
		#write_scan_header(P,Task,t.processed_params()['output_dir'])
		t.run()
		
		url = json.loads(requests.get(server_url+'/upload').content)['url']
		
		if len(url) > 0:
			print js
			
			sshpub = open('%s/.ssh/id_rsa.pub'%(os.environ['HOME']),'r').read()
			print sshpub
			
			out_dir = t.processed_params()['output_dir']
			print out_dir
			print url
			print t.out_dir()
			cmd = ('scp -r %s %s'%(out_dir,url+t.out_dir()))
			
			#TODO remove files
			
			print cmd
			#exit(0)
			os.system(cmd)
		
def main_((options,args)):
	print (options,args)
	if options.tunnel:
		print 'ssh -NfL 19880:localhost:1988 %s' % args[0]
		os.system('ssh -NfL 19880:localhost:1988 %s' % args[0])
		args[0] = 'http://localhost:19880'

	if options.deploy:
		import paramiko
		hosts = json.loads(open(options.deploy,'r').read())
		os.system('hg commit -m "autodeploy"')
		for h in hosts['hosts']:
			print h
			#os.system('if [ ! -d %s ]; then echo hg clone ssh://%s/%s; fi'%(h['hg_dir'],h['hostname'],h['hg_dir']))
			os.system('hg push  ssh://%s/%s'%(h['hostname'],h['hg_dir']))
			
			
			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			print((h['hostname'],h['username'],h['password']))
			ssh.connect(h['hostname'],username=h['username'],password=h['password'])
            
			ssh.exec_command('cd %s && hg up'%(h['hg_dir']))
	
	if options.launch:
		import paramiko
		
		hosts = json.loads(open(options.launch,'r').read())
		for h in hosts['hosts']:
			cmd = ('cd %s && nohup python bubble/webtask.py -n %s -x %d'%(h['hg_dir'], options.hostname, h['cores']))
			print cmd
			
			ssh = paramiko.SSHClient()
			ssh.load_system_host_keys()
			#ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			print((h['hostname'],h['username'],h['password']))
			ssh.connect(h['hostname'],username=h['username'],password=h['password'])
            
			ssh.exec_command(cmd)
		return
		
	if options.server:
		js,clspath = get_js_from_params(options,args)

		cwd = os.path.abspath(os.getcwd())
		
		if not options.upload and options.hostname:
			options.upload = '%s:%s'%(options.hostname,cwd)
		elif not options.upload and not options.hostname:
			options.upload = 'castor:%s'%cwd
			options.upload = ''
			
		run_server(js,clspath,options.upload)
		
	elif options.post:
		js,clspath = get_js_from_params(options,args)
		tasks = [dict(params=p,class_path=clspath) for p in js]
		js = requests.post(args[0]+'/tasks',data=dict(data=json.dumps(tasks)),headers={'Content-type': 'application/json'})
		
	else:
		run_client(args[0],options)
	
def main((options,args)):
	try:
		main_((options,args))
	except KeyboardInterrupt:
		pass

	
if __name__ == '__main__':
	sys.path += [os.getcwd()]

	from optparse import OptionParser
	parser = OptionParser()

	parser.add_option("-s", "--server",action='store_true',default=False,dest='server')

	parser.add_option("-q", "--queuestep",action='store_true',default=False,dest='q')
	parser.add_option("-r", "--run",action='store',default=None,dest='r')
	parser.add_option("-a", "--args",action='store',default=None,dest='a')
	parser.add_option("-p", "--params",action='store',default=None,dest='params')
	parser.add_option("-u", "--upload",action='store',default=None,dest='upload')
	parser.add_option("-n", "--hostname",action='store',default=None,dest='hostname')
	parser.add_option("-t", "--tunnel",action='store_true',default=None,dest='tunnel')
	parser.add_option("-d", "--deploy",action='store',default=None,dest='deploy')
	parser.add_option("-x", action='store',default=None,dest='x')
	parser.add_option("-l","--launch", action='store',default=None,dest='launch')
	parser.add_option("--retry_if_empty", action='store_true',default=None,dest='retry_if_empty')
	
	
	parser.add_option("--post",action='store_true',default=None,dest='post')

	(options, args) = parser.parse_args()

	if options.x:
		from multiprocessing import Pool
		
		pool = Pool(int(options.x))
		print ([options,args])*int(options.x)
		pool.map_async(main, ([options,args],)*int(options.x))

		pool.close()
		pool.join()
	else:
		main([options,args])
