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

import os,sys,json,glob

def js_scan(scanfile):
    js = json.loads(open(scanfile,'r').read())
    
def scan(args,options,rm=False):
    js = json.loads(open(args[0],'r').read())
    
    if rm:
        for j in js:
            cmd = 'rm -Rf %s'%(j['output_dir'])
            print cmd
            os.system(cmd)
        
    else:
        for j in js:
            print j['hash']
            try:
                st = open(j['output_dir']+'/status.txt','r').read()
                print(st)
            except:
                print('ERR')
            
    
def ls(args,options):

    if not options.queue:
        cfgs = glob.glob('exes/DUMP/bubble-*/config.json')
    else:
        cfgs = json.loads(open(options.queue,'r').read())
        cfgs = [l['output_dir'] + '/config.json' for l in cfgs]
        
    lengths = []    
    fields  = []
    fields_set = set()
    
    cfgs_a = cfgs
    cfgs   = []
    for c in cfgs_a:   
        try:
            js = json.loads(open(c,'r').read())
            cfgs += [c]
        except:
            continue
            
    for i,c in enumerate(cfgs):
        js = json.loads(open(c,'r').read())
        
        for k in js:
            if not k in fields_set:
                lengths.append(len(str(js[k])))
                fields.append(k)
                fields_set.add(k)
    
    line = ''
    for i,k in enumerate(fields):
        spacing = ''
        if lengths[i]-len(k) > 0:
            spacing = '-'*(lengths[i]-len(k))
        line += k + spacing + '\t'
    print(line)
                
                
    for i,c in enumerate(cfgs):
        js = json.loads(open(c,'r').read())
        
        line = ''
        
        for i,k in enumerate(fields):
            if js.has_key(k):
                if type(js[k]) == type(0.1):
                    line += '%.2f'%(js[k]) + '\t'
                elif type(js[k]) == type(1):
                    line += '%d'%(js[k]) + '\t'
                else:
                    line += str(js[k]) + '\t'
                    
            else:
                line += '-'*lengths[i]
        
        print(line)

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-l", "--ls",action='store_true',dest='ls')
    parser.add_option("-s", "--scan",action='store_true',dest='scan')
    parser.add_option("-q", "--queue",action='store',dest='queue')
    parser.add_option("--rm",action='store_true',dest='rm')
    
    (options, args) = parser.parse_args()
    print options,args
    
    if not options.ls:
        ls(args,options)
    elif options.rm:
        scan(args,options,1)
    elif options.scan:
        scan(args,options)
    else:
        pass