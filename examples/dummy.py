from ltask.parameter_scan import *


class Task(object):
    def __init__(self,params):
        self.params = params
 
    def processed_params(self):
        hash = hashlib.md5(json.dumps(self.params,indent=4)).hexdigest()
        
        #(omega0/omegap)^2*kpL/kp
        #(omega0/omegap)^2 * omegap/c
        
        kp   = lfwa.k0/self.params['k0_kp']
        ctau = self.params['kpL']/kp
        ne   = lfwa.ne(kp)
        
        p2 = self.params.copy()
        
        depl = (lfwa.L_depletion(ne, ctau)/lfwa.c)*lfwa.omegap(ne)
        p2['hash'] = hash
        if p2.has_key('smax') == False:
        	p2['smax'] = depl
        else:
	    	print 'depletion/500Zr'
	    	print depl,p2['smax'] 
	    
        p2['output_dir'] = self.out_dir()+'bubble-'+hash
        
    	
        return p2
  
    def out_dir(self):
        return './exes/DUMP/'
		
    def kill(self):
  		os.system('killall -9 QS_bubble')
  		
    def run(self):
        
        params = self.processed_params()
        od = params['output_dir']
        try:
            os.mkdir(od)
        except:
            pass
            
        util.dict2configfile(od+'/config.txt',params)
        open(od+'/config.json','w').write(json.dumps(params,indent=4))
        cmd = './exes/QS_bubble %s'%(od+'/config.txt')
        
        open(od+'/status.txt','w').write(str(datetime.datetime.now()) + ':' + 'running\n')
        print cmd + " : "+str(self.params) 
        
        if os.system('make -C build QS_bubble'):
        	print 'failed>> make -C build QS_bubble'
        	#exit(1)
        	return -1
        	
        ret = os.system(cmd)
        
        open(od+'/status.txt','a').write(str(datetime.datetime.now()) + ':' + 'done\n')
        
        return ret
     

def ParamsD(k0kp,name):
	Pk0_kp = pset('k0_kp',k0kp)
	Pa0    = pset('a0',[0.25, 0.5, 0.75, 1.0])
    
	PkpW  = pset('kpW',[4.5])
	PkpL  = pset('kpL',[2.0])
	Pidz  = pset('idz',[30.0,50.0])
    
	Pids = pset('ids',[100.0,200.0])
	Pfrozen = pset('frozen',[0])
	Pfilter = pset('filter_passes',[100])
    
	P = Pa0*Pk0_kp*PkpW*PkpL*Pidz*Pfrozen*Pids*Pfilter

	P.name = name
	return P  


def phys():
	Pk0_kp = pset('k0_kp',[10.,15.,20.,25.,30.,35.])
	Pa0    = pset('a0',[0.1, 0.25, 0.30, 0.50, 0.65, 0.70, 0.85, 1.0])
    
	PkpW  = pset('kpW',[4.5])
	PkpL  = pset('kpL',[2.0])
	Pidz  = pset('idz',[40.0])
    
	Pids = pset('ids',[150.0])
	Pfrozen = pset('frozen',[0])
	Pfilter = pset('filter_passes',[100])
    
	P = Pa0*Pk0_kp*PkpW*PkpL*Pidz*Pfrozen*Pids*Pfilter

	P.name = 'phys'
   
	return P  


def bubble2():
    Pk0_kp = pset('k0_kp',[10.,15.,20.,25.,30.,35.,])
    
    Pa0    = pset('a0',   [0.1, 0.25, 0.30, 0.50, 0.65, 0.70, 0.85, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25, 3.5, 3.75, 4.0, 4.25, 4.5, 4.75, 5.0])
    kpW_   = []
    for a in [f[0][1] for f in Pa0.config]:
        kpw = 4.5
        if a > 2.0:
            kpw = 2.0*math.sqrt(a)
        #print a,kpw
        
        kpW_ += [kpw]
        
    kpW    = pset('kpW',  kpW_)
    P    = Pa0|kpW
    
    #PkpW  = pset('kpW',[4.5])
    PkpL  = pset('kpL',[2.0])
    Pidz  = pset('idz',[40.0])
    
    Pids = pset('ids',[150.0])
    Pfrozen = pset('frozen',[0])
    Pfilter = pset('filter_passes',[100])
    
    P = Pk0_kp*P
    
    kpsmax   = []
    for k0_kp,kpW in [(f[0][1],f[1][1]) for f in P.config]:
        print 'k0kp,kpw,500zr',k0_kp,kpW,10.*lfwa.kpZr(k0_kp,kpW)#,(lfwa.L_depletion(ne, ctau)/lfwa.c)*lfwa.omegap(ne)
        kpsmax += [25.*lfwa.kpZr(k0_kp,kpW)]
    PsSMAX    = pset('smax',kpsmax)
    
    print len(P.config),len(PsSMAX.config)
    
    P = P | PsSMAX
    
    P = P*PkpL*Pidz*Pfrozen*Pids*Pfilter

    P.name = 'bubble2'
    
    L = ls(P)
    for u,l in enumerate(L):
        print u,l
    
    return P  
    

def pop_trapping():
    Pk0_kp = pset('k0_kp',[10.,20.,30.,40.,   50.,60.,70.,80.,    90.,100.,110.,120.,    130.,140.,150.,160.])
    
    Pa0    = pset('a0',   [4.5])
    kpW    = pset('kpW',  [2*math.sqrt(4.5)])
    Pa0    = Pa0 * kpW
    PkpL  = pset('kpL',[1.0])
    
    Pidz  = pset('idz',[40.0])
    
    Pids = pset('ids',[150.0])
    Pfrozen = pset('frozen',[0])
    Pfilter = pset('filter_passes',[100])
    Ppsimin = pset('psi_min',[-0.96])
    
    P = Pa0*Pk0_kp*PkpL*Pidz*Pfrozen*Pids*Pfilter*Ppsimin

    P.name = 'pop_trapping'
    
    L = ls(P)
    for u,l in enumerate(L):
        print u,l
    
    return P 
    
def pop_trapping_3():
    Pk0_kp = pset('k0_kp',[10.,20.,30.,40.,   50.,60.,70.,80.,    90.,100.,110.,120.,    130.,140.,150.,160.])
    
    Pa0    = pset('a0',   [4.5])
    kpW    = pset('kpW',  [2*math.sqrt(4.5)])
    Pa0    = Pa0 * kpW
    PkpL  = pset('kpL',[1.0])
    
    Pidz  = pset('idz',[50.0])
    
    Pids = pset('ids',[150.0])
    Pfrozen = pset('frozen',[0])
    Pfilter = pset('filter_passes',[100])
    Ppsimin = pset('psi_min',[-0.96])
    
    PsMatched = pset('matched',[0])
    
    P = Pk0_kp*kpW
    
    kpsmax   = []
    for k0_kp,kpW in [(f[0][1],f[1][1]) for f in P.config]:
    	print 'k0kp,kpw,500zr',k0_kp,kpW,10.*lfwa.kpZr(k0_kp,kpW)#,(lfwa.L_depletion(ne, ctau)/lfwa.c)*lfwa.omegap(ne)
        kpsmax += [4.*lfwa.kpZr(k0_kp,kpW)]
    PsSMAX    = pset('smax',kpsmax)
    
    print len(P.config),len(PsSMAX.config)
    P = P | PsSMAX
    
    P = P*Pa0*PkpL*Pidz*Pfrozen*Pids*Pfilter*Ppsimin*PsMatched

    P.name = 'pop_trapping_3'
    
    L = ls(P)
    for u,l in enumerate(L):
        print u,l
        
    return P 
    
def Params():
	return ParamsD([10.,20.,30.,40],'all')



def params10():
	return ParamsD([10.],'k10')

def params20():
	return ParamsD([20.],'k20')

def params25():
	return ParamsD([25.],'k25')

def params30():
	return ParamsD([30.],'k30')

def params40():
	return ParamsD([40.],'k40')
    


        

