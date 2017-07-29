import pandas as pd
import sframe as gl
from datetime import datetime, timedelta
import sframe.aggregate as agg
from itertools import permutations
import numpy as np
import pickle
from operator import itemgetter
import collections
import json
from flask import Flask,jsonify, request

class Con:
    def __init__(self,docknodict):
    	docknodict = json.loads(docknodict)
        tcdf = pd.DataFrame({'dockno': [int(docknodict.keys()[0])], 'arrv':[docknodict.values()[0][0]],'orgn':[docknodict.values()[0][1]],'locn': [docknodict.values()[0][2]],'dest': [docknodict.values()[0][3]]})
        print 'tcdf', tcdf
        #eta = Geteta(datetime.strptime("2017-07-07 15:30:00","%Y-%m-%d %H:%M:%S"),"DELM","DELM","NAGB")
        #tcsf['processed'] = tcsf.apply(lambda x: x['condata']+'_processed')
        tcdf['arrv'] = tcdf.apply(lambda x: datetime.strptime(x['arrv'],"%Y-%m-%d %H:%M:%S"), axis=1)
        tcdf['eta'] = tcdf.apply(lambda x: Geteta(x['arrv'], x['orgn'],x['locn'],x['dest']).geteta(journey="no"), axis=1)
        print 'type', tcdf['eta'][0], type(tcdf['eta'][0])
        if isinstance(tcdf['eta'][0], datetime): print 'dt'
        if isinstance(tcdf['eta'][0], pd.tslib.Timestamp): print 'ts'
        if hasattr(tcdf['eta'][0], 'isoformat'): print 'yes iso'
        print tcdf
        #a.geteta(journey="no")
        self.tc = tcdf
        
    #date_handler = lambda obj: (obj.isoformat() if isinstance(obj, (datetime.datetime, datetime.date)) else None)
        
    def returndata(self):
     	#retdf = self.tc.to_dataframe()
        retdf = self.tc
     	js = retdf.set_index('dockno')
     	js = js.to_json(orient='index')
     	#js1 = jsonify(js)
        date_handler = lambda obj: obj.isoformat() if hasattr(obj, 'isoformat') else json.JSONEncoder().default(obj)
        js1 = json.dumps(js, default=date_handler)
        return js1


class Geteta:
    def __init__(self,arratloc,origin,location,destination):
        pkl_file = open('path_dict.pkl', 'rb')
        path_dict = pickle.load(pkl_file)
        pkl_file.close()

        schedulesf = gl.SFrame.read_csv('schedule file.csv')
        schedulesf["Route Origin"] = schedulesf.apply(lambda x: x["Route Details"].split("-")[0])
        schedulesf["Route Destn"] = schedulesf.apply(lambda x: x["Route Details"].split("-")[-1])
        schedulesf["NEW PATH"] = schedulesf.apply(lambda x: x["Route Origin"]+"-"+x["Route Destn"])
        schedule=schedulesf[schedulesf["Origin"]==schedulesf["Route Origin"]]
        schedulesf=schedulesf[(schedulesf["Departure time"]!='-')&(schedulesf["Arrival time"]!='-')]
        def get_tt(dept,depd,arrt,arrd): #check if self required
            dep = datetime.strptime('01/01/2016','%d/%m/%Y')+timedelta(days=depd,hours=int(str(dept.split(":")[0])))
            arr = datetime.strptime('01/01/2016','%d/%m/%Y')+timedelta(days=arrd,hours=int(str(arrt.split(":")[0])))
            tt=((arr-dep).total_seconds())/3600
            return tt
        schedulesf["TT"]=schedulesf.apply(lambda x: get_tt(x["Departure time"], x["Departure Day"], x["Arrival time"],x["Arrival day"]))
        getttdict = {}
        for ele in zip(schedulesf['Origin'],schedulesf['Destination'],schedulesf['TT']):
            getttdict.update({(ele[0],ele[1]):ele[2]})
        scheduledeps = gl.SFrame.read_csv('schedule file.csv').select_columns(["Route Code","Route Details","Origin","Destination","Departure time"])
        scheduledeps["Routelist"]=scheduledeps.apply(lambda x: x["Route Details"].split("-"))
        scheduledeps["OD Routelist"] = scheduledeps.apply(lambda x: [[i[0],i[1]] for i in permutations(x['Routelist'],2) if x['Routelist'].index(i[0])<x['Routelist'].index(i[1]) and x["Origin"]==i[0]])
        scheduledeps2 = scheduledeps.stack("OD Routelist","OD Combos") 
        def odpath(odcombos,routedetails):  #check if self required
            odpathlist=[]
            transittime=0.0
            for i in range(routedetails.index(odcombos[0]),(routedetails.index(odcombos[1]))):
                odpathlist = odpathlist+[(routedetails[i],routedetails[i+1])]
            for i in odpathlist:
                if getttdict.get((i[0],i[1]))!=None:
                    transittime = transittime+float(str(getttdict.get((i[0],i[1]))))
                else:
                    transittime=transittime
            return transittime
        scheduledeps2["TT"]=scheduledeps2.apply(lambda x: odpath(x["OD Combos"],x['Routelist']))
        scheduledeps2["Dept_TT"]=scheduledeps2.apply(lambda x: [x["Departure time"]]+[x["TT"]])
        scheduledeps2group = scheduledeps2.groupby("OD Combos",operations={"Dept_TT":agg.DISTINCT("Dept_TT")})
        scheduledeps2group["Origin"] = scheduledeps2group.apply(lambda x: x["OD Combos"][0])
        scheduledeps2group["Destn"] = scheduledeps2group.apply(lambda x: x["OD Combos"][1])
        scheduledepsdict={}
        for i in zip(scheduledeps2group["Origin"],scheduledeps2group["Destn"],scheduledeps2group["Dept_TT"]):
            scheduledepsdict.update({(i[0],i[1]):i[2]})
        
        self.path_dict = path_dict
        self.scheduledepsdict = scheduledepsdict
        self.origin = origin
        self.arratloc = arratloc
        self.location=location
        self.destination=destination
        
#### METHODS/FUNCTIONS BEGIN HERE
    def getconpath(self): #,origin,location,destination 
        path_dict = self.path_dict
        origin = self.origin
        destination = self.destination
        location = self.location
        fullpath = min(path_dict.get((origin,destination)) if path_dict.get((origin,destination))!=None else [[]],key=lambda x: len(x))
        if location in fullpath:
            return fullpath[fullpath.index(location):]
        else:
            auxpath = min(path_dict.get((location,destination)) if path_dict.get((location,destination))!=None else [[location,destination]],key=lambda x: len(x))
            return auxpath


    def nextdepfunc(self,org,dest,arratloc): #check if self required
        timelist=[]
        scheduledepsdict=self.scheduledepsdict
        if scheduledepsdict.get((org,dest))!=None:
            for i in scheduledepsdict.get((org,dest)):
                nextdep1 = arratloc.replace(hour=int(i[0].split(":")[0]),minute=int(i[0].split(":")[1]),second=0,microsecond=0)
                nextarr1 = nextdep1+timedelta(hours=i[1])
                nextdep2 = arratloc.replace(hour=int(i[0].split(":")[0]),minute=int(i[0].split(":")[1]),second=0,microsecond=0)+timedelta(days=1)
                nextarr2 = nextdep2+timedelta(hours=i[1])
                if nextdep1>arratloc:
                    timelist.append([nextdep1,nextarr1])
                else:
                    timelist.append([nextdep2,nextarr2])
            return min(timelist,key=itemgetter(0))
        else:
            return [arratloc+timedelta(hours=3),arratloc+timedelta(hours=3)]  #for market and vb movements  --- con can never fail here


    def geteta(self,etatype='schedule',journey="yes"):
        arratloc = self.arratloc
        conpath = self.getconpath()
        etadict = collections.OrderedDict()
        for i in range (1,len(conpath)):
            org = conpath[i-1]
            dest = conpath[i]
            if conpath[i-1]==conpath[0] and etatype=="market":
                nextdep = datetime.today()+timedelta(hours=3)
                etadict.update({(org,dest):(arratloc,nextdep)})
                arratloc = self.nextdepfunc(org,dest,arratloc)[1]+timedelta(seconds=(self.nextdepfunc(org,dest,arratloc)[1]-self.nextdepfunc(org,dest,arratloc)[0]).total_seconds()*0.1)+timedelta(hours=5)  ##market penalty & unloading and loading hours
            else:
                nextdep = self.nextdepfunc(org,dest,arratloc)[0]
                etadict.update({(org,dest):(arratloc,nextdep)})
                arratloc = self.nextdepfunc(org,dest,arratloc)[1]+timedelta(hours=5) #unloading hours & loading hours
        
        if journey=="yes":
            return etadict.items()
        else:
            try:
                return max(max(etadict.values()))
            except:
                return None
    
    
   
app = Flask(__name__)

@app.route('/')
@app.route('/<condata>')

def index(condata):

	if condata:
		a = Con(condata).returndata()
        #print 'after a', a
		#return jsonify(a)
        #return jsonify({stringify:true})
        #return json.dumps(a)
        return a
	#else:
		#return 'error'

if __name__ == '__main__':
    app.run(debug=True)