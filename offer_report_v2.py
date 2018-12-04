[root@delffdpnoti1 RServer]# cat offer_report_v2.py
#!/usr/bin/python

import sqlite3 as s3
from collections import namedtuple
import datetime
import sys
import os
from collections import defaultdict
import gzip
import shutil
import glob
import re

offercfg=namedtuple('offercfg',['circle','rulename','offer_eq','daysago','offer_ne','dbpath','outpath'])
rulecfg=namedtuple('rulecfg',['circle','rulename','offer_eq','expdate','offer_ne','dbpath','outpath'])

cfg=[
  offercfg(
    circle='DL',rulename='Nav04152',offer_eq=4152,daysago=0,offer_ne=[4121,4123],
    dbpath='/opt/offline/RServer/db_files/DL',outpath='/opt/offline/RServer/db_files/DL'
  ),
  offercfg(
    circle='DL',rulename='Nav04152out',offer_eq=4121,daysago=0,offer_ne=[4152,4123],
    dbpath='/opt/offline/RServer/db_files/DL',outpath='/opt/offline/RServer/db_files/DL'
  ),
]

#15032331|4152|2017-11-06|2017-11-07|18:47:17+05:30|00:00:00+05:30|2|||
#15032331|3163|2017-11-06|2017-11-07|18:47:18+05:30|00:00:00+05:30|2|||
#15181422|4121|2017-11-06|2017-11-07|08:28:03+05:30|00:00:00+05:30|2|||
#15181422|4152|2017-11-06|2017-11-07|||0|||


def getfiles(dbpath,dateout):
  outlist=set()
  filedate=dateout.strftime('%Y%m%d')
  for file in glob.glob(dbpath+'/DUMP_offer_*'+filedate+'*'):
    if re.search('_offer_attributes_account_',file):
      continue
    if re.search('.gz',file):
      filename_sqlite=file.replace('.gz','')
      f_in=gzip.open(file, 'rb')
      f_out=open(filename_sqlite,'wb')
      shutil.copyfileobj(f_in, f_out)
      f_in.close()
      f_out.close()
      outlist.add(filename_sqlite)
      print "Found {f}\n".format(f=filename_sqlite)
    else:
      outlist.add(file)
      print "Found {f}\n".format(f=file)
  return outlist



def process_dump(circlein,rulelist,datetimeout,dateout):

  circlerules=[]
  allcirclerules=rulelist[circlein]
  #rulecfg=namedtuple('rulecfg',['circle','rulename','offer_eq','expdate','offer_ne','dbpath','outpath'])
  for rulevalue in allcirclerules:
    expdate_value=datetime.datetime.now() - datetime.timedelta(days=rulevalue.daysago)
    expdate=expdate_value.strftime("%Y-%m-%d")
    circlerules.append(rulecfg(circle=rulevalue.circle,rulename=rulevalue.rulename,offer_eq=rulevalue.offer_eq,expdate=expdate,offer_ne=rulevalue.offer_ne,dbpath=rulevalue.dbpath,outpath=rulevalue.outpath))


  offerfiles=getfiles(circlerules[0].dbpath,dateout)
  if len(offerfiles) == 0:
    print "No offer files found in {p}\n".format(p=circlerules[0].rule.dbpath)
    sys.exit()

  #06112017110511
  outf={}
  for rname in circlerules:
    outfilename=circlein+'_'+rname.rulename+'_'+datetimeout.strftime("%d%m%Y%H%M%S")
    reportout=rname.dbpath+'/'+outfilename
    reportoutfinal=rname.outpath+'/'+outfilename
    fout=open(reportout+'.tmp','w')
    outf[rname.rulename]={'fout': fout ,'record_count': 0, 'file': reportout, 'fileout' : reportoutfinal  }


  for file in offerfiles:
    print "File : {f} started {dt}\n".format(f=file,dt=datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    conn = s3.connect(file)
    c1 = conn.cursor()
    c2 = conn.cursor()
    for row in c1.execute('SELECT distinct(Account_ID) FROM OFFER'):
      msisdn=row[0]
      rv=c2.execute('select Offer_ID,Expiry_Date FROM OFFER where Account_ID=(?)',(row[0],))
      values=rv.fetchall()
      offers={}
      for v in values:
        offers[v[0]]=v[1]
      #print offers

      for rule in circlerules:
        #circle='DL',rulename='outoffer1',offer_eq=4152,expdate='2017-11-07',offer_ne=4121
        condition1=None
        condition2=None

        for ofne in rule.offer_ne :
          if ofne in offers:
            condition1=False
          else:
            condition1=True

        if rule.offer_eq in offers:
          offerexp=offers[rule.offer_eq]
          if offerexp == rule.expdate :
            condition2=True
        else:
          condition2=False

        if condition1 and condition2:
          outf[rule.rulename]['record_count'] += 1
          outf[rule.rulename]['fout'].write("91{v1},{v2},{v3}\n".format(v1=msisdn,v2=rule.offer_eq ,v3=expdate))
    conn.close()
    print "File : {f} Ended {dt}\n".format(f=file,dt=datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

  for outrule in circlerules:
    if outf[outrule.rulename]['record_count'] == 0:
      print "No Record found for Rule {d} for {ci}\n".format(d=outrule.rulename,ci=outrule.circle)
      outf[outrule.rulename]['fout'].close()
      os.remove(outf[outrule.rulename]['file']+'.tmp')
    else:
      outf[outrule.rulename]['fout'].close()
      print "Total Records: {t} for Rule {d} for {ci}\n".format(t=outf[outrule.rulename]['record_count'],d=outrule.rulename,ci=outrule.circle)
      os.rename(outf[outrule.rulename]['file']+'.tmp',outf[outrule.rulename]['fileout'])

# main start here
rulelist=defaultdict(list)
for r in cfg:
  rulelist[r.circle].append(r)

circlein=None
if len(sys.argv) == 1:
  print "Provide the circlename argument \n"
  sys.exit()
else:
  circlein = sys.argv[1]
  if circlein  not in rulelist:
    print "{c} not found in script cfg\n".format(c=circlein)
    sys.exit()


dateout= datetime.datetime.now() - datetime.timedelta(days=0)

datetimeout = datetime.datetime.now()
process_dump(circlein,rulelist,datetimeout,dateout)
[root@delffdpnoti1 RServer]#

					
			
			
