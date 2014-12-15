#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os
import shutil
import json
from datetime import datetime
import time
import MySQLdb as mdb
from urlparse import urlparse
import argparse
sys.path.append(os.path.abspath("."))
from properties import Properties

def getBrandSafety( json ):
	bsTypes=["drg","alc","off","dlm","adt","hat","sam"];
	scoreList=[];
	for bsType in bsTypes:
		try:
			score=json["bsc"][bsType];
			scoreList.append(score);
		except (KeyError), e: pass
	if scoreList:
		minScore=scoreList[0];
		for score in scoreList:
			if score<minScore:
				minScore=score
	else:
		minScore=0;
	return minScore

def gen_db_file( iasFile, dbFile):
	print "iasFile:"+iasFile+", dbFile:"+dbFile
	iasFH=open(iasFile,'r');
	dbFH=open(dbFile,'w')
	for line in iasFH:
		#print line
		fields = line.rstrip().split('\t');
		if len(fields) < 2:
			continue;
		url=fields[0];
		iasJson=fields[1];
		decJson= json.loads(iasJson);
		viab=decJson["uem"]["iviab"];
		bsc=getBrandSafety(decJson);
		db_line=url+"\t"+str(viab)+"\t"+str(bsc)+"\n";
		#print db_line
		dbFH.write(db_line);	
	return 0;

def upload_to_db( con, dbFile, table_name, maxPeriod):
	print "bdFile:"+dbFile
	#lastPos=dbFile.rfind("/");
	#tmpFile="/mnt/"+dbFile[lastPos+1:];
	#print "tmpFile:"+tmpFile
	#shutil.copyfile(dbFile,tmpFile);
	try:
		cur = con.cursor()
		#table_name = "domainViewability";
		dropTable="drop table if exists "+table_name;
		#print dropTable
		#cur.execute(dropTable);	
		createTableQuery="create table "+table_name+" like dmvSchema ";
		#print createTableQuery
		#cur.execute(createTableQuery);
		curDate=time.strftime("%Y-%m-%d");
		#curDate="01-01-13";
		loadDataQuery="load data local infile \""+dbFile+"\" into table "+table_name+" (url,viewability,brandSafety) set date=\""+curDate+"\"";
		print loadDataQuery
		ret=cur.execute(loadDataQuery);
		print "Inserted "+str(ret)+" rows for date "+ curDate
		con.commit();
		cutoffTime=int(time.time())-maxPeriod*3600*24;
		cutoffDate=time.strftime("%Y-%m-%d",time.gmtime(cutoffTime));
		purgeQuery="delete from "+table_name+" where date<=\""+cutoffDate+"\"";
		print purgeQuery;
		ret=cur.execute(purgeQuery);
		print "Deleted "+str(ret)+" rows for date "+ cutoffDate
		con.commit();
	except (IOError, mdb.Error), e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if cur:
			cur.close()
	return 0;

argParser=argparse.ArgumentParser(description='Upload Domain Viewability Data to DB');
argParser.add_argument('-iap', dest='appProperties', nargs='?', help='application properties file',required=True);
argParser.add_argument('-ipb', dest='inputPathBase', nargs='?', help='Input Path Base');
argParser.add_argument('-opb', dest='outputPathBase', nargs='?', help='Output Path Base');
argParser.add_argument('-lpb', dest='logPathBase', nargs='?', help='Log Path Base');
argParser.add_argument('-apb', dest='appPathBase', nargs='?', help='App Path Base',required=True);
args=argParser.parse_args()

appProperties=args.appProperties
appPathBase = args.appPathBase;
print "Args:"
print "appProperties:"+appProperties;
print "$APP_PATH:"+appPathBase;
try:
	p=Properties();
	p.load(open(appProperties,'r'));
	
	user_sql=p['user_sql'];	
	pwd_sql=p['pwd_sql'];
	url_sql=p['url_sql_infradb'];
	adq_db=p['adq_db'];
	dmnViewabilityTable=p['domain_viewability_table'];
	
	ias_data_file=p['output_file'].replace("$APP_PATH",appPathBase);
	db_upload_file=p['db_upload_file'].replace("$APP_PATH",appPathBase);
	max_history_period=int(p['max_history_period']);	
	print "Properties:"
	for item in p.items():
		print item;

	url=url_sql[5:];
	parsedUrl=urlparse(url);
	#print "parsedUrl="+parsedUrl
	db_host=parsedUrl.netloc
	print "db_host="+db_host+",user_sql="+user_sql+", pwd_sql="+pwd_sql+", adq_db="+adq_db
	ret=gen_db_file(ias_data_file, db_upload_file);
	if ret==0:
		con=mdb.connect(db_host, user_sql, pwd_sql, adq_db)
		ret = upload_to_db(con, db_upload_file, dmnViewabilityTable, max_history_period)
	if ret !=0:
		sys.exit(1);
except (argparse.ArgumentError), e:
	print "Error %d: %s" % (e.args[0],e.args[1])
	argParser.print_help()
	sys.exit(1)
except (IOError, mdb.Error), e:
	print "Error %d: %s" % (e.args[0],e.args[1])
	sys.exit(1)
finally:    
	print "close DB connection.";
	if con:
		con.close()

