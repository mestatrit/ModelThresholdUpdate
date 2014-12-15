#!/bin/bash

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo "install root is $install_root"

PROJ_DIR=$install_root
CONF_DIR=${PROJ_DIR}/conf
. ${CONF_DIR}/env.sh

HOST=$rtbDelivery_ip
USER=$rtbDelivery_user
PASSWORD=$rtbDelivery_pw

currentdate=`date +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
todayh=${currentdate:0:4}${currentdate:5:2}${currentdate:8:2}${currentdate:11:2}
today=${currentdate:0:4}-${currentdate:5:2}-${currentdate:8:2}
stringZ=`date --date="$today - 1 days" +%Y-%m-%d`

mysql -h${HOST} -u${USER} -p${PASSWORD} --show-warnings <<EOF

CREATE TABLE IF NOT EXISTS rtbDelivery.UserSegmentMapping (
  adgroupid bigint(20) NOT NULL,
  userlistid int(11) NOT NULL,
  createDate date NOT NULL,
  PRIMARY KEY (adgroupid, userlistid, createDate)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TEMPORARY TABLE IF NOT EXISTS rtbDelivery.temp_table1 (
  adgroupid bigint(20),
  audid int(11));

CREATE TEMPORARY TABLE IF NOT EXISTS rtbDelivery.temp_table2 (
  adgroupid bigint(20),
  label varchar(512));

INSERT INTO rtbDelivery.temp_table1
SELECT adgId, audid FROM campaignDB.adgroupAssoc where keyDate = '${stringZ}';
#SELECT max(adgId), audid FROM campaignDB.adgroupAssoc GROUP BY audid;

INSERT INTO rtbDelivery.temp_table2
SELECT a.nwAdgId AS adgroup_id, c.name AS label FROM campaignDB.adgroup a, rtbDelivery.temp_table1 b, campaignDB.audience c WHERE a.id = b.adgroupid AND b.audid=c.id;

INSERT IGNORE INTO rtbDelivery.UserSegmentMapping
SELECT distinct a.adgroupid, b.id AS userlistid, '${today}' FROM rtbDelivery.temp_table2 a, rtbDelivery.UserListId b WHERE a.label = b.label;

DELETE FROM rtbDelivery.UserSegmentMapping WHERE createDate < (ADDDATE(NOW(), INTERVAL -7 DAY));

DROP TABLE rtbDelivery.temp_table1;
DROP TABLE rtbDelivery.temp_table2;

#Logging
system echo 'number of rows in rtbDelivery.UserListId | number of rows dated ${today} in rtbDelivery.UserSegmentMapping';
SELECT COUNT(1) FROM rtbDelivery.UserListId;
SELECT COUNT(1) FROM rtbDelivery.UserSegmentMapping WHERE createDate = '${today}';

EOF
 
