Delivery Optimizer 0.2

Delivery optimizer fetches the delivery data for the previous 
delivery interval, allocates daily impressions optimally between
available audience segments, and sets the daily impression 
targets for RTB. The optimizer uses the rtbDelivery2 database.

1. Build the project and run tests: 

   ant all

2. Build the installer (tar):

   ant tarball -Dversion=<version>

3. Run the delivery optimizer jobs in the following order:

   a) ant update
   b) ant transfer
   c) ant optimize
   d) ant deliver
   
   The KPI data can only be entered after the transfer step (b).

4. update

   This stage update the delivery-optimizer database with the most
   resent rtb or adx data (AdWords). The adx option should not used
   after November 2013, because all data operations are based on
   the ShareThis adplatform.
   
5. transfer

   This stage transfers the impression, kpi, and eCPM data to 
   the campaignPerformances table. The default input data source 
   is RTB. Three option values are possible: imp, kpi, and all.
   
   a) ant transfer
      The default command that reads the previous day's impression
      delivery data from RTB.
      
   b) ant transfer -Ddata=rtb -Dcase=all
      This option is same as the default without the data option
      and updates both impression and kpi data.
      
   The default table for price-volume curves can be overwritten
   by the pv option:
   
   ant transfer -Dpv=<table>
   
   The command 'ant transfer' without the pv option is same as 
   the above command but with the default value: pv=priceVolumeCurves.
      
6. optimize

   This stage computes the optimal impression delivery targets
   for each ad group. The delivery targets are written to 
   the impressionTargets table. The campaignPerformances table
   is also updated for the CPA Optimizer UI. The default table
   for price-volume curves can be overwritten by the pv option:
   
   ant optimize -Dpv=<table> -Ddate=<date>
   
   The command 'ant optimize' is same as the above command but
   with the default values: pv=priceVolumeCurves and date=yesterday. 
   
7. deliver

   This stage writes the impression targets to the RTB_AdGroupDelivery
   table of the RTB database from the impressionTargets table.
   By default, the adx settings are not updated. The adx option should
   not be used because the adplatform has replaced the adx. 
   The delivery options are:
   
   ant deliver -Dadx=false -Drtb=true
   
   The default value of adx is false.
   
8. Campaign Setup

   A new campaign is added by updating the following three tables:
	
   a) A row defining the campaign specific parameters is added to 
      the campaignSettings table.
	   
   b) The campaign specific ad groups are added to the adGroupMappings
      table.
	   
   c) The ad group specific parameters are set in the adGroupSettings
      table.
      
9. Reporting

   a) Delivery Optimizer Reporting
   
   The command:
   
   ant report -Dtype=all
   
   gives the delivery reports of all active campaigns and their 
   ad groups running on RTB. The time delay is at most 10 min.
   
   Reports are also available from UI which uses a reports server.
   The server is started by the command
   
   ./targets/dors.sh
   
   b) CTR Reporting
   
   For ADX:
   
   [btdev@ads-app2 prod]$ ant ctr_report -DstartDate=20131101 -DendDate=20131130 -Dnetwork=adx
   
   ant ctr_report -DstartDate=20140901 -DendDate=20140930 -DminClk=1 -DminImp=1000 -Dnetwork=adx > adx_ctr_performance_0901_0930.txt

   or
   
   [btdev@ads-app2 prod]$ ant ctr_report -DstartDate=20131101 -DendDate=20131130
   
   For ANX:
   
   [btdev@ads-app2 prod]$ ant ctr_report -DstartDate=20131101 -DendDate=20131130 -Dnetwork=anx
  
   ant ctr_report -DstartDate=20140901 -DendDate=20140930 -DminClk=1 -DminImp=1000 -Dnetwork=anx > anx_ctr_performance_0901_0930.txt
 
   For ADX and ANX:
   
   [btdev@ads-app2 prod]$ ant ctr_report -DstartDate=20131101 -DendDate=20131130 -Dnetwork=all
   
10.Production

   The production database for Delivery Optimizer is:
   
   adopsdb1001.east.sharethis.com/rtbDelivery2
   
   It now runs on ads-app2.east.sharethis.com under the user btdev.
   The production directory is:
   
   /home/btdev/products/deliveryOptimizer/prod
   
   The step to deploy a new version of Delivery Optimizer:
   
   a) In your deliveryOptimizer2 repository directory, create the tarball:
   		
   .../adoptimization_branches/main/deliveryOptimizer2> ant tarball -Dversion=20131203r1
   		
   b) Copy sharethis.DO.20131203r1.tgz to the btdev@ads-app2 directory
   
   /home/btdev/products/deliveryOptimizer 
      
   c) Untar the deployment tarball to the prod directory:
   
   [btdev@ads-app2 deliveryOptimizer]$ tar -xvzf sharethis.DO.20131203r1.tgz -C prod
   
   d) Change the shell script permissions to executable:
   
   [btdev@ads-app2 deliveryOptimizer]$ cd prod
   [btdev@ads-app2 prod]$ chmod +x scripts/*.sh
   [btdev@ads-app2 prod]$ chmod +x targets/*.sh

   e) Kill the old Report and restart it:
   
   [btdev@ads-app2 prod]$ ps -ef | grep Report
   [btdev@ads-app2 prod]$ kill <pid for Report>
   [btdev@ads-app2 prod]$ ./targets/dors.sh
   
   This step is only needed if the adplatform UI uses the report feature.
   
11.Cron jobs

   [btdev@ads-app2 prod]$ crontab -l
   
   # Delivery optimizer master (w/imps and kpi)
   20 11 * * * /home/btdev/products/deliveryOptimizer/prod/scripts/adoptimization_master.sh > /home/btdev/products/deliveryOptimizer/logs/do_master.log 2>&1

   # Delivery optimizer update (w/kpi)
   30 0,12,15,17,21 * * * /home/btdev/products/deliveryOptimizer/prod/scripts/adoptimization_update.sh > /home/btdev/products/deliveryOptimizer/logs/do_update.log 2>&1

   # Delivery optimizer report sent to selected developers via email
   1 13,23 * * * /home/btdev/products/deliveryOptimizer/prod/scripts/adoptimization_report_developers.sh > /home/btdev/products/deliveryOptimizer/logs/do_report.log 2>&1

   # Delivery optimizer report sent to media managers via email
   0 14 * * * /home/btdev/products/deliveryOptimizer/prod/scripts/adoptimization_report_mediamanagers.sh > /home/btdev/products/deliveryOptimizer/logs/do_report.log 2>&1
