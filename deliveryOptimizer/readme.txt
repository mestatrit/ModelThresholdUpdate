Delivery Optimizer 0.1

Delivery optimizer fetches the delivery data for the previous 
delivery interval, allocates daily impressions optimally between
available audience segments, and sets the daily impression 
targets for RTB. The optimizer uses the rtbDelivery database.

1. Build the project and run tests: 

   ant all

2. Build the installer (tar):

   ant tarball -Dversion=<version>

3. Run the delivery optimizer jobs in the following order:

   a) ant update
   b) ant transfer
   c) ant optimize
   d) ant deliver
   
   The KPI data can only be entered after the transfer step (a).

4. update

   This stage update the delivery-optimizer database with the most
   resent adx data (AdWords).
   
5. transfer

   This stage transfers the impression and eCPM data to 
   the campaignPerformances table. The default input data source 
   is RTB from the priceVolumeCurves table. Two option values are
   possible:
   
   a) ant transfer
      The default command that reads the previous day's delivery
      data from the priceVolumeCurves table obtained from RTB.
      
   b) ant transfer -Ddata=rtb
      This option is same as the default without the data option.

   c) ant transfer -Ddata=adx
      This option reads the data directly from adx using AdWords APIs.
      
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
   By default, the adx settings are not updated. The adx settings are
   updated using the adx option:
   
   ant deliver -Dadx=true
   
   The default value of adx=false.
   
8. Campaign Setup

   A new campaign is added by updating the following three tables:
	
   a) A row defining the campaign specific parameters is added to 
      the campaignSettings table.
	   
   b) The campaign specific ad groups are added to the adGroupMappings
      table.
	   
   c) The ad group specific parameters are set in the adGroupSettings
      table.
      
9. Reporting

   The command:
   
   ant report
   
   gives the current delivery report of all active ad groups running
   on RTB. The time delay is at most 10 min.
   