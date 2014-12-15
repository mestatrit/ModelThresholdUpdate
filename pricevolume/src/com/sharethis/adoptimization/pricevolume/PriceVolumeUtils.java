package com.sharethis.adoptimization.pricevolume;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sharethis.adoptimization.adopt.PriceVolumeModel;
import com.sharethis.adoptimization.common.JdbcOperations;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;


/**
 * This is the class to run the flow.
 */

public class PriceVolumeUtils 
{	
	private static final Logger sLogger = Logger.getLogger(PriceVolumeUtils.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Configuration config = new Configuration();

	public PriceVolumeUtils(Configuration config){
		this.config=config;
	}

	public void loggingTimeUsed(long time1, long time2, String taskName){			
		double time_used = (time2-time1)/(1000.0);							
		sLogger.info("The time to run " + taskName
	    		+ " : " + time_used + " seconds.");
	}
		
	public String getPDateStr(Date lastRun, int dayInterval) throws Exception{		
		String processDateStr = null;
		Date pDate = null;
		int delayDay = config.getInt("DelayDay", 2);					
		try{
			if((config.get("ProcessingDate")==null)||(config.get("ProcessingDate").isEmpty())) {
				if(lastRun != null){
					pDate = DateUtils.addDays(lastRun, dayInterval);
					sLogger.info("\nThe last run date: " + lastRun 
							+ "\nThe processing date: " + pDate);
				}else{
					Date today = new Date();
					pDate = DateUtils.addDays(today, -delayDay);
				}
				processDateStr = sdf.format(pDate);
			}else{
				processDateStr = config.get("ProcessingDate");
			}
		}catch(Exception e){
			throw new Exception(e);
		}
		return processDateStr;
	}
	
	public Date getAdminDate(String poolName, String key) throws Exception	    
	{	        
		Connection con = null;	        
		Date d = null;	        
		try{	            
			con = new JdbcOperations(config, poolName).getConnection();	            
			d = getValue(con, key);	        
		}catch(SQLException e){	            
			sLogger.info(e.toString());	       
			throw new Exception(e);	        
		}
		finally{
			con.close();
		}	       
		return d;	    
	}
	
	private java.util.Date getValue(Connection con, String name) throws SQLException, Exception {
		java.util.Date date = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.prepareStatement("select value from admin_date where name=?");
			stmt.setString(1, name);
			rs = stmt.executeQuery();
			if (rs!=null && rs.next()) {
				date = rs.getTimestamp("value");
			}else{
				date = null;
			}
		}
		finally {
			rs.close();
			stmt.close();
		}
		return date;
	}

	private static boolean updateValue(Connection con, String name, java.util.Date date)
		throws SQLException {
		int cnt = 0;
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement("update admin_date set value=? where name=?");
			stmt.setTimestamp(1, new Timestamp(date.getTime()));
			stmt.setString(2, name);
			cnt = stmt.executeUpdate();
		}
		finally {
			stmt.close();
		}
		return cnt > 0;
	}

	private static boolean insertValue(Connection con, String name, java.util.Date date)
		throws SQLException {
		int cnt = 0;
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement("insert admin_date set value=?, name=?");
			stmt.setTimestamp(1, new Timestamp(date.getTime()));
			stmt.setString(2, name);
			cnt = stmt.executeUpdate();
		}
		finally {
			stmt.close();
		}
		return cnt > 0;
	}
	
	public void upsertAdminDate(String poolName, String key, Date d) throws Exception	    
	{	        
		Connection con = null;	        
		try{	            
			con = new JdbcOperations(config, poolName).getConnection();	   
			if (getValue(con, key)!= null)
				updateValue(con, key, d);	   
			else
				insertValue(con, key, d);
		}catch(SQLException e){	            
			sLogger.info(e.toString());	            
			throw new Exception(e);	        
		}	        
		finally{	            
			con.close();	        
		}	    
	}
	
	public boolean checkingBlockDates(String processDateStr, String requiredTasks,
			String poolSources, String offsetDays) throws Exception{
		try{
			Date pDate = sdf.parse(processDateStr);	
			String[] requiredTasksArr = null; 
			if (StringUtils.isNotBlank(requiredTasks)) {
				requiredTasksArr = requiredTasks.split(",");
			}

			String[] poolSourcesArr = null;
			if (StringUtils.isNotBlank(poolSources)) {
				poolSourcesArr = poolSources.split(",");
			}

			String[] offsetDaysArr = null;
			if (StringUtils.isNotBlank(offsetDays)) {
				offsetDaysArr = offsetDays.split(",");
			}

			if ((requiredTasksArr != null) && (poolSourcesArr != null) && (offsetDaysArr != null)
					&&(requiredTasksArr.length == poolSourcesArr.length)
					&&(requiredTasksArr.length == offsetDaysArr.length)){
				Connection con = null;
				for (int i = 0; i < requiredTasksArr.length; ++i) {
					con = new JdbcOperations(config, poolSourcesArr[i]).getConnection();	
					Date lastRun = DateUtils.addDays(getValue(con, requiredTasksArr[i]), 
							-Integer.parseInt(offsetDaysArr[i]));
					if (lastRun != null && pDate.after(lastRun)) {
						sLogger.info("Stopping the process since the processing date " + 
								processDateStr + " is after the last run of " +
								requiredTasksArr[i] + " on " + sdf.format(lastRun));
						return true;
					}
				}
				con.close();
			}else{
				sLogger.warn("The run parameters to check the admin_dates are incorrectly set up.");
				sLogger.warn("Checking admin_dates of the tables is ignored.");
			}
		}catch(Exception e){
			throw new Exception(e);
		}
		return false;
	}

	public boolean checkingBlockDates(String processDateStr, String requiredTasks,
			String poolSources, String offsetDays, String csDays) throws Exception{
		try{
			Date pDate = sdf.parse(processDateStr);	
			String[] requiredTasksArr = null; 
			if (StringUtils.isNotBlank(requiredTasks)) {
				requiredTasksArr = requiredTasks.split(",");
			}

			String[] poolSourcesArr = null;
			if (StringUtils.isNotBlank(poolSources)) {
				poolSourcesArr = poolSources.split(",");
			}

			String[] offsetDaysArr = null;
			if (StringUtils.isNotBlank(offsetDays)) {
				offsetDaysArr = offsetDays.split(",");
			}

			String[] csDaysArr = null;
			if (StringUtils.isNotBlank(csDays)) {
				csDaysArr = csDays.split(",");
			}

			if ((requiredTasksArr != null) && (poolSourcesArr != null) && (offsetDaysArr != null)
					&&(requiredTasksArr.length == poolSourcesArr.length)
					&&(requiredTasksArr.length == offsetDaysArr.length)){
				Connection con = null;
				for (int i = 0; i < requiredTasksArr.length; ++i) {
					con = new JdbcOperations(config, poolSourcesArr[i]).getConnection();	
					Date lastRun = DateUtils.addDays(getValue(con, requiredTasksArr[i]), 
							-Integer.parseInt(offsetDaysArr[i])-Integer.parseInt(csDaysArr[i]));
					if (lastRun != null && pDate.after(lastRun)) {
						sLogger.info("Stopping the process since the processing date " + 
								processDateStr + " is after the last run of " +
								requiredTasksArr[i] + " on " + sdf.format(lastRun));
						return true;
					}
				}
				con.close();
			}else{
				sLogger.warn("The run parameters to check the admin_dates are incorrectly set up.");
				sLogger.warn("Checking admin_dates of the tables is ignored.");
			}
		}catch(Exception e){
			throw new Exception(e);
		}
		return false;
	}
		
	public boolean doesFileExist(String fileName) throws Exception{
		Scheme fScheme = new TextLine();
		Tap fTap = new Hfs(fScheme, fileName);
		JobConf fJobConf = new JobConf();
		if(fTap.resourceExists(fJobConf)){
			sLogger.info("The file: " + fileName + " exists.");
			return true;
		}else{
			sLogger.info("The file: " + fileName + " does not exist.");
			return false;
		}
	}
	
	public boolean isEmptyFile(String fileName) throws Exception{
		Scheme fScheme = new TextLine();
		Tap fTap = new Hfs(fScheme, fileName);
		JobConf fJobConf = new JobConf();
		FlowProcess<?> flowProcess = new HadoopFlowProcess(fJobConf);
		TupleEntryIterator teIter = fTap.openForRead(flowProcess);
		if(teIter.hasNext()){
			sLogger.info("The file: " + fileName + " is not empty.");
			return false;
		}else{
			sLogger.info("The file: " + fileName + " is empty.");
			return true;
		}
	}
	
	public String getDataFieldStr(String fileName) throws Exception{
		Scheme fScheme = new TextLine();
		Tap fTap = new Hfs(fScheme, fileName);
		JobConf fJobConf = new JobConf();
		FlowProcess<?> flowProcess = new HadoopFlowProcess(fJobConf);
		TupleEntryIterator teIter = fTap.openForRead(flowProcess);
		String fieldStr = null;
		if(teIter.hasNext()){
			fieldStr = ((teIter.next()).getTuple()).toString();		
			// Removing the first filed '0'
			fieldStr = fieldStr.replaceAll("0\t", "");					
		}else{
			sLogger.info("The file: " + fileName + " is empty.");
		}
		return fieldStr;
	}

	public void deleteFile(String fileName) throws Exception{
		Scheme fScheme = new TextLine();
		Tap fTap = new Hfs(fScheme, fileName);
		JobConf fJobConf = new JobConf();
		if(fTap.resourceExists(fJobConf)){
			fTap.deleteResource(fJobConf);
			sLogger.info("Deleted the data file: " + fileName);
		}
	}

	public void makeFileDir(String filePath) throws Exception{
		JobConf pJobConf = new JobConf();
		Scheme pScheme = new TextLine();
		Tap pTap = new Hfs(pScheme, filePath);
		if(!pTap.resourceExists(pJobConf)){
			pTap.createResource(pJobConf);
			sLogger.info("Made the data file path: " + filePath);
		}
	}
	
	public void deleteFilesByDate(String filePath, String pDateStr, String fileName) 
		throws Exception{
		String fileNamePath = filePath + pDateStr + "/" + fileName;
		deleteFile(fileNamePath);
	}

	public void deleteFilesBeforeDate(String filePath, String pDateStr, String fileName, 
			int daysKept, int daysDel) throws Exception{
		if(daysKept<=0) daysKept = 0;
		Date sDate = DateUtils.addDays(sdf.parse(pDateStr), -1-daysKept-daysDel);
		for(int i = 0; i < daysDel; i++){
			Date iDate = DateUtils.addDays(sDate, i);
			String iDateStr = sdf.format(iDate);
			String fileNamePath = filePath + iDateStr + "/" + fileName;
			deleteFile(fileNamePath);
		}
	}
	
	public void updatingDate(String poolName, String tableName, String columnName, String pDateStr) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "update " + tableName + " set " + columnName + " = ?"
			+ " where " + columnName + " is null";
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}

	public void updatingColumns(String poolName, String tableName, String columnName, String pDateStr, 
			String columnName1, int numOfDays) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "update " + tableName + " set " + columnName + " = ?, " 
						+ columnName1 + " = " + numOfDays 
						+ " where " + columnName + " is null";
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}

	public void updatingBidDataIntoWinTable(String poolName, String winTableName, String bidTableName, String pDateStr) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Start updating the bid data into the win data table ...\n");	
			String query = "update " + winTableName + " a, " + bidTableName + " b set a.sum_bids = b.sum_bids, " +
					"a.sum_nobids = b.sum_nobids where a.adgroup_id = b.adgroup_id and a.creative_id = b.creative_id " +
					"and a.domain_name = b.domain_name and a.num_days = b.num_days and a.date_=b.date_ and a.date_ = ? ";
			sLogger.info("UpdatingBidDataIntoWinTable query: " + query);
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
			sLogger.info("Updating the bid data into the win data table is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	public void updatingWinDataIntoBidTable(Configuration config) throws SQLException, ParseException{
		String pDateStr = config.get("ProcessingDate");
		String poolName = config.get("PoolName");
		sLogger.info("Export to MySql Pool Name: " + poolName);						        
		String bidTableName = config.get("BidTableName");
		sLogger.info("Bid Table Name: " + bidTableName);			
		String winTableName = config.get("WinTableNameUI");
		updatingWinDataIntoBidTable(poolName, winTableName, bidTableName, pDateStr);
	}
	
	private void updatingWinDataIntoBidTable(String poolName, String winTableName, String bidTableName, 
			String pDateStr) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Start updating the win data into the bid data table ...\n");	
			String aggQuery = "(select adgroup_id, creative_id, domain_name, num_days, date_, sum(sum_wins) as sum_wins, " +
					"sum(sum_mwins) as sum_mwins, ROUND(sum(winning_price*sum_wins)/sum(sum_wins),2) as e_cpm from " + winTableName +
					" where date_ = ? group by adgroup_id, creative_id, domain_name, num_days, date_) b" ;					
					
			String query = "update " + bidTableName + " a, " + aggQuery + " set a.sum_wins = b.sum_wins, " +
						"a.sum_mwins = b.sum_mwins, a.e_cpm = b.e_cpm where a.adgroup_id = b.adgroup_id " +
						"and a.creative_id = b.creative_id and a.domain_name = b.domain_name and a.num_days = b.num_days " +
						"and a.date_ = b.date_";
			sLogger.info("UpdatingWinDataIntoBidTable query: " + query);
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
			sLogger.info("Updating the win data into the bid data table is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	public void updatingInvDataIntoBidTable(Configuration config, int invSource) throws SQLException, ParseException, Exception{
		String pDateStr = config.get("ProcessingDate");
		String poolName = config.get("PoolName");
		sLogger.info("Export to MySql Pool Name: " + poolName);						        
		String bidTableName = config.get("BidTableName");
		sLogger.info("Bid Table Name: " + bidTableName);	
		sLogger.info("Inventory Source: " + invSource);			
		if(invSource==0){
			String invTableNameNB = config.get("InvTableNameNB");
			updatingInvDataIntoBidTable(poolName, bidTableName, invTableNameNB, pDateStr);
		}else{
			String invPoolName = config.get("InvPoolName");
			String invTableName = config.get("InvTableName");
			updatingInvDataIntoBidTable(poolName, bidTableName, invPoolName, invTableName, pDateStr);
		}
	}

	private void updatingInvDataIntoBidTable(String poolName, String bidTableName, String invPoolName, 
			String invTableName, String pDateStr)
		throws SQLException, ParseException, Exception{
		
		sLogger.info("InvPoolName: " + invPoolName + "   InvTableName: " + invTableName);
		Date pDate = sdf.parse(pDateStr);
		Connection con = new JdbcOperations(config, poolName).getConnection();	
		Connection con1 = new JdbcOperations(config, invPoolName).getConnection();	            
		PreparedStatement stmt = null;
		PreparedStatement stmt3 = null;
		ResultSet rs = null;
		try {
			sLogger.info("Start updating inventory data into the bid data table ...\n");	
			computingInvRatio(con1, invTableName, pDate);
			long sum_reqs = 0;			
			String adgroupQuery = "select adgroup_id from " + bidTableName + 
					" where domain_name='-1' and creative_id=-1 and num_days=1 and date_ = ?";
			
			stmt = con.prepareStatement(adgroupQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
//			sLogger.info("Query to get the list of adgroups: " + stmt.toString());
			rs = stmt.executeQuery();
			if(rs!=null){
				while (rs.next()) {
					long adgroupId = Long.parseLong(rs.getString("adgroup_id"));
					sum_reqs = getDayInventoryFrom5Mins(con1, invTableName, adgroupId, pDate)[0];
//					sum_reqs = getDayTotalInventoryFrom5Mins(con1, invTableName, adgroupId, pDate)[0];
					String query = "update " + bidTableName + " set sum_nobids = " + sum_reqs +
							" where adgroup_id = " + adgroupId + " and creative_id = -1 and " +
							"domain_name = '-1' and num_days = 1 and date_ = ?";
//					sLogger.info("update query: " + query);
					PreparedStatement stmt2 = null;
					stmt2 = con.prepareStatement(query);
					stmt2.setTimestamp(1, new Timestamp(pDate.getTime()));
//					sLogger.info("UpdatingInvDataIntoBidTable query: " + stmt2.toString());
					stmt2.executeUpdate();
					stmt2.close();
				}		
			}else{
				sLogger.warn("There is no data in " + bidTableName + " on " + pDateStr);
			}	
			
			Date initDate = DateUtils.addDays(pDate,  -7);
			String aggQuery = "(select adgroup_id, creative_id, domain_name, sum(sum_nobids) as sum_nobids " +
					"from " + bidTableName + " where creative_id = -1 and domain_name = '-1' and num_days = 1 " +
					"and date_ > ? and date_<= ? group by adgroup_id, creative_id, domain_name) b" ;					
					
			String query = "update " + bidTableName + " a, " + aggQuery + " set a.sum_nobids = b.sum_nobids " +
						"where a.adgroup_id = b.adgroup_id and a.creative_id = b.creative_id and " +
						"a.domain_name = b.domain_name and a.num_days = 7 and a.date_ = ?";
//			sLogger.info("UpdatingInvDataIntoBidDataTable query: " + query);
			stmt3=con.prepareStatement(query);
			stmt3.setTimestamp(1, new Timestamp(initDate.getTime()));
			stmt3.setTimestamp(2, new Timestamp(pDate.getTime()));
			stmt3.setTimestamp(3, new Timestamp(pDate.getTime()));
			stmt3.executeUpdate();			
			stmt3.close();
			sLogger.info("Updating inventory data into the bid data table is done.\n");	
			
		}finally {
			rs.close();
			stmt.close();
			con1.close();
			con.close();
		}
	}

	private void updatingInvDataIntoBidTable(String poolName, String bidTableName, String invTableName, String pDateStr)
		throws SQLException, ParseException, Exception{
		
		sLogger.info("InvPoolName: " + poolName + "   InvTableName: " + invTableName);
		Date pDate = sdf.parse(pDateStr);
		String resList = config.get("ResList");
		Connection con = new JdbcOperations(config, poolName).getConnection();	
		PreparedStatement stmt = null;
		PreparedStatement stmt3 = null;
		ResultSet rs = null;
		try {
			sLogger.info("Start updating inventory data into the bid data table ...\n");	
			long sum_reqs = 0;			
			String adgroupQuery = "select adgroup_id from " + bidTableName + 
					" where domain_name='-1' and creative_id=-1 and num_days=1 and date_ = ?";
			
			stmt = con.prepareStatement(adgroupQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			sLogger.info("Query to get the list of adgroups: " + stmt.toString());
			rs = stmt.executeQuery();
			if(rs!=null){
				while (rs.next()) {
					long adgroupId = Long.parseLong(rs.getString("adgroup_id"));
					sum_reqs = Math.round(getDayInventoryFromNoBid(con, invTableName, resList, adgroupId, pDate));
					String query = "update " + bidTableName + " set sum_nobids = " + sum_reqs +
							" where adgroup_id = " + adgroupId + " and creative_id = -1 and " +
							"domain_name = '-1' and num_days = 1 and date_ = ?";
					PreparedStatement stmt2 = null;
					stmt2 = con.prepareStatement(query);
					stmt2.setTimestamp(1, new Timestamp(pDate.getTime()));
//					sLogger.info("UpdatingInvDataIntoBidTable query: " + stmt2.toString());
					stmt2.executeUpdate();
					stmt2.close();
				}		
			}else{
				sLogger.warn("There is no data in " + bidTableName + " on " + pDateStr);
			}	
			
			Date initDate = DateUtils.addDays(pDate,  -7);
			String aggQuery = "(select adgroup_id, creative_id, domain_name, sum(sum_nobids) as sum_nobids " +
					"from " + bidTableName + " where creative_id = -1 and domain_name = '-1' and num_days = 1 " +
					"and date_ > ? and date_<= ? group by adgroup_id, creative_id, domain_name) b" ;					
					
			String query = "update " + bidTableName + " a, " + aggQuery + " set a.sum_nobids = b.sum_nobids " +
						"where a.adgroup_id = b.adgroup_id and a.creative_id = b.creative_id and " +
						"a.domain_name = b.domain_name and a.num_days = 7 and a.date_ = ?";
//			sLogger.info("UpdatingInvDataIntoBidDataTable query: " + query);
			stmt3=con.prepareStatement(query);
			stmt3.setTimestamp(1, new Timestamp(initDate.getTime()));
			stmt3.setTimestamp(2, new Timestamp(pDate.getTime()));
			stmt3.setTimestamp(3, new Timestamp(pDate.getTime()));
			stmt3.executeUpdate();			
			stmt3.close();
			sLogger.info("Updating inventory data into the bid data table is done.\n");	
			
		}finally {
			rs.close();
			stmt.close();
			con.close();
		}
	}
	
	public void deleteData(String poolName, String tableName) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "delete from " + tableName;
			stmt = con.prepareStatement(query);
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	public void deleteDataByDate(String poolName, String tableName, String columnName, String pDateStr) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "delete from " + tableName + " where " + columnName + " = ?";
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	public void deleteDataByDate(String poolName, String tableName, String columnName, String pDateStr, String[] keyFields) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		String whKeyFields = null;
		if (keyFields !=null && keyFields.length>0){
			whKeyFields = keyFields[0] + " <> -1";
			for(int i=1; i<keyFields.length; i++)
				whKeyFields = whKeyFields + " and " + keyFields[i] + " <> -1";
		}
		try {
			String query = "delete from " + tableName + " where " + columnName + " = ? and " + whKeyFields;
			sLogger.info("delete the data query: " + query);
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}

	public void deleteDataByDate(String poolName, String tableName, String columnName, String pDateStr, String[] keyFields, int numOfDays) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		String whKeyFields = null;
		if (keyFields !=null && keyFields.length>0){
			whKeyFields = keyFields[0] + " <> -1";
			for(int i=1; i<keyFields.length; i++)
				whKeyFields = whKeyFields + " and " + keyFields[i] + " <> -1";
		}
		try {
			String query = "delete from " + tableName + " where " + columnName + " = ? and " + whKeyFields + " and num_days = " + numOfDays;
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			sLogger.info("delete the data query: " + stmt.toString());
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	public void deleteDataBeforeDate(String poolName, String tableName, String columnName, String pDateStr, int daysKept) 
		throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "delete from " + tableName + " where " + columnName + " <= ?";
			Date pDate = DateUtils.addDays(sdf.parse(pDateStr), -1-daysKept);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}

	public void createTable(String poolName, String tableName) throws IOException, SQLException {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement pstmt1 = null;
		try{
			String createTableQuery = "CREATE TABLE " + tableName + " ( " + 
					" adgroup_id bigint(30) default '-1', " +
					" creative_id bigint(30) default '-1', " +
					" domain_name varchar(120) default '-1', " +
					" winning_price double default '0', " +
					" sum_wins double default '0', " +
					" sum_mwins double default '0', " +
					" cum_wins double default '0', " +
					" num_days int(5) default '-1', " +
					" date_ date default NULL, " +
					" sum_bids double default '0', " +
					" sum_nobids double default '0', " +
					" primary key (adgroup_id, creative_id, domain_name, winning_price, num_days, date_) " + 
					" ) ENGINE=MyISAM";
			pstmt1 = con.prepareStatement(createTableQuery);
			pstmt1.execute();
		} catch (SQLException se){
			sLogger.error(se);
			throw se;
		} finally {
			pstmt1.close();				 
			con.close();					
		}
	}

	public void createBidTable(String poolName, String tableName) throws IOException, SQLException {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement pstmt1 = null;
		try{
			String createTableQuery = "CREATE TABLE " + tableName + " ( " + 
					" adgroup_id bigint(30) default '-1', " +
					" creative_id bigint(30) default '-1', " +
					" domain_name varchar(120) default '-1', " +
					" sum_bids double default '0', " +
					" sum_nobids double default '0', " +
					" num_days int(5) default '-1', " +
					" date_ date default NULL, " +
					" sum_wins double default '0', " +
					" sum_mwins double default '0', " +
					" e_cpm double default '0', " +
					" primary key (adgroup_id, creative_id, domain_name, num_days, date_) " + 
					" ) ENGINE=MyISAM";
			pstmt1 = con.prepareStatement(createTableQuery);
			pstmt1.execute();
		} catch (SQLException se){
			sLogger.error(se);
			throw se;
		} finally {
			pstmt1.close();				 
			con.close();					
		}
	}
	
	private int num_days_max = 1;
	private List<String> adgroupL = new ArrayList<String>();
	private List<String> adgroupNameL = new ArrayList<String>();
	
	private void getAllAdgroups(String poolName, String bidTableName, String pDateStr) 
			throws SQLException, ParseException{
		adgroupL = new ArrayList<String>(0);
		adgroupNameL = new ArrayList<String>(0);
		Date pDate = sdf.parse(pDateStr);
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String select_num_days = "select max(num_days) as num_days_max from " + bidTableName + " where date_ = ?";	
			stmt = con.prepareStatement(select_num_days);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			rs = stmt.executeQuery();
			if (rs!=null && rs.next()) {
				num_days_max = rs.getInt("num_days_max");
			}

			String selectQuery = "select distinct(adGroupId), adGroupName from adGroupMappings";	
			
//			String selectQuery = "select distinct(adgroup_id) as adgroup_id from " + winTableName +
//					" where date_ = ? and num_days = " + num_days_max;	
	//		sLogger.info("Query to get the list of adgroups: " + selectQuery);
			stmt = con.prepareStatement(selectQuery);
//			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			rs = stmt.executeQuery();
			if(rs!=null){
				int i_cnt = 0;
				while (rs.next()) {
					adgroupL.add(i_cnt, rs.getString("adGroupId"));
					adgroupNameL.add(i_cnt, rs.getString("adGroupName"));
					i_cnt++;
				}
			}else{
				sLogger.warn("There is no data in adGroupMappings");
			}
		}
		finally {
			rs.close();
			stmt.close();
			con.close();
		}		
	}

	private void insertingOneIntoPriceVolumeCurve(Connection con, Connection con1, String winTableName, 
			String bidTableName, String modelTableName, String priceVolumeCurve, String pDateStr, 
			long adgroupId, String adGroupName, boolean PVDailyFlag, int invSource) 
		throws SQLException, ParseException, JSONException, Exception{
		Date pDate = sdf.parse(pDateStr);
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String invTableNameNB= config.get("InvTableNameNB");
		String invTableName= config.get("InvTableName");
		PriceVolumeModel pvMod = new PriceVolumeModel(con, modelTableName);
		try {
			int numOfDays = 1;
			if(!PVDailyFlag)
				numOfDays = num_days_max;
			String selectWinQuery = "select winning_price, sum_wins/num_days as sum_wins, " +
					"cum_wins/num_days as cum_wins, sum_bids from " + winTableName +
					" where date_ = ? and num_days = " + numOfDays + " and adgroup_id = " + adgroupId +
					" and creative_id = -1 and domain_name = '-1' order by winning_price";	
			stmt = con.prepareStatement(selectWinQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
//			sLogger.info("Select win data query: " + stmt.toString());
			rs = stmt.executeQuery();
			List<Double> winning_price = new ArrayList<Double>();
			List<Double> sum_wins = new ArrayList<Double>();
			List<Double> cum_wins = new ArrayList<Double>();
			List<Double> win_rate = new ArrayList<Double>();
			long impressionDelivery = 0;
			double sum_cost_total = 0;
			double max_cum_wins = -1;
//			String cumulative = null;
//			String histogram = null;
			int i_cnt = 0;
			if(rs!=null){
				while (rs.next()) {
					winning_price.add(i_cnt, rs.getDouble("winning_price"));
					sum_wins.add(i_cnt, rs.getDouble("sum_wins"));
					double cum_wins_tmp = rs.getDouble("cum_wins");
					cum_wins.add(i_cnt, cum_wins_tmp);
					win_rate.add(i_cnt, cum_wins_tmp/rs.getDouble("sum_bids"));
					if(max_cum_wins<cum_wins_tmp)
						max_cum_wins = cum_wins_tmp;
					i_cnt++;
				}
			}else{
				sLogger.warn("It is null for the adgroup: " + adgroupId);
			}
			
			double sum_reqs = 0;
			if(i_cnt > 0){
			
				int num_points = winning_price.size();
				impressionDelivery = Math.round(max_cum_wins);
				sum_cost_total = 0;
				for(int i = 0; i < num_points; i++){
//					sLogger.info("i: " + i + "   winning_price: " + winning_price.get(i) + 
//							"   sum_wins: " + sum_wins.get(i) + "   cum_wins: " + cum_wins.get(i));
					if(sum_wins.get(i)>0)
						sum_cost_total = sum_cost_total + sum_wins.get(i) * winning_price.get(i);
				}
				//Select the capacity for the adgroup_id
				String selectBidQuery = "select (sum_bids+sum_nobids) as sum_reqs from " + bidTableName +
						" where date_ = ? and num_days = 1 and adgroup_id = " + adgroupId +
						" and creative_id = -1 and domain_name = '-1'";	
				stmt = con.prepareStatement(selectBidQuery);
				stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
// 				sLogger.info("Select bid data query: " + stmt.toString());
				rs = stmt.executeQuery();
				while (rs.next())
					sum_reqs = rs.getDouble("sum_reqs");
			}else{
				sLogger.warn("There is no win data for the adgroup: " + adgroupId);
				if(invSource==0){
					String resList = config.get("ResList");
					sum_reqs = getDayInventoryFromNoBid(con, invTableNameNB, resList, adgroupId, pDate);
				}else
					sum_reqs = getDayInventoryFrom5Mins(con1, invTableName, adgroupId, pDate)[1];
			}
			pvMod.getOneModel(adgroupId, "-1", -1, numOfDays, null);
			int modelType = pvMod.getModelType();
			double beta0 = pvMod.getBeta0();
			double beta1 = pvMod.getBeta1();
			double rSquare = pvMod.getRSquare();
			if(modelType!=-99){
				double ecpmDelivery = 0;
				if (impressionDelivery > 0)
					ecpmDelivery = Math.round(sum_cost_total/impressionDelivery*100)/100.0;
									
				String selectMaxCpm = "select maxCpm from adGroupSettings where adGroupId = " + adgroupId;
				stmt = con.prepareStatement(selectMaxCpm);
				rs = stmt.executeQuery();
				double maxCpm = 0;
				while(rs.next())
					maxCpm = rs.getDouble("maxCpm");
				double wr_maxCpm = pvMod.getWinRate(maxCpm);
				double impressionMaxCpm = Math.min(Math.round(sum_reqs*wr_maxCpm), Long.MAX_VALUE);
					
				beta0 = Math.round(beta0*1000000)/1000000.0;
				beta1 = Math.round(beta1*1000000)/1000000.0;
				rSquare = Math.round(rSquare*1000000)/1000000.0;
//				sLogger.info("priceThreshold: " + priceThreshold);
//				sLogger.info("impressionThreshold: " + impressionThreshold);
//				sLogger.info("maxCpm: " + maxCpm);
//				sLogger.info("impressionMaxCpm: " + impressionMaxCpm);
					
				// insert into priceVolumeCurve table.												
				String insertQuery = "insert " + priceVolumeCurve + " set adGroupId = " + adgroupId +
						", adGroupName = '" + adGroupName + "', impressionDelivery = " + impressionDelivery + 
						", ecpmDelivery = " + ecpmDelivery + ", maxCpm = " + maxCpm + 
						", impressionMaxCpm = " + impressionMaxCpm + ", beta0 = " + beta0 +
						", beta1 = " + beta1 + ", model_type = " + modelType + ", r_square = " + rSquare +
						", updateDate = ? " + ", totalInventory = " + sum_reqs;
				
				stmt = con.prepareStatement(insertQuery);
				stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
// 				sLogger.info("inserting price volume curve query: " + stmt.toString());
				stmt.executeUpdate();	
			}
		}
		finally {
			rs.close();
			stmt.close();
		}
	}

	public void insertingPriceVolumeCurve(Configuration config, int invSource) throws SQLException, ParseException, Exception{
		String pDateStr = config.get("ProcessingDate");
		String poolName = config.get("PoolName");
		sLogger.info("Export to MySql Pool Name: " + poolName);						        
		String winTableName = config.get("WinTableNameUI");
		sLogger.info("Win Table Name: " + winTableName);
		String bidTableName = config.get("BidTableName");
		sLogger.info("Bid Table Name: " + bidTableName);			
		String modelTableName = config.get("ModelTableName");
		String priceVolumeCurves = config.get("PriceVolumeCurve");
		sLogger.info("Price Volume Curve Table Name: " + priceVolumeCurves);			
		boolean PVDailyFlag = config.getBoolean("PVDailyFlag", true);
		insertingPriceVolumeCurve(poolName, winTableName, bidTableName, 
				modelTableName, priceVolumeCurves, pDateStr, PVDailyFlag, invSource);
	}
	
	private void insertingPriceVolumeCurve(String poolName, String winTableName, String bidTableName, 
			String modelBaseTable, String priceVolumeCurve, String pDateStr, boolean PVDailyFlag, int invSource) 
		throws SQLException, ParseException, Exception{
		try {			
			String invPoolName = config.get("InvPoolName");
			Connection con = new JdbcOperations(config, poolName).getConnection();	            
			Connection con1 = new JdbcOperations(config, invPoolName).getConnection();
			deleteData(poolName, priceVolumeCurve);
			getAllAdgroups(poolName, bidTableName, pDateStr);
			sLogger.info("The number of adgroups: " + adgroupL.size());
			if(adgroupL.size()>0){
				for(int i=0; i<adgroupL.size(); i++){
					long adgroupId = Long.parseLong(adgroupL.get(i));
					String adgroupName = adgroupNameL.get(i);
//					sLogger.info("i: " + i + "   adGroupId: " + adgroupId);
//					sLogger.info("i: " + i + "   adGroupName: " + adgroupName);
					insertingOneIntoPriceVolumeCurve(con, con1, winTableName, bidTableName, modelBaseTable, 
							priceVolumeCurve, pDateStr, adgroupId, adgroupName, PVDailyFlag, invSource);
				}
			}else{
				sLogger.warn("There is no adGroupId in adGroupMappings, therefore, no insert into priceVolumeCurves.");
			}
			con.close();
			con1.close();
		}catch(Exception ex){
			throw new Exception(ex);
		}
	}

    private String countToString(List<Double> winning_price, List<Double> cnt_wins, 
    		String cntName) throws JSONException {    	
        JSONObject cnt = new JSONObject();
        JSONArray wPrice = new JSONArray();
        JSONArray cntWins = new JSONArray();
        int num_points = winning_price.size();
        double price_min = winning_price.get(0);
        double price_max = price_min;
        for (int i = 0; i < num_points; i++) {
        	double val_temp = winning_price.get(i);
        	if(price_min > val_temp) 
        		price_min = val_temp;
        	if(price_max < val_temp)
        		price_max = val_temp;
        	wPrice.put(val_temp);
        	cntWins.put(Math.round((double) cnt_wins.get(i)));
        }
        cnt.put("num_points", winning_price.size());
        cnt.put("min_price", price_min);
        cnt.put("max_price", price_max);
        cnt.put("winning_price", wPrice);
        cnt.put(cntName, cntWins);
        return cnt.toString();
    }	

	public Map<String, List<String>> adgroupUserMapping() 
			throws SQLException, ParseException, Exception{
		String poolName = "rtbDelivery";
		String userSegTable = "UserSegmentMapping";
		sLogger.info("Entering to get the mapping ...");						
		poolName = config.get("PoolName", poolName);
		sLogger.info("Export to MySql Pool Name: " + poolName);		
		userSegTable = config.get("UserSegmentTable", userSegTable);
		sLogger.info("User Segment ID Table: " + userSegTable);		
					        
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Map<String, List<String>> adgroupUserMap = new HashMap<String, List<String>>();
		try {
			
			String selectDateQuery = "select max(createDate) as max_date from " + userSegTable;
			sLogger.info("Select max date query: " + selectDateQuery);
			stmt = con.prepareStatement(selectDateQuery);
			rs = stmt.executeQuery();
			String mappingDateStr = null;
			if(rs!=null){
				while (rs.next()) {
					mappingDateStr = rs.getString("max_date");
				}
			}
			sLogger.info("The latest date in " + userSegTable + " is " + mappingDateStr);
			if(mappingDateStr!=null){
				Date mappingDate = sdf.parse(mappingDateStr);
			
				String selectMapQuery = "select adgroupid, userlistid from " + userSegTable + " where createDate = ? ";	
				sLogger.info("Select mapping data query: " + selectMapQuery);
				stmt = con.prepareStatement(selectMapQuery);
				stmt.setTimestamp(1, new Timestamp(mappingDate.getTime()));
				rs = stmt.executeQuery();
				List<String> userIdList = new ArrayList<String>();
				if(rs!=null){
					while (rs.next()) {
						int i_cnt = 0;
						String adgroupId = rs.getString("adgroupid");
						String userId = rs.getString("userlistid");
						//sLogger.info("adgroupId: " + adgroupId + "   userId: " + userId);
						if (adgroupUserMap.containsKey(adgroupId)) {
							userIdList = (List<String>) adgroupUserMap.get(adgroupId);
							i_cnt = userIdList.size();
						}else{
							userIdList = new ArrayList<String>();
							i_cnt = 0;						
						}
						userIdList.add(i_cnt, userId);
						adgroupUserMap.put(adgroupId, userIdList);
					}
				}else{
					sLogger.warn("It is null for the mapping of adgroup and user");
				}				
			}else{
				sLogger.warn("*************The table: " + userSegTable + " is empty****************");
			}
			return adgroupUserMap;
		}
		finally {
			rs.close();
			stmt.close();
			con.close();
		}
	}    

	private long[] getDayTotalInventoryFrom5Mins(Connection con1, String invTableName, 
			long adgroupId, Date pDate) throws SQLException, ParseException, Exception{

		long sum_nobids = 0;
		long sum_bids = 0;
		PreparedStatement stmt1 = null;
		ResultSet rs1 = null;
		try{
			Date startDate = DateUtils.addMinutes(pDate, 478);
			long startTime = startDate.getTime();
			Date endDate = DateUtils.addDays(startDate, 1);
			long endTime = endDate.getTime();
			String selectInvQuery = " select sum(Success) as sum_bids, " +
					"sum(Admin+dailyDeliveryCap+DeliveryFrequencyCap) as sum_nobids from " + 
					invTableName + " where adGroupId = " + adgroupId + " and " +
					"createDateStart >= ? and createDateStart < ? ";
			
			stmt1 = con1.prepareStatement(selectInvQuery);
			stmt1.setTimestamp(1, new Timestamp(startTime));
			stmt1.setTimestamp(2, new Timestamp(endTime));
//			sLogger.info("Select inventory data query: " + stmt1.toString());
			rs1 = stmt1.executeQuery();
			while (rs1.next()){
				sum_bids = rs1.getLong("sum_bids");
				sum_nobids = rs1.getLong("sum_nobids");
			}		
//			sLogger.info("adgroupid: " + adgroupId + "  sum_bids: " + sum_bids + "     sum_nobids: " + sum_nobid);
            long[] total_inv = {sum_nobids, sum_bids+sum_nobids};
			return total_inv;
		}finally{
			rs1.close();
			stmt1.close();
		}
	}
	
	public void updatingWinTable(Configuration config) 
			throws SQLException, ParseException{
		String pDateStr = config.get("ProcessingDate");
		String poolName = config.get("PoolName");
		sLogger.info("Export to MySql Pool Name: " + poolName);						        
		String winTableName = config.get("WinTableNameUI");
		sLogger.info("Win Table Name: " + winTableName);
		updatingWinTable(poolName, winTableName, pDateStr);
	}
	
	private void updatingWinTable(String poolName, String winTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Start updating the default values in the win table ...\n");	
			String query = "update " + winTableName + " set cum_wins = null, " +
						"win_rate_hist = null where cum_wins = 0 and date_ = ? ";
			sLogger.info("UpdatingWinTable query: " + query);
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
			sLogger.info("Updating the win data table is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	private double inv_ratio = 0;
	
	private void computingInvRatio(Connection con1, String invTableName, Date pDate) throws Exception{
		PreparedStatement stmt1 = null;
		ResultSet rs1 = null;
		try{
			Date startDate = DateUtils.addMinutes(pDate, 478);
			long startTime = startDate.getTime();
			Date endDate = DateUtils.addDays(startDate, 1);
			long endTime = endDate.getTime();
//			String selectRatio = "select sum(Success+Admin+dailyDeliveryCap+DeliveryFrequencyCap)" +
//					"/sum(Success+Admin+dailyDeliveryCap+DeliveryFrequencyCap+None+BidPricePublisher+Placement) as inv_ratio " +
//					"from " + invTableName + " where createDateStart >= ? and createDateStart < ? and Success > 0";
			String selectRatio = "select sum(Success+Admin+dailyDeliveryCap+DeliveryFrequencyCap)" +
					"/sum(Success+Admin+dailyDeliveryCap+DeliveryFrequencyCap+None+BidPricePublisher+Placement) as inv_ratio " +
					"from " + invTableName + " where createDateStart >= ? and createDateStart < ? and Success > 0 " +
					"and Success<=1000000";
			stmt1 = con1.prepareStatement(selectRatio);
			stmt1.setTimestamp(1, new Timestamp(startTime));
			stmt1.setTimestamp(2, new Timestamp(endTime));
			sLogger.info("Computing the overall ratio: " + stmt1.toString());
			rs1 = stmt1.executeQuery();
			while (rs1.next()){
				inv_ratio = rs1.getDouble("inv_ratio");
			}
			sLogger.info("inv_ratio: " + inv_ratio);
		}finally{
			rs1.close();
			stmt1.close();
		}
	}
	
	private long[] getDayInventoryFrom5Mins(Connection con1, String invTableName, 
			long adgroupId, Date pDate) throws SQLException, ParseException, Exception{

		long sum_nobids = 0;
		long sum_bids = 0;
		PreparedStatement stmt1 = null;
		ResultSet rs1 = null;
		try{
			Date startDate = DateUtils.addMinutes(pDate, 478);
			long startTime = startDate.getTime();
			Date endDate = DateUtils.addDays(startDate, 1);
			long endTime = endDate.getTime();
			
//			String selectInvQuery = " select if(Success>0,1,0) as inv_flag, sum(Success) as sum_succ, " +
//					"sum(Admin+dailyDeliveryCap+DeliveryFrequencyCap) as sum_inv, " +
//					"sum(None+BidPricePublisher+Placement) as sum_noninv from " + 
//					invTableName + " where adGroupId = " + adgroupId + " and " +
//					"createDateStart >= ? and createDateStart < ? group by inv_flag " +
//					"order by inv_flag";

			String selectInvQuery = " select if(Success>0,1,0) as inv_flag, sum(Success) as sum_succ, " +
					"sum(Admin+dailyDeliveryCap+DeliveryFrequencyCap) as sum_inv, " +
					"sum(None+BidPricePublisher+Placement) as sum_noninv from " + 
					invTableName + " where adGroupId = " + adgroupId + " and " +
					"createDateStart >= ? and createDateStart < ? " +
					"and Success<=1000000 " +
					"group by inv_flag order by inv_flag";

			stmt1 = con1.prepareStatement(selectInvQuery);
			stmt1.setTimestamp(1, new Timestamp(startTime));
			stmt1.setTimestamp(2, new Timestamp(endTime));
//			sLogger.info("Select inventory data query: " + stmt1.toString());
			rs1 = stmt1.executeQuery();
			int[] inv_flag = new int[2];
			double[] sum_succ = new double[2];
			double[] sum_inv = new double[2];
			double[] sum_noninv = new double[2];
			int cnt = 0;
			if(rs1!=null){
				while (rs1.next()){
					inv_flag[cnt] = rs1.getInt("inv_flag");
					sum_succ[cnt] = rs1.getDouble("sum_succ");
					sum_inv[cnt] = rs1.getDouble("sum_inv");
					sum_noninv[cnt] = rs1.getDouble("sum_noninv");	
					cnt++;
				}		
			}
//			sLogger.info("adgroupid: " + adgroupId + "      cnt: " + cnt);
//			for(int i=0; i<cnt; i++){
//				sLogger.info("inv_flag: " + inv_flag[i] + "     sum_bids: " + sum_succ[i] 
//						+ "     sum_inv: " + sum_inv[i] + "     sum_noninv: " + sum_noninv[i]);
//			}
			switch(cnt){
				case 0:
					sum_nobids = 0;
					sum_bids = 0;
					break;
				case 1:
					if(inv_flag[0]==1){
						sum_nobids = Math.round(sum_inv[0]);
						sum_bids = Math.round(sum_succ[0]);
					}else{
						sum_nobids = Math.round(inv_ratio*sum_inv[0]);
						sum_bids = 0;
					}
					break;
				case 2: 
					double inv_ratio_ad = (sum_succ[1]+sum_inv[1])/(sum_succ[1]+sum_inv[1]+sum_noninv[1]);
					inv_ratio_ad = Math.min(inv_ratio_ad, inv_ratio);
					sum_nobids = Math.round(inv_ratio_ad*(sum_inv[0]+sum_noninv[0])+ sum_inv[1]);
					sum_bids = Math.round(sum_succ[1]);
					break;
			}
//			sLogger.info("sum_bids: " + sum_bids + "     sum_nobids: " + sum_nobids);
			long[] total_inv = {sum_nobids, sum_bids + sum_nobids};
			return total_inv;
		}finally{
			if(rs1!=null){
				rs1.close();
				stmt1.close();
			}
		}
	}

	private double getDayInventoryFromNoBid(Connection con, String invTableName, String resList, 
			long adgroupId, Date pDate) throws SQLException, ParseException, Exception{

		double sum_nobids = 0;
		PreparedStatement stmt1 = null;
		ResultSet rs1 = null;
		try{			
			String selectInvQuery = "select sum(sum_nobids) as sum_nobids from " + 
					invTableName + " where adgroup_id = " + adgroupId + " and " +
					"Date_ = ? and result_reason in (" + resList + ") and num_days=1 and " + 
					"domain_name=-1 and user_seg_id =-1 and ctl1_id=-1 and " +
					"asl1_id=-1 and max_cpm=-1 and freq_cap=-1";
			
			stmt1 = con.prepareStatement(selectInvQuery);
			stmt1.setTimestamp(1, new Timestamp(pDate.getTime()));
//			sLogger.info("Select inventory data query: " + stmt1.toString());
			rs1 = stmt1.executeQuery();
			if(rs1!=null){
				while (rs1.next()){
					sum_nobids = rs1.getDouble("sum_nobids");
				}	
			}
			return sum_nobids;
		}finally{
			rs1.close();
			stmt1.close();
		}
	}
	
	public Map<String,Integer> gettingFrequencyCap(Connection con, String adgTableName) 
		throws SQLException, ParseException, JSONException, Exception{
		Map<String, Integer> adgFreqCap = new HashMap<String, Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			String selectFreqCap = "select adggoupId, frequencyCap from " + adgTableName;
			stmt = con.prepareStatement(selectFreqCap);
			rs = stmt.executeQuery();
			String adgroupId = null;
			int frequencyCap = 0;
			while(rs.next()){
				adgroupId = rs.getString("adgroupId");
				frequencyCap = rs.getInt("frequencyCap");
				adgFreqCap.put(adgroupId, frequencyCap);
			}
			return adgFreqCap;
		}
		finally{
			rs.close();
			stmt.close();
		}
	}
	
	public Map<String, List<String>> CreativeCategory() 
			throws SQLException, ParseException, Exception{
		String poolName = "rtb";
		String sourceTable = "CreativeCategoryM";
		sLogger.info("Entering to get the creative category ...");						
		poolName = config.get("RTBPoolName", poolName);
		sLogger.info("Export to MySql Pool Name: " + poolName);		
		sourceTable = config.get("CreativeCategoryTable", sourceTable);
		sLogger.info("Creative Category Table: " + sourceTable);							        
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		try {										
			return GetCategoryMap(poolName, sourceTable, con, "creative_id", "category_id");
		}
		finally {
			con.close();
		}
	}    

	public Map<String, List<String>> AdSpaceCategory() 
			throws SQLException, ParseException, Exception{
		String poolName = "rtb";
		String sourceTable = "AdSpaceCategoryM";
		sLogger.info("Entering to get the ad slot category ...");						
		poolName = config.get("RTBPoolName", poolName);
		sLogger.info("Export to MySql Pool Name: " + poolName);		
		sourceTable = config.get("AdSpaceCategoryTable", sourceTable);
		sLogger.info("AdSpace Category Table: " + sourceTable);							        
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		try {										
			return GetCategoryMap(poolName, sourceTable, con, "adspace_id", "category_id");
		}
		finally {
			con.close();
		}
	}    
	
	private Map<String, List<String>> GetCategoryMap(String poolName, String sourceTable, Connection con, 
			String keyName1, String keyName2) 
			throws SQLException, ParseException, Exception{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Map<String, List<String>> categoryMap = new HashMap<String, List<String>>();
		try {										
			String selectMapQuery = "select " + keyName1 + ", " + keyName2 + " from " + sourceTable + " where flag = 0";					
			sLogger.info("Select mapping data query: " + selectMapQuery);				
			stmt = con.prepareStatement(selectMapQuery);
			rs = stmt.executeQuery();
			List<String> categoryIdList = new ArrayList<String>();
			if(rs!=null){
				while (rs.next()) {
					int i_cnt = 0;
					String keyId = rs.getString(keyName1);
					String categoryId = rs.getString(keyName2);
					//sLogger.info("keyId: " + keyId + "   categoryId: " + categoryId);
					if (categoryMap.containsKey(keyId)) {
						categoryIdList = (List<String>) categoryMap.get(keyId);
						i_cnt = categoryIdList.size();
					}else{
						categoryIdList = new ArrayList<String>();
						i_cnt = 0;						
					}
					categoryIdList.add(i_cnt, categoryId);
					categoryMap.put(keyId, categoryIdList);
				}
			}else{
				sLogger.warn("It is null for the mapping of key and category");
			}				
			return categoryMap;
		}
		finally {
			rs.close();
			stmt.close();
		}
	}    	
}