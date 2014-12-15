package com.sharethis.adoptimization.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

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

public class AdoptimizationUtils 
{	
	private static final Logger sLogger = Logger.getLogger(AdoptimizationUtils.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Configuration config = new Configuration();

	public AdoptimizationUtils(Configuration config){
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

	public void updatingColumns(String poolName, String tableName, String columnName, 
			String columnValue, String defaultVal) throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String colVal = "'" + columnValue + "'";
			String defVal = "'" + defaultVal + "'";
			String query = "update " + tableName + " set " + columnName + " = " + colVal
						+ " where " + columnName + " = " + defVal;
			stmt = con.prepareStatement(query);
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}
	
	public void updatingColumns(String poolName, String tableName, String columnName, 
			String pDateStr) throws SQLException, ParseException{
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

	public void updatingColumns(String poolName, String tableName, String columnName, long timestamp) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "update " + tableName + " set " + columnName + " = " 
						+ timestamp + " where " + columnName + " is null";
			stmt = con.prepareStatement(query);
			stmt.executeUpdate();
		}
		finally {
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

	public void deleteDataByDate(String poolName, String tableName, String columnName, long timestamp) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			String query = "delete from " + tableName + " where " + columnName + " = " + timestamp;
			stmt = con.prepareStatement(query);
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

	public void deleteDataByDate(String poolName, String tableName, String columnName, long timestamp, String[] keyFields) 
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
			String query = "delete from " + tableName + " where " + columnName + " = " + timestamp + " and " + whKeyFields;
			sLogger.info("delete the data query: " + query);
			stmt = con.prepareStatement(query);
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
			Date pDate = DateUtils.addDays(sdf.parse(pDateStr), -daysKept);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			sLogger.info("Delete Query: " + stmt.toString());
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}
}