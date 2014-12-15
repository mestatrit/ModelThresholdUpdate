package com.sharethis.adoptimization.pricevolume.pvmodel;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.sharethis.adoptimization.common.JdbcOperations;


/**
 * This is the class to run the flow.
 */

public class ModelEvaluationUtils 
{	
	private static final Logger sLogger = Logger.getLogger(ModelEvaluationUtils.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public ModelEvaluationUtils(){
	}
	
	public void updatingBidDataIntoModelTable(Configuration config) throws SQLException, ParseException{
		String pDateStr = config.get("ProcessingDate");		
		String poolName = config.get("PoolName");				        
		String bidTableName = config.get("BidTableName");
		String modelTableName = config.get("ModelTableName");
		updatingBidDataIntoModelTable(config, poolName, modelTableName, bidTableName, pDateStr) ;
		
	}
	
	public void updatingBidDataIntoModelTable(Configuration config, String poolName, String modelTableName, String bidTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Start updating the bid data into the model data table ...\n");	
			String query = "update " + modelTableName + " a, " + bidTableName + " b set a.impressionDelivery = b.sum_wins, " +
					"a.ecpmDelivery = b.e_cpm, a.sum_bids = b.sum_bids, a.sum_nobids = b.sum_nobids, " + 
					"a.total_inventory = (b.sum_bids + b.sum_nobids) where a.adgroup_id = b.adgroup_id and a.creative_id = b.creative_id " +
					"and a.domain_name = b.domain_name and a.num_days = b.num_days and a.date_=b.date_ and a.date_ = ? ";
			sLogger.info("UpdatingBidDataIntoModelTable query: " + query);
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
			sLogger.info("Updating the bid data into the model data table is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}	
}