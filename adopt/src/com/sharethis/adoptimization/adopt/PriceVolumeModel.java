package com.sharethis.adoptimization.adopt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * This is the class to run the flow.
 */

public class PriceVolumeModel 
{	
	private static final Logger sLogger = Logger.getLogger(PriceVolumeModel.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	private int modelType = -99;
	private double beta0 = 0;
	private double beta1 = 0;
	private double rSquare = 0;
	private double totalInventory = 0;
	private String modelTableName = "price_volume_model_pred";

	protected String url;
	protected String driver;
	protected String login;
	protected String password;
	Connection con = null;

	public PriceVolumeModel() {
	}
	
	public PriceVolumeModel(Connection con, String modelTableName) throws Exception{
		this.con = con;
		this.modelTableName = modelTableName;
	}

	public PriceVolumeModel(long adgroupId) throws Exception{
		con = new JdbcOperations().getConnection();
		getModelCoefficients(adgroupId);
	}

	public PriceVolumeModel(Connection con, long adgroupId) throws Exception{
		this.con = con;
		getModelCoefficients(adgroupId);
	}

	public PriceVolumeModel(Connection con, String modelTableName, long adgroupId) throws Exception{
		this.con = con;
		this.modelTableName = modelTableName;
		getModelCoefficients(adgroupId);
	}
	
	public double getWinningPrice(double win_rate){
		return computingWP(beta0, beta1, win_rate, modelType);
	}
	
	public double getWinRate(double winning_price){
		return predictingWR(beta0, beta1, winning_price, modelType);
	}
		
	private void getModelCoefficients(long adgroupId) throws Exception{
		getOneModel(adgroupId, "-1", -1, 1, null);
		sLogger.info("Price Volume Model for adgroup: " + adgroupId + "    beta0 = " + beta0 + "   beta1 = " + beta1 + "   model_type = " + modelType);
	}

	public void getOneModel(long adgroupId, String domainName, long creativeId, int numOfDays, Date pDate) 		
		throws Exception{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			Date maxDate = null;
			if (pDate==null){
				String selectMaxDate = "select max(date_) as max_date from " + modelTableName + 
					" where adgroup_id = " + adgroupId + " and domain_name = ?" + 
					" and creative_id = " + creativeId + " and num_days = " + numOfDays;
				stmt = con.prepareStatement(selectMaxDate);
				stmt.setString(1, domainName);
				sLogger.info("Select max date: " + stmt.toString());
				rs = stmt.executeQuery();
				if(rs!=null){
					while (rs.next()) {
						String maxDateStr = rs.getString("max_date");
						if((maxDateStr!=null)&&!"null".equalsIgnoreCase(maxDateStr)&&!"".equalsIgnoreCase(maxDateStr))
							maxDate = sdf.parse(rs.getString("max_date"));
					}
//					sLogger.info("The max date: " + maxDate + " for adgroup: " + adgroupId);
				}else{
					sLogger.warn(modelTableName + " does not have any record for the adgroup: " + adgroupId 
						+ "  domain: " + domainName + "  creative: " + creativeId);					
				}
				rs.close();
				stmt.close();
			}else{
				maxDate = pDate;
			}
			if (maxDate != null){ 
				String selectModelPred = "select model_type, beta0, beta1, r_square, total_inventory from " + modelTableName + 
						" where adgroup_id = " + adgroupId + " and domain_name = ?" +
						" and creative_id = " + creativeId + " and num_days = " + numOfDays + 
						" and date_ = ?";
				stmt = con.prepareStatement(selectModelPred);
				stmt.setString(1, domainName);	
				stmt.setTimestamp(2, new Timestamp(maxDate.getTime()));
//				sLogger.info("Select one model: " + stmt.toString());
				rs = stmt.executeQuery();
				if (rs!=null){
					while (rs.next()){
						modelType = rs.getInt("model_type");
						beta0 = rs.getDouble("beta0");
						beta1 = rs.getDouble("beta1");
						rSquare = rs.getDouble("r_square");
						totalInventory = rs.getDouble("total_inventory");
					}
//					sLogger.info("One Model for adgroup: " + adgroupId + "    beta0 = " + beta0 + "   beta1 = " + beta1 + "   model_type = " + modelType);
				}else{
					sLogger.warn(modelTableName + " does not have any record for the adgroup: " + adgroupId 
							+ "  domain: " + domainName + "  creative: " + creativeId);					
				}
				rs.close();
				stmt.close();
			}else{
				sLogger.warn(modelTableName + " does not have any record for the adgroup: " + adgroupId 
						+ "  domain: " + domainName + "  creative: " + creativeId);
			}
		}catch (Exception ex){
			throw new Exception(ex);
		}
	}

	public double predictingWR(double beta0, double beta1, double winning_price, int modelType){
		if(winning_price<=0)
			return 0;
		double wr = -1;
		if(modelType<=-2){
			wr = beta0 + beta1 * Math.pow(winning_price, 1.0/(-modelType));
			wr = wr/(1+wr);
		}
		if(modelType==-1){
			wr = beta0 + beta1 * winning_price;
			wr = wr/(1+wr);
			if(wr<0) 
				wr = 0;
			else
				wr = Math.pow(wr, 2);
		}
		if(modelType==0){
			wr = beta0 + beta1 * winning_price;
			if(wr<0)
				wr = 0;
			else
				wr = Math.pow(wr, 2);
			wr = wr/(1+wr);
		}
		if(modelType>=1){
			wr = beta0 + beta1 * Math.pow(winning_price, modelType);
			wr = wr/(1+wr);
		}
		if (wr<0) wr = 0;
		return wr;
	}
		
	public double computingWP(double beta0, double beta1, double win_rate, int modelType){
		double wp = 0;
		if(modelType<=-2){
			wp = win_rate/(1-win_rate);
			wp = (wp - beta0)/beta1;
			wp = Math.pow(wp, -modelType);
		}
		if(modelType==-1){
			wp = Math.sqrt(win_rate);
			wp = wp/(1-wp);
			wp = (wp - beta0)/beta1;
		}
		if(modelType==0){
			wp = Math.sqrt(win_rate/(1-win_rate));
			wp = (wp - beta0)/beta1;
		}
		if(modelType>=1){
			wp = win_rate/(1-win_rate);
			wp = (wp - beta0)/beta1;
			if (wp <= 0)
				wp = 0.0;
			else
			    wp = Math.pow(wp, 1.0/modelType);
		}
		if(wp<0) wp = 0;
		return wp;
	}
	
	public int getModelType() {
		return modelType;
	}

	public void setModelType(int modelType) {
		this.modelType = modelType;
	}

	public double getBeta0() {
		return beta0;
	}

	public void setBeta0(double beta0) {
		this.beta0 = beta0;
	}

	public double getBeta1() {
		return beta1;
	}

	public void setBeta1(double beta1) {
		this.beta1 = beta1;
	}

	public double getRSquare() {
		return rSquare;
	}

	public void setRSquare(double rSquare) {
		this.rSquare = rSquare;
	}

	public double getTotalInventory() {
		return totalInventory;
	}

	public void setTotalInventory(double totalInventory) {
		this.totalInventory = totalInventory;
	}	
	
}