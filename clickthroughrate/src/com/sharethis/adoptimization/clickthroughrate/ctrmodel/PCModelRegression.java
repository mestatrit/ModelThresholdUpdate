package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.ArrayList;
import java.util.List;

import com.sharethis.adoptimization.adopt.PriceVolumeModel;
import com.sharethis.adoptimization.common.LinearRegression;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** 
 * This is a function to do modeling.
 */

public class PCModelRegression extends BaseOperation<PCModelRegression.Context> implements Aggregator<PCModelRegression.Context>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int wpThVal = 15;
	private double rsThVal = 0.75;
	private int numOfDays = 1;
	private String pDateStr=null;
	private double maxPredCTR = 0.75;
	
	public PCModelRegression(Fields fields, int wpThVal, double rsThVal, double maxPredCTR, 
			int numOfDays, String pDateStr) {
		super(1, fields);
		this.wpThVal = wpThVal;
		this.rsThVal = rsThVal;
		this.maxPredCTR = maxPredCTR;
		this.numOfDays = numOfDays;
		this.pDateStr = pDateStr;
	}
	
	protected static class Context{
		protected ArrayList<Double> wpL = new ArrayList<Double>(0);
		protected ArrayList<Double> wrL = new ArrayList<Double>(0);
		protected int cnt = 0;
		protected double wr_max = 0;
		protected double min_wp = 0;
		protected double max_wp = 0;
	}

	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();
		double wp_tmp = aggregatorCall.getArguments().getDouble(0);
		if(context.max_wp<wp_tmp)
			context.max_wp = wp_tmp;
		if(context.min_wp>wp_tmp)
			context.min_wp = wp_tmp;		
		context.wpL.add(context.cnt, wp_tmp);
		
		double wr_tmp = aggregatorCall.getArguments().getDouble(1)/aggregatorCall.getArguments().getDouble(2);
		if(context.wr_max<wr_tmp)
			context.wr_max = wr_tmp;
		context.wrL.add(context.cnt, wr_tmp);
		context.cnt++;		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		PriceVolumeModel pMod = new PriceVolumeModel();
		Tuple tuple = new Tuple();
		Context context = aggregatorCall.getContext();	
		double r_square = -1;
		double beta0 = 0;
		double beta1 = 0;
		int model_type = -99;
		if (context.cnt>=wpThVal){
			int wpL_size = context.wpL.size();
			for(int i_m=-3; i_m<4; i_m++){
//				if(i_m!=0){
					List<Double> wpAdjL = new ArrayList<Double>(0);
					List<Double> wrAdjL = new ArrayList<Double>(0);
					if(i_m<=-2){
						for(int i=0; i<wpL_size; i++){
							double wp_tmp = Math.pow(context.wpL.get(i),1.0/(-i_m));
							wpAdjL.add(i, wp_tmp);
							double wr = context.wrL.get(i);
							wrAdjL.add(i, wr/(1-wr));
						}
					}
					if(i_m==-1){
						for(int i=0; i<wpL_size; i++){
							double wp_tmp = context.wpL.get(i);
							wpAdjL.add(i, wp_tmp);
							double wr = Math.sqrt(context.wrL.get(i));
							wrAdjL.add(i, wr/(1-wr));
						}					
					}
					if(i_m==0){
						for(int i=0; i<wpL_size; i++){
							double wp_tmp = context.wpL.get(i);
							wpAdjL.add(i, wp_tmp);
							double wr = Math.sqrt(context.wrL.get(i)/(1-context.wrL.get(i)));
							wrAdjL.add(i, wr);
						}					
					}
					if(i_m>=1){
						for(int i=0; i<wpL_size; i++){
							double wp_tmp = Math.pow(context.wpL.get(i),i_m);
							wpAdjL.add(i, wp_tmp);
							double wr = context.wrL.get(i);
							wrAdjL.add(i, wr/(1-wr));
						}					
					}
					LinearRegression lg = new LinearRegression(wpAdjL, wrAdjL);
				
					double r_square_tmp = lg.generatingLRM();
					double beta0_tmp = lg.getBeta0();
					double beta1_tmp = lg.getBeta1();
					if((r_square_tmp > r_square)&&(beta1_tmp>=0.000001)&&(Math.abs(beta0_tmp)>=0.000001)){
						r_square = r_square_tmp;
						beta0 = beta0_tmp;
						beta1 = beta1_tmp;
						model_type = i_m;
					}	
//				}
			}

			if(r_square>rsThVal){
				tuple.add(model_type);
				beta0 = Math.round(beta0*1000000)/1000000.0;
				tuple.add(beta0);
				beta1 = Math.round(beta1*1000000)/1000000.0;
				tuple.add(beta1);
				r_square = Math.round(r_square*1000000)/1000000.0;
				tuple.add(r_square);
				tuple.add(context.cnt);
				context.wr_max = Math.round(context.wr_max*1000000)/1000000.0;
				tuple.add(context.wr_max);
				tuple.add(context.min_wp);
				tuple.add(context.max_wp);
				double maxPredPrice = pMod.computingWP(beta0, beta1, maxPredCTR, model_type);       
				maxPredPrice = Math.min(maxPredPrice, 2.0*context.max_wp);
				maxPredPrice = Math.round(Math.max(maxPredPrice, context.max_wp)*100)/100.0;
				tuple.add(maxPredPrice);
				tuple.add(numOfDays);
				tuple.add(pDateStr);
				aggregatorCall.getOutputCollector().add(tuple);
			}
		}
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {		
		aggregatorCall.setContext(new Context());
	}
}
