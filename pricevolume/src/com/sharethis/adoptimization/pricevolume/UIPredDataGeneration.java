package com.sharethis.adoptimization.pricevolume;

import java.util.ArrayList;

import com.sharethis.adoptimization.adopt.PriceVolumeModel;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** 
 * This is a function to do the weighted-average.
 */

public class UIPredDataGeneration extends BaseOperation<UIPredDataGeneration.Context> implements Aggregator<UIPredDataGeneration.Context>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int numOfPointsUI = 0;
	private int numOfDays = 1;
	private String pDateStr = null;
	
	public UIPredDataGeneration(Fields fields, int numOfPointsUI, int numOfDays, String pDateStr) {
		super(1, fields);
		this.numOfPointsUI = numOfPointsUI;
		this.numOfDays = numOfDays;
		this.pDateStr = pDateStr;
	}
	
	protected static class Context{
		protected ArrayList<Double> wpL = new ArrayList<Double>(0);
		protected int cnt = 0;
		protected double min_wp = 0;
		protected double max_wp = 0;
		protected ArrayList<Integer> swL = new ArrayList<Integer>(0);
		protected ArrayList<Integer> smL = new ArrayList<Integer>(0);
		protected ArrayList<Integer> cwL = new ArrayList<Integer>(0);
		protected ArrayList<Integer> sbL = new ArrayList<Integer>(0);
		protected ArrayList<Integer> snL = new ArrayList<Integer>(0);
		protected ArrayList<Integer> mtL = new ArrayList<Integer>(0);
		protected ArrayList<Double> b0L = new ArrayList<Double>(0);
		protected ArrayList<Double> b1L = new ArrayList<Double>(0);
		protected ArrayList<Double> max_wp_predL = new ArrayList<Double>(0);
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
		context.swL.add(context.cnt, aggregatorCall.getArguments().getInteger(1));
		context.smL.add(context.cnt, aggregatorCall.getArguments().getInteger(2));
		context.cwL.add(context.cnt, aggregatorCall.getArguments().getInteger(3));
		context.sbL.add(context.cnt, aggregatorCall.getArguments().getInteger(4));
		context.snL.add(context.cnt, aggregatorCall.getArguments().getInteger(5));
		context.mtL.add(context.cnt, aggregatorCall.getArguments().getInteger(6));
		context.b0L.add(context.cnt, aggregatorCall.getArguments().getDouble(7));
		context.b1L.add(context.cnt, aggregatorCall.getArguments().getDouble(8));
		context.max_wp_predL.add(context.cnt, aggregatorCall.getArguments().getDouble(9));
		context.cnt++;		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
//        ModelFunctionEvaluation mfe = new ModelFunctionEvaluation();
		PriceVolumeModel pvMod = new PriceVolumeModel();
		Context context = aggregatorCall.getContext();	
		int num_points = 1;
		double max_wp_pred = context.max_wp_predL.get(0);
		int sum_bids = context.sbL.get(0);
		int sum_nobids = context.snL.get(0);
		int modelType = context.mtL.get(0);
		double beta0 = context.b0L.get(0);
		double beta1 = context.b1L.get(0);
		float deltaVal = (float) (Math.round((float)(context.max_wp-context.min_wp)/numOfPointsUI*10000.0)/10000.0);
		if(context.cnt>numOfPointsUI){
			float[] wpLb = new float[numOfPointsUI+1]; 
			float[] wpUb = new float[numOfPointsUI+1];
			double[] winning_price = new double[numOfPointsUI+1];
			int[] sum_wins = new int[numOfPointsUI+1];
			int[] sum_mbids = new int[numOfPointsUI+1];
			int[] cum_wins = new int[numOfPointsUI+1];
			wpLb[0] = (float) (Math.round((float) context.min_wp*10000)/10000.0);
			wpUb[0] = wpLb[0] + (float) deltaVal;
			winning_price[0] = 0;
			sum_wins[0] = 0;
			sum_mbids[0] = 0;
			cum_wins[0] = 0;
			for(int i=1; i<numOfPointsUI+1; i++){
				wpLb[i] = wpUb[i-1];
				wpUb[i] = wpLb[i] + deltaVal;
				winning_price[i] = 0;
				sum_wins[i] = 0;
				sum_mbids[i] = 0;
				cum_wins[i] = 0;
			}
			for(int i_org=0; i_org<context.cnt; i_org++){
				double wp_tmp = context.wpL.get(i_org);
				int sw_tmp = context.swL.get(i_org);
				int sm_tmp = context.smL.get(i_org);
				int cw_tmp = context.cwL.get(i_org);
				for(int i=0; i<numOfPointsUI+1; i++){
					if(wp_tmp>=wpLb[i]&&wp_tmp<wpUb[i]){
						winning_price[i] += wp_tmp*sw_tmp;
						sum_wins[i] += sw_tmp;
						sum_mbids[i] += sm_tmp;
						if(cum_wins[i] < cw_tmp)
							cum_wins[i] = cw_tmp;
					}
				}
			}
			
			for(int i=0; i<numOfPointsUI+1; i++){
				if(sum_wins[i]>0){
					Tuple tuple = new Tuple();
					double wp_tmp = winning_price[i]/sum_wins[i];
					tuple.add(Math.round(wp_tmp*100)/100.0);
					tuple.add(sum_wins[i]);
					tuple.add(sum_mbids[i]);
					tuple.add(cum_wins[i]);
					tuple.add(sum_bids);
					tuple.add(sum_nobids);
					tuple.add(numOfDays);
					tuple.add(pDateStr);
					double wr_hist = Math.round((cum_wins[i]*1000000.0)/sum_bids)/10000.0;
					double wr_pred = Math.round(pvMod.predictingWR(beta0, beta1, wp_tmp, modelType)*1000000)/10000.0;
					tuple.add(wr_hist);
					tuple.add(wr_pred);
					aggregatorCall.getOutputCollector().add(tuple);
				}
			}
			num_points = numOfPointsUI+1;
		}else{
			for(int i=0; i<context.cnt; i++){
				double wp_tmp = context.wpL.get(i);
				Tuple tuple = new Tuple();
				tuple.add(context.wpL.get(i));
				tuple.add(context.swL.get(i));
				tuple.add(context.smL.get(i));
				tuple.add(context.cwL.get(i));
				tuple.add(sum_bids);
				tuple.add(sum_nobids);
				tuple.add(numOfDays);
				tuple.add(pDateStr);
				double wr_hist = Math.round((context.cwL.get(i)*1000000.0)/sum_bids)/10000.0;
				double wr_pred = Math.round(pvMod.predictingWR(beta0, beta1, wp_tmp, modelType)*1000000)/10000.0;
				tuple.add(wr_hist);
				tuple.add(wr_pred);
				aggregatorCall.getOutputCollector().add(tuple);
			}
			num_points = context.cnt;
		}
		// Computing the predictive winning rate.
		double predDelta = Math.round((context.max_wp-context.min_wp)/num_points*100)/100.0;
		int num_points_pred = (int) Math.round((max_wp_pred-context.max_wp)/predDelta);
		for(int i=1; i<num_points_pred+1; i++){
			double wp_tmp = Math.round((context.max_wp + i*predDelta)*100)/100.0;
			Tuple tuple = new Tuple();
			tuple.add(wp_tmp);
			tuple.add("\\N");
			tuple.add("\\N");
			tuple.add("\\N");
			tuple.add("\\N");
			tuple.add("\\N");
			tuple.add(numOfDays);
			tuple.add(pDateStr);
			double wr_pred = Math.round(pvMod.predictingWR(beta0, beta1, wp_tmp, modelType)*1000000)/10000.0;
			tuple.add("\\N");
			tuple.add(wr_pred);
			aggregatorCall.getOutputCollector().add(tuple);			
		}		
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {		
		aggregatorCall.setContext(new Context());
	}
}
