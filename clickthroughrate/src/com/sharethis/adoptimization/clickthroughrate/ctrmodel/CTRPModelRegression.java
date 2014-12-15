package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.ArrayList;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.Logger;

/** 
 * This is a function to do modeling.
 */

public class CTRPModelRegression extends BaseOperation<CTRPModelRegression.Context> implements Aggregator<CTRPModelRegression.Context>
{
	/**
	 * 
	 */
	private static final Logger sLogger = Logger.getLogger(CTRPModelRegression.class);
	private static final long serialVersionUID = 1L;
	private int ptThVal = 15;
	private int ptMult = 1;
	private double rsThVal = 0.75;
	private int modelType = 0;
	private double dataCons = 0.0005;
	private double dataMult = 1.0;

	public CTRPModelRegression(Fields fields, int ptThVal, int ptMult, double rsThVal, 
			int modelType, double dataCons, double dataMult) {
		super(1, fields);
		this.ptThVal = ptThVal;
		this.ptMult = ptMult;
		this.rsThVal = rsThVal;
		this.modelType = modelType;
		this.dataCons = dataCons;
		this.dataMult = dataMult;
	}

	protected static class Context{
		protected ArrayList<Double> dataL = new ArrayList<Double>(0);
		protected int cnt = 0;
		protected int numObs = 0;
		protected int numVars = 0;
		protected int numFields = 0;
	}

	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();
		TupleEntry tupEntry = aggregatorCall.getArguments();
		context.numFields = tupEntry.size();
		context.numVars = tupEntry.size()-1;
		context.numObs ++;
		for(int i=0; i<context.numFields; i++){
			double yxData = aggregatorCall.getArguments().getDouble(i);
			if(modelType==1&&i==0){
				yxData = yxData/(1-yxData);
			}
			if(modelType==2){
				yxData = Math.log(dataCons+yxData);				
			}
			context.dataL.add(context.cnt, yxData);
			context.cnt++;
		}
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		OLSMultipleLinearRegression mlr = new OLSMultipleLinearRegression();
		Tuple tuple = new Tuple();
		Context context = aggregatorCall.getContext();	
		double r_square = -1;
		double adj_r_square = -1;
		if (context.numObs>=ptThVal+ptMult*context.numVars){
			int dataLen = context.dataL.size();
			double[] yxData = new double[dataLen];
			for(int i=0; i<dataLen; i++){
				yxData[i] = context.dataL.get(i);
			}
			mlr.newSampleData(yxData, context.numObs, context.numVars);
			try{
				double[] coeffs = mlr.estimateRegressionParameters();
				r_square = mlr.calculateRSquared();
				adj_r_square = mlr.calculateAdjustedRSquared();

				if(r_square>=rsThVal&&adj_r_square>=rsThVal){
					for(int i=0; i<coeffs.length; i++){
						double betaVal = Math.round(coeffs[i]*1000000)/1000000.0;
						tuple.add(betaVal);
					}
					r_square = Math.round(r_square*1000000)/1000000.0;
					adj_r_square = Math.round(adj_r_square*1000000)/1000000.0;
					tuple.add(r_square);
					tuple.add(adj_r_square);
					tuple.add(context.numObs);
					tuple.add(modelType);
					aggregatorCall.getOutputCollector().add(tuple);
				}
			}catch(Exception ex){
				sLogger.warn("Regression throws an exception: " + ex.toString());
/*				for(int i=0; i<context.numVars+1; i++){
					tuple.add(0);
				}
				tuple.add(-1);
				tuple.add(-1);
				tuple.add(context.numObs);
				tuple.add(-1);
				aggregatorCall.getOutputCollector().add(tuple);
*/
			}
		}
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {		
		aggregatorCall.setContext(new Context());
	}
}
