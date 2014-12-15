package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class PredictingCTRforModelValidation extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private String[] keyNames;
	private String[] keyIds = null;
	private double[] coeffs;
	private double intercept;
	private Map<String, Map<String, Double>> ctrBaseMap = null;
	private String impField = null;
	
	public PredictingCTRforModelValidation(Fields fields, String[] keyNames, String[] keyIds, double[] coeffs, 
			double intercept, Map<String, Map<String, Double>> ctrBaseMap, String impField) {
		super(1, fields);
		this.keyNames = keyNames;
		this.keyIds=keyIds;
		this.coeffs = coeffs;
		this.intercept = intercept;
		this.ctrBaseMap = ctrBaseMap;
		this.impField = impField;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		CTRPQueryingCTRBase qCTR = new CTRPQueryingCTRBase();

		TupleEntry arguments = functionCall.getArguments();
		Tuple aTuple = new Tuple();
		String[] keyValues = new String[keyIds.length];
		for(int i=0; i<keyIds.length; i++){
			keyValues[i] = arguments.getString(keyIds[i]);			
		}
		
		double[] ctrBase = qCTR.quaryingCTRBase(ctrBaseMap, keyNames, keyValues);
		
		double ctrPred = intercept;
		for(int i=0; i<keyNames.length; i++){
			ctrPred += coeffs[i]*ctrBase[i];
		} 
		if(ctrPred<0)
			ctrPred=0;
		ctrPred = ctrPred*arguments.getDouble(impField);
		aTuple.addAll(ctrPred);
		functionCall.getOutputCollector().add(aTuple);
	}
}
