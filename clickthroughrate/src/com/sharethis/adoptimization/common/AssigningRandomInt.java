package com.sharethis.adoptimization.common;

import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import org.apache.log4j.Logger;

public class AssigningRandomInt extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(AssigningRandomInt.class);
	private Fields dFields = null;
	private int maxRandNum = 0;

	public AssigningRandomInt(Fields fieldDeclaration, Fields dFields, int maxRandNum)
    {
		super(1, fieldDeclaration);
		this.dFields = dFields;
		this.maxRandNum = maxRandNum;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		Tuple group = arguments.selectTuple(dFields);
		Random ran = new Random();
		int randInd = ran.nextInt(maxRandNum);
		Tuple gp_tmp = new Tuple(randInd);
		group.addAll(gp_tmp);
		functionCall.getOutputCollector().add(group);	
    }
}
