package com.sharethis.iasapi;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class IasCampaignViewabiltyReportTest {

	@Test
	public void testNoViewabilityRecord(){
		String json = "{\"recordCount\":0,\"viewability\":[],\"query\":\"{sortIndex=[totalImpressions], page=[1], totalrows=[5000], sortOrder=[desc], period=[twodaysago], includeBenchmark=[true], cutoff=[1000], groups=[[camp]], rows=[5000], _search=[false]}\",\"path\":\"teams/1314/cm/campaigns/5265/campaignviewability\",\"executionTimeMS\":169}";
		IasCampaignViewabiltyReport iasReport=null;
		try {
			iasReport = IasCampaignViewabiltyReport.fromJson(json);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTrue(iasReport.recordCount==0);
	}

}
