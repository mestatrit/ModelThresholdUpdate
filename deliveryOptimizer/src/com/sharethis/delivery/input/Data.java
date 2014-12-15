package com.sharethis.delivery.input;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

import com.sharethis.delivery.base.DOException;

public interface Data {
	public boolean read() throws IOException, ParseException, SQLException, DOException;
	public void writeCampaignPerformances() throws IOException, ParseException, SQLException;
	public void writeAdGroupSettings() throws IOException, ParseException, SQLException;
	public boolean isActive();
}
