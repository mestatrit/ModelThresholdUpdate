package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StatusType {
	private int status;
	
	public StatusType() {
		status = 0;
	}
	
	private StatusType(int status) {
		this.status = status;
	}
	
	public static StatusType parseStatus(ResultSet result) throws SQLException {
		int status = result.getInt("status");
		return new StatusType(status);
	}
	
	public void setEstimator() {
		status |= 1;
	}
	
	public void setImpression() {
		status |= 2;
	}
	
	public void setKpi() {
		status |= 4;
	}
	
	public void setAudit() {
		status |= 8;
	}

	public boolean getKpi() {
		return (status & 4) != 0;
	}

	public boolean getAudit() {
		return (status & 8) != 0;
	}
	
	public String toString() {
		return String.format("b'%4s'", Integer.toBinaryString(status)).replace(' ', '0');
	}
}