package com.sharethis.adquality.IASReport;

import java.io.BufferedWriter;
import java.io.IOException;

import com.sharethis.common.bo.BaseBO;

public class AdGroup {
	public class Flags {
		public static final int USES_VIEWAB=1;
		public static final int USES_BRAND_SAFETY=2;
	}
	boolean usesViewability() { return (this.flags & Flags.USES_VIEWAB)>0; }
	boolean usesBrandSafety() { return (this.flags & Flags.USES_BRAND_SAFETY)>0; }
	//Member variables
	long adGroupId;
	String name;
	int flags;
	@Override
	public String toString(){
		return adGroupId+BaseBO.DB_FILE_FIELD_DELIMITER+name+BaseBO.DB_FILE_FIELD_DELIMITER+flags;
	}
	public void write(BufferedWriter bw) throws IOException {
		bw.write(this.toString());
	}
	public void fromLine(String line) {
		String[] fields=line.split(BaseBO.DB_FILE_FIELD_DELIMITER);
		int cnt=0;
		this.adGroupId = Long.parseLong(fields[cnt++]);
		this.name = fields[cnt++];
		this.flags = Integer.parseInt(fields[cnt++]);
	}
}
