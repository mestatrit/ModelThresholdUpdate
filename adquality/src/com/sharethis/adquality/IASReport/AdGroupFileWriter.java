package com.sharethis.adquality.IASReport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sharethis.adquality.dao.AdQualityAdGroupsViewDAO;
import com.sharethis.common.error.ErrorCode;
import com.sharethis.common.helper.DBConnectionHelper;
import com.sharethis.common.helper.app.AppConfig;
import com.sharethis.common.helper.log.LogHelper;
import com.sharethis.common.sql.DBConnect;
import com.sharethis.common.state.RuntimeState;

public class AdGroupFileWriter {
	static Logger log = Logger.getLogger(AdGroupFileWriter.class);
	private List<AdGroup> adGroupList = new LinkedList<AdGroup>();
/*	public int init(AppConfig config){
		int ret=ErrorCode.ERROR_SUCCESS;
		
		return ret;
	}*/
	DBConnect getConnection(AppConfig config) {
		DBConnect conn = null;
		try {
			conn = DBConnectionHelper.createConnection(config);
		}
		catch(Exception e) {
			log.error(LogHelper.formatMessage(e));
		}		
		return conn;
	}
	public int write(String file, AppConfig config) throws Exception{

		RuntimeState rs = new RuntimeState();
		DBConnect conn = getConnection(config);
		if (conn==null)
			return ErrorCode.ERROR_INVALID_ACCESS;
		//read from file
		int ret=downloadAdGroups(conn,rs);
		if(ret==ErrorCode.ERROR_SUCCESS){
			ret = writeFile(file);
		}
		conn.doClose();
		return ret;
	}
	private int writeFile(String file) {
		int ret=  ErrorCode.ERROR_SUCCESS;
		BufferedWriter bw =null;
		try {
			bw = new BufferedWriter(new FileWriter(new File(file)));
			for(AdGroup adg : this.adGroupList){
				adg.write(bw);
				bw.newLine();
			}
			bw.close();
		} catch (IOException e) {
				try {
					if(bw!=null)
						bw.close();
				} catch (IOException e1) {
					log.error("writeFile:Cannot close file "+file+"\n"+LogHelper.formatMessage(e1));
				}
			log.error("writeFile:"+file+"\n"+LogHelper.formatMessage(e));
			ret=  ErrorCode.ERROR_GENERAL;
		}
		return 0;
	}
	private static int readFile(List<AdGroup> list, String file) {
		int ret=  ErrorCode.ERROR_SUCCESS;
		BufferedReader br =null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line=null;
			while((line=br.readLine())!=null){
				AdGroup adg = new AdGroup();
				adg.fromLine(line);
				list.add(adg);
			}
			br.close();
		} catch (IOException e) {
				try {
					if(br!=null)
						br.close();
				} catch (IOException e1) {
					log.error("writeFile:Cannot close file "+file+"\n"+LogHelper.formatMessage(e1));
				}
			log.error("readFile:"+file+"\n"+LogHelper.formatMessage(e));
			ret=  ErrorCode.ERROR_GENERAL;
		}
		return 0;
	}
	int downloadAdGroups(DBConnect conn, RuntimeState rs ){
		int ret=  ErrorCode.ERROR_SUCCESS;
		List<AdQualityAdGroupsViewDAO> daoList;
		String query=AdQualityAdGroupsViewDAO.SELECT;
		try {
			daoList = AdQualityAdGroupsViewDAO.readByQuery(conn,query,rs);

			for(AdQualityAdGroupsViewDAO dao: daoList){
				AdGroup adg = new AdGroup();
				adg.adGroupId = dao.getAdGroupId();
				adg.name = dao.getName();
				adg.flags = dao.getFlags();
				this.adGroupList.add(adg);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("downloadAdGroups:"+query+"\n"+LogHelper.formatMessage(e));
			ret=  ErrorCode.ERROR_ACCESS_DENIED;
		}
		return ret;
	}
	public static List<AdGroup> listFromFile(String adgouplistFile) {
		List<AdGroup> list = new LinkedList<AdGroup>();
		AdGroupFileWriter.readFile(list, adgouplistFile);
		return list;
	}
}
