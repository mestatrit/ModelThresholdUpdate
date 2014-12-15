package com.sharethis.adoptimization.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;

public class DBUtils {
	public static void createTable(Configuration conf, String poolName, String schema) throws SQLException{
		Connection conn = null;
		Statement st = null;
		try{
			conn = new JdbcOperations(conf, poolName).getConnection();
			st = conn.createStatement();
			st.executeUpdate(schema);
		}finally{
			if(st!=null)st.close();
			if(conn!=null)conn.close();
		}
	}
	
	public static void close(Connection conn){
		try{
			if(conn != null) conn.close();
		}catch(SQLException e){}
	}
	
	public static void close(Statement st){
		try{
			if(st!=null)st.close();
		}catch(SQLException e){}
	}
	
	public static void close(ResultSet rs){
		try{
			if(rs!=null)rs.close();
		}catch(SQLException e){}
	}
	
	public static void close(ResultSet rs, Statement st, Connection conn){
		close(rs);
		close(st);
		close(conn);
	}
}
