package com.sharethis.adoptimization.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ExportToDBJob extends Configured implements Tool{

	/**
	 * @param args
	 */
	public final static String POOL_NAME_KEY = "PoolName";
	public final static String TABLE_SCHEMA_KEY = "TableSchema";
	public final static String TABLE_PROPERTIES= "TableProperties";
	public final static String SOURCE_PATH_KEY = "SourcePath";
	protected final static Logger sLogger = Logger.getLogger(ExportToDBJob.class);
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if(!args.equals(null)){
			int argsLen = args.length;
			String[] parmStr = new String[2];
			for(int i=0; i<argsLen; i++){
				if(args[i].contains("=")){
					parmStr = StringUtils.split(args[i],'=');
					conf.set(parmStr[0], parmStr[1]);
				}
			}
		}
		
		System.exit(ToolRunner.run(conf, new ExportToDBJob(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Job job = new Job(config);
		
		job.setJobName("ExportToDBJob");
		job.setJarByClass(ExportToDBJob.class);
		job.setMapperClass(ExportToDBJobMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(FileStreamInputFormat.class);
		String path = config.get(SOURCE_PATH_KEY);
		sLogger.info("Source path: " + path);
		FileStreamInputFormat.addInputPath(job, new Path(path));
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		String tableCreateSchema = config.get(TABLE_SCHEMA_KEY);
		if(StringUtils.isNotBlank(tableCreateSchema)){
			DBUtils.createTable(config, config.get(POOL_NAME_KEY), tableCreateSchema);
		}
		int ret = 0;
		try {
			ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	public static class ExportToDBJobMapper extends
			Mapper<NullWritable, FSDataInputStream, NullWritable, NullWritable> {

		protected static final Logger sLogger = Logger.getLogger(ExportToDBJobMapper.class);
		protected Connection conn;
		protected String tableProperties;
		@Override
		protected void setup(
				Mapper<NullWritable, FSDataInputStream, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			//initialize the connection and statement here
			try{
				//also add these properties to apache config
				String poolName=context.getConfiguration().get(POOL_NAME_KEY, "");
				sLogger.info("PoolName: " + poolName);
				JdbcOperations jdbc = new JdbcOperations(context.getConfiguration(), poolName);
				sLogger.info("jdbc.drive: " + jdbc.getDriver() + "\n" + "jdbc.url: " + jdbc.getUrl() + "\n" + 
						"jdbc.user: " + jdbc.getLogin() + "\n" + "jdbc.password: " + jdbc.getPassword());
				Class.forName(jdbc.getDriver());
				tableProperties = context.getConfiguration().get(TABLE_PROPERTIES);
				conn = DriverManager.getConnection(jdbc.getUrl(), jdbc.getLogin(), jdbc.getPassword());
			}catch(Exception e){
				throw new IOException(e);
			}
		}

		@Override
		public void map(NullWritable key, FSDataInputStream val, Context context)
				throws IOException, InterruptedException {
			String query = "load data local infile 'dummy.txt' into table "
					+ tableProperties;
			sLogger.info("Query: " + query);
			Statement st = null;
			try {
				st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
				((com.mysql.jdbc.Statement) st).setLocalInfileInputStream(val);
				st.executeUpdate(query);
			} catch (SQLException e) {
				throw new IOException(e);
			} finally {
				DBUtils.close(st);
			}
		}

		@Override
		protected void cleanup(
				Mapper<NullWritable, FSDataInputStream, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			DBUtils.close(conn);
		}
	}
	
	public static class FileStreamInputFormat extends FileInputFormat<NullWritable, FSDataInputStream>{

		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}
		
		@Override
		public RecordReader<NullWritable, FSDataInputStream> createRecordReader(
				InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
				InterruptedException {
			return new RecordReader<NullWritable, FSDataInputStream>() {
				protected FileSplit split;
				protected TaskAttemptContext context;
				protected FSDataInputStream stream;
				protected boolean isOpen = false;
				
				@Override
				public void close() throws IOException {
					if(stream != null) stream.close();
				}

				@Override
				public NullWritable getCurrentKey() throws IOException,
						InterruptedException {
					return NullWritable.get();
				}

				@Override
				public FSDataInputStream getCurrentValue() throws IOException,
						InterruptedException {
					return stream;
				}

				@Override
				public float getProgress() throws IOException,
						InterruptedException {
					return 0;
				}

				@Override
				public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
						throws IOException, InterruptedException {
					this.context = taskAttemptContext;
					this.split = (FileSplit)inputSplit;
				}

				@Override
				public boolean nextKeyValue() throws IOException,InterruptedException {
					if(!isOpen){
						Path file = split.getPath();
						sLogger.debug("FileName: " +file.getName());
						stream = FileSystem.get(context.getConfiguration()).open(split.getPath());
						isOpen = true;
						sLogger.debug(file.getName() + " loaded as stream");
						return true;
					}
					return false;
				}
			};
		}
	}
}
