package org.teiid.test.framework.datasource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataSource {
	public static final String CONNECTOR_TYPE="connectortype";
	public static final String DIRECTORY="dir";
	public static final String DB_TYPE="db.type";
	
	static Map<String, String> dbtypeBitMaskMap = null;

	
	private Properties props;

	private String name;
	private String group;
	private String dbtype;
	private int bitMask;
	
	public DataSource(String name, String group, Properties properties) {
		this.name = name;
		this.group = group;
		this.props = properties;
		this.dbtype = this.props.getProperty(DB_TYPE);
		this.setBitMask();
	}

	
	public String getName() {
		return name;
	}
	
	public String getGroup() {
		return group;
	}
	
	public String getConnectorType() {
		return props.getProperty(CONNECTOR_TYPE);
	}
	
	public String getProperty(String propName) {
		return props.getProperty(propName);
	}
	
	public Properties getProperties() {
		return this.props;
	}
	
	public String getDBType() {
		return this.dbtype;
	}

	public int getBitMask() {
		return this.bitMask;
	}
	
	
	/**
	 * These types match the "ddl" directories for supported database types
	 * and it also found in the datasources connection.properties defined by the DB_TYPE property
	 * @author vanhalbert
	 *
	 */
	public static interface DataSourcTypes{
		public static String MYSQL = "mysql";
		public static String ORACLE = "oracle";
		public static String POSTRES = "postgres";
		public static String SQLSERVER = "sqlserver";
		public static String DB2 = "db2";
		public static String SYBASE = "sybase";
		public static String DERBY = "derby";		
		
	}
	
	
	/**
	 * These bitmask are used by the test to indicate which database type(s) a specific
	 * test is not supported to run against.   
	 * 
	 * The exclusion postion was taken because the goal is that all test should be able to
	 * run against all sources.   However, there are cases where specific database type
	 * test are needed.
	 * 
	 * 
	 * @author vanhalbert
	 *
	 */
	public static interface ExclusionTypeBitMask{
		
		// Constants to hold bit masks for desired flags
		static final int NONE_EXCLUDED = 0;  //         000...00000000 (empty mask)
		static final int MYSQL = 1;    // 2^^0    000...00000001
		static final int ORACLE = 2;    // 2^^1    000...00000010
		static final int POSTGRES = 4;    // 2^^2    000...00000100
		static final int SQLSERVER = 8;    // 2^^3    000...00001000
		static final int DB2 = 16;   // 2^^4    000...00010000
		static final int SYBASE = 32;   // 2^^5    000...00100000
		static final int DERBY = 64;   // 2^^6    000...01000000
		
	}
	// don't include excluded
	public static int NumBitMasks = 7;
//	
//	static int ALLOWABLE_DATA_TYPES = ExclusionTypeBitMask.MYSQL |
//										ExclusionTypeBitMask.ORACLE |
//										ExclusionTypeBitMask.POSTGRES |
//										ExclusionTypeBitMask.SQLSERVER |
//										ExclusionTypeBitMask.DB2 |
//										ExclusionTypeBitMask.SYBASE |
//										ExclusionTypeBitMask.DERBY;
	
	static {
		dbtypeBitMaskMap = new HashMap<String, String>(NumBitMasks );
		dbtypeBitMaskMap.put(DataSourcTypes.MYSQL, String.valueOf(ExclusionTypeBitMask.MYSQL));
		dbtypeBitMaskMap.put(DataSourcTypes.ORACLE, String.valueOf(ExclusionTypeBitMask.ORACLE));
		dbtypeBitMaskMap.put(DataSourcTypes.POSTRES, String.valueOf(ExclusionTypeBitMask.POSTGRES));
		dbtypeBitMaskMap.put(DataSourcTypes.DB2, String.valueOf(ExclusionTypeBitMask.DB2));
		dbtypeBitMaskMap.put(DataSourcTypes.SQLSERVER, String.valueOf(ExclusionTypeBitMask.SQLSERVER));
		dbtypeBitMaskMap.put(DataSourcTypes.SYBASE, String.valueOf(ExclusionTypeBitMask.SYBASE));
		dbtypeBitMaskMap.put(DataSourcTypes.DERBY, String.valueOf(ExclusionTypeBitMask.DERBY));
		
	}
	
	public void setBitMask() {
		int rtn = ExclusionTypeBitMask.NONE_EXCLUDED;
		
		String bitmask = dbtypeBitMaskMap.get(dbtype);
		if (bitmask == null) {
			bitMask = rtn;
		} else {
			bitMask =  new Integer(bitmask).intValue();
		}
	}
	

}
