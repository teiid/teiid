package org.teiid.test.framework.datasource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;



/**
 * The DataSourceFactory is responsible for managing the datasources used during a single test.
 * It ensures the same data source is used in both the connector binding and during validation to ensure validation is performed
 * against the same datasource the test was performed on.
 *
 * The following are the available options for controlling which datasources are used during a test:
 * <li>Control which datasources are used and to which model they are assigned</li>
 * 
 * <p>
 * Use the {@link ConfigPropertyNames#USE_DATASOURCES_PROP} property to specify a comma delimited ordered list to control which data sources to use. 
 * 
 * <br><br>
 * This will enable integration testing to be setup to use well known combinations, to ensure coverage and makes it easier to replicate issues.   
 * 
 * This indicates to use only the specified datasources for this test.   This option can be used in test cases where only 
 * specific sources are to be used and tested against.
 * 
 * This ordered list will map to the model:order mapping in the config.properties file.
 * <br><br>
 * Example:  in the config.properties file, the models in the test and their order can be specified:
 * 	<li>		pm1=1	</li>
 * 	<li>		pm2=2	</li>
 * <br>
 * 
 * Then set property: ConfigPropertyNames.USE_DATASOURCES_PROP=oracle,sqlserver
 * 
 * This will use oracle and sqlserver datasources in the test, and when a datasource is requested for model "pm1", the oracle datasource will be returned.  And when a data source
 * for "pm2" is requested, the data source mapped to 2nd ordered datasource of sqlserver will be returned. 
 *
 * </p>
 * <li>Control which datasources of a specific database type to exclude</li>
 * <p>
 *
 * Use the {@link ConfigPropertyNames#EXCLUDE_DATASBASE_TYPES_PROP} property to specify a comma delimited list of {@link DataBaseTypes} to exclude.
 * <br><br>
 * This will remove all datasources of a specific database type from the list of available datasources.  This option will be applied after 
 * {@link ConfigPropertyNames#USE_DATASOURCES_PROP} option.   This is done because there are some test that will not work with certain database
 * types, and therefore, if the required datasources are not met, the test will be bypassed for execution.
 *</p>
 * <li>Control for a specfic model, which database type of datasource to be assigned</li>
 * <p>
 * This option gives the developer even more fine grain control of how datasources are assigned.   There are cases where a specific model must be assigned
 * to a datasource of a specific database type.
 * 
 * To use this option, 
 *  
 * </p>
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class DataSourceFactory {
	
	/**
	 * These types match the "ddl" directories for supported database types
	 * and it also found in the datasources connection.properties defined by the DB_TYPE property
	 * These are also the values used to specify the required database types.
	 *
	 */
	public static interface DataBaseTypes{
		public static String MYSQL = "mysql";
		public static String ORACLE = "oracle";
		public static String POSTRES = "postgres";
		public static String SQLSERVER = "sqlserver";
		public static String DB2 = "db2";
		public static String SYBASE = "sybase";
		public static String DERBY = "derby";	
		public static String ANY = "any";
		
	}

	// the DO_NO_USE_DEFAULT will be passed in when the test are run from maven and no property is passed in for UseDataSources 
	private static final String DO_NOT_USE_DEFAULT="${" + ConfigPropertyNames.USE_DATASOURCES_PROP + "}";
		
	private Properties configprops;
	
	// contains the names of the datasources when the -Dusedatasources option is used
	private Map<String, String> useDS = null;
	
	// contains all the datasources available to be used
	private Map<String, DataSource> availDS = null;

	
	// contains any dbtype preconditions on a model, which requires a model to be assigned a certain database type
	private Map<String, String> requiredDataBaseTypes = null; // key=modelname  value=dbtype

	// this set is use to track datasources that have already been assigned
	private Set<String> assignedDataSources = new HashSet<String>();

	private int lastassigned = 0;
	
	// indicates if the datasource requirements have been, it will be false in the case
	// a specific dbtype is required and that type was not one of the available types defined
	private boolean metDBRequiredTypes = true;
	
	// indicates that there are required dbtypes to consider
	private boolean hasRequiredDBTypes = false;

	
	public DataSourceFactory(ConfigPropertyLoader config) {
	    	this.configprops = PropertiesUtils.clone(config.getProperties(), null, true);
		this.requiredDataBaseTypes = config.getModelAssignedDatabaseTypes();

	}
	
	public Properties getConfigProperties() {
	    return this.configprops;
	}

	/**
	 * config is called at the start / setup of the {@link
	 * TransactionContainer#} test. This is to ensure any exclusions /
	 * inclusions are considered for the next executed set of test.
	 * 
	 * 
	 * 	1st, check for the usedatasource property, if exist, then only add those specific datasources
	 * 		to the useDataSources, otherwise add all available.
	 *  2nd, if the exclude option is used, then remove any excluded datasources from the useDataSources.
	 *  
	 * @since
	 */
	protected void config(DataSourceMgr dsmgr) {
	    TestLogger.logDebug("Configure Datasource Factory ");
	    
	    	dsmgr = DataSourceMgr.getInstance();
		
		Map<String, DataSource> availDatasources = dsmgr.getDataSources();
		
		availDS = new HashMap<String, DataSource>(availDatasources.size());
		
		String usedstypeprop = configprops
		.getProperty(ConfigPropertyNames.USE_DATASOURCE_TYPES_PROP);
		
		Set<String> useDBTypes = null;

		if (usedstypeprop != null && usedstypeprop.length() > 0) {
			List<String> eprops = StringUtil.split(usedstypeprop, ",");
			useDBTypes = new HashSet<String>(eprops.size());
			useDBTypes.addAll(eprops);
			System.out.println("EXCLUDE datasources: " + usedstypeprop);
		} else {
		    useDBTypes = Collections.EMPTY_SET;
		}
		
		String excludeprop = configprops
		.getProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP);
		
		Set<String> excludedDBTypes = null;

		if (excludeprop != null && excludeprop.length() > 0) {
			List<String> eprops = StringUtil.split(excludeprop, ",");
			excludedDBTypes = new HashSet<String>(eprops.size());
			excludedDBTypes.addAll(eprops);
			System.out.println("EXCLUDE datasources: " + excludeprop);
		} else {
			excludedDBTypes = Collections.EMPTY_SET;
		}

		
		String limitdsprop = configprops
				.getProperty(ConfigPropertyNames.USE_DATASOURCES_PROP);
		if (limitdsprop != null && limitdsprop.length() > 0 && ! limitdsprop.equalsIgnoreCase(DO_NOT_USE_DEFAULT)) {
		    TestLogger.log("Use ONLY datasources: " + limitdsprop);

			List<String> dss = StringUtil.split(limitdsprop, ",");

			useDS = new HashMap<String, String>(dss.size());
			DataSource ds = null;
			int i = 1;
			for (Iterator<String> it = dss.iterator(); it.hasNext(); i++) {
				String dssName = it.next();
				ds = availDatasources.get(dssName);
				
				if (ds != null && !excludedDBTypes.contains(ds.getDBType())) {
				
					useDS.put(String.valueOf(i), dssName);
				
					availDS.put(dssName, ds);
					TestLogger.logInfo("Using ds: " + dssName);
				}
				
			}

		} else {
			for (Iterator<DataSource> it = availDatasources.values().iterator(); it.hasNext(); ) {
				DataSource ds = it.next();
				// if the datasource type is not excluded, then consider for usages
				if (!excludedDBTypes.contains(ds.getDBType())) {
				    
				    // if use a specific db type is specified, then it must match,
				    // otherwise add it to the available list
				    if (useDBTypes.size() > 0)  {
				            if ( usedstypeprop.contains(ds.getDBType())) {
				        	availDS.put(ds.getName(), ds);
				            } 
				    } else {
					availDS.put(ds.getName(), ds);
				    }
				    
				}
			}
		    
		    
		}

		
		
		if (requiredDataBaseTypes != null && requiredDataBaseTypes.size() > 0) {
			this.hasRequiredDBTypes = true;
			
			Iterator<String> rit = this.requiredDataBaseTypes.keySet().iterator();

			// go thru all the datasources and remove those that are excluded
				while (rit.hasNext()) {
					String modelName = rit.next();
					String rdbtype = this.requiredDataBaseTypes.get(modelName);
					
					Iterator<DataSource> ait = availDS.values().iterator();

					metDBRequiredTypes = false;
					
					// go thru all the datasources and find the matching datasource of the correct dbtype
					while (ait.hasNext()) {
						DataSource ds = ait.next();
						if (ds.getDBType().equalsIgnoreCase(rdbtype)) {
							assignedDataSources.add(ds.getName());

							dsmgr.setDataSource(modelName, ds);
						//	modelToDatasourceMap.put(modelName, ds);
							metDBRequiredTypes = true;
				
						}
						
					}
					
					if (!metDBRequiredTypes) {
						// did find a required dbtype, no need going any further
						break;
					}
					
				}
								
		}
		
		
	}
	
	public int getNumberAvailableDataSources() {
		return (metDBRequiredTypes ? this.availDS.size() :0);
	}

	public synchronized DataSource getDatasource(String modelName) throws QueryTestFailedException {
		DataSource ds = null;

		// map the datasource to the model and datasourceid
		// this is so the next time this combination is requested,
		// the same datasource is returned to ensure when consecutive calls
		// during the process
		// corresponds to the same datasource
		String key = null;

		key = modelName;
		//+ "_" + datasourceid;

		// if the datasourceid represents a group name, then use the group name
		// so that all future request using that group name for a specified
		// model
		// will use the same datasource
		
				
		if (this.hasRequiredDBTypes) {
			if (this.requiredDataBaseTypes.containsKey(modelName)) {
				String dbtype = this.requiredDataBaseTypes.get(modelName);
				
				Iterator<DataSource> it = availDS.values().iterator();

				// need to go thru all the datasources to know if any has already been
				// assigned
				// because the datasourceid passed in was a group name
				while (it.hasNext()) {
					DataSource checkit = it.next();

					if (dbtype.equalsIgnoreCase(checkit.getDBType())) {
						ds = checkit;
						break;
					}

				}

				
			}
			
		} else if (useDS != null) {
				String dsname = useDS.get(modelName);
				if (dsname != null) {
					ds = availDS.get(dsname);
					if (ds == null) {
						throw new QueryTestFailedException("Datasource name "
								+ dsname
								+ " was not found in the allDatasources map");
	
					}
				} 
	

		} else {
		
			Iterator<DataSource> it = availDS.values().iterator();
	
			// need to go thru all the datasources to know if any has already been
			// assigned
			// because the datasourceid passed in was a group name
			while (it.hasNext()) {
				DataSource checkit = it.next();
	
				if (!assignedDataSources.contains(checkit.getName())) {
					ds = checkit;
					break;
				}
	
			}
		}

		if (ds == null) {
			int cnt = 0;
			Iterator<String> itds = assignedDataSources.iterator();

			// when all the datasources have been assigned, but a new model
			// datasource id is
			// passed in, need to reassign a previously assigned datasource
			// This case will happen when more models are defined than there are
			// defined datasources.
			while (itds.hasNext()) {
				String dsname = itds.next();

				if (cnt == this.lastassigned) {

					ds = availDS.get(dsname);

					this.lastassigned++;
					if (lastassigned >= assignedDataSources.size()) {
						this.lastassigned = 0;
					}

					break;
				}
			}

		}
		
		if (ds != null) {
		    assignedDataSources.add(ds.getName());
		}

		return ds;

	}

	public void cleanup() {

		assignedDataSources.clear();
//		requiredDataBaseTypes.clear();

		if (useDS != null) useDS.clear();
//		if (availDS != null) availDS.clear();

	}
	
	public static void main(String[] args) {
		//NOTE: to run this test to validate the DataSourceMgr, do the following:
		//   ---  need 3 datasources,   Oracle, SqlServer and 1 other
		
		ConfigPropertyLoader config = ConfigPropertyLoader.getInstance();
		
		DataSourceFactory factory = new DataSourceFactory(config);

		try {
			if (factory.getDatasource("model1") == null) {
				throw new TransactionRuntimeException("No datasource was not found");
			}
			

		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
		
		factory.cleanup();
		
		ConfigPropertyLoader.reset();

		
		// the following verifies that order of "use" datasources is applied to request for datasources.
		config = ConfigPropertyLoader.getInstance();
		
		config.setProperty(ConfigPropertyNames.USE_DATASOURCES_PROP, "oracle,sqlserver");
				
		factory = new DataSourceFactory(config);

		try {
			
			DataSource dsfind = factory.getDatasource( "model2");
			if (dsfind == null) {
				throw new TransactionRuntimeException("No datasource was not found as the 2nd datasource");
				
			}
			
			if (dsfind.getConnectorType() == null) {
				throw new TransactionRuntimeException("Connector types was not defined");
			}
			
			if (!dsfind.getName().equalsIgnoreCase("sqlserver")) {
				throw new TransactionRuntimeException("Sqlserver was not found as the 2nd datasource");
				
			}
			
			dsfind = factory.getDatasource( "model1");
			if (dsfind == null) {
				throw new TransactionRuntimeException("No datasource was not found as the 2nd datasource");
				
			}
			if (!dsfind.getName().equalsIgnoreCase("oracle")) {
				throw new TransactionRuntimeException("Oracle was not found as the 2nd datasource");
				
			}
			System.out.println("Datasource :" + dsfind.getName() + " was found");
			
			
			// the following test verifies that a sqlserver datasource is not
			// returned (excluded)
			factory.cleanup();
		
			ConfigPropertyLoader.reset();


			config = ConfigPropertyLoader.getInstance();
			config.setProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP, "sqlserver");

			
			factory = new DataSourceFactory(config);

			int n = factory.getNumberAvailableDataSources();
			TestLogger.log("Num avail datasources: " + n);

			for (int i=0; i<n; i++) {
				
				String k = String.valueOf(i);
				DataSource ds1 = factory.getDatasource( "model" + k);
				if (ds1 == null) {
					throw new TransactionRuntimeException("No datasource was found for: model:" + k);
					
				} if (ds1.getDBType().equalsIgnoreCase(DataSourceFactory.DataBaseTypes.SQLSERVER)) {
					throw new TransactionRuntimeException("sqlserver dbtype should have been excluded");
				}
			}
				
				
				DataSource reuse = factory.getDatasource( "model1");
				if (reuse != null) {
					
				} else {
					throw new TransactionRuntimeException("The process was not able to reassign an already used datasource");
					
				}
				
				factory.cleanup();
				
				ConfigPropertyLoader.reset();

				
				// test required database types
				// test 1 source

				config = ConfigPropertyLoader.getInstance();
				
				config.setModelAssignedToDatabaseType("pm1", DataSourceFactory.DataBaseTypes.ORACLE);
			
				factory = new DataSourceFactory(config);

				DataSource ds1 = factory.getDatasource("pm1");
				if (!ds1.getDBType().equalsIgnoreCase(DataSourceFactory.DataBaseTypes.ORACLE)) {
					throw new TransactionRuntimeException("Required DB Type of oracle for model pm1 is :" + ds1.getDBType());
				}
				
				TestLogger.log("Test1 Required DS1 " + ds1.getDBType());
				factory.cleanup();
				
				ConfigPropertyLoader.reset();

				// test required database types
				// test 2 sources, 1 required and other ANY
				config = ConfigPropertyLoader.getInstance();
			
				
				config.setModelAssignedToDatabaseType("pm2", DataSourceFactory.DataBaseTypes.SQLSERVER);
				config.setModelAssignedToDatabaseType("pm1", DataSourceFactory.DataBaseTypes.ANY);
			
				factory = new DataSourceFactory(config);

				DataSource ds2 = factory.getDatasource("pm2");
				if (!ds2.getDBType().equalsIgnoreCase(DataSourceFactory.DataBaseTypes.SQLSERVER)) {
					throw new TransactionRuntimeException("Required DB Type of sqlserver for model pm2 is :" + ds2.getDBType());
				}
				TestLogger.log("Test2 Required DS2 " + ds2.getDBType());
			
				factory.cleanup();
				ConfigPropertyLoader.reset();

				
				// test required database types
				// test 2 sources, 2 required 
				config = ConfigPropertyLoader.getInstance();
			
				
				config.setModelAssignedToDatabaseType("pm2", DataSourceFactory.DataBaseTypes.SQLSERVER);
				config.setModelAssignedToDatabaseType("pm1", DataSourceFactory.DataBaseTypes.ORACLE);
			
				factory = new DataSourceFactory(config);

				DataSource ds3a = factory.getDatasource("pm2");
				if (!ds3a.getDBType().equalsIgnoreCase(DataSourceFactory.DataBaseTypes.SQLSERVER)) {
					throw new TransactionRuntimeException("Required DB Type of sqlserver for model pm12 is :" + ds3a.getDBType());
				}
				
				DataSource ds3b = factory.getDatasource("pm1");
				if (!ds3b.getDBType().equalsIgnoreCase(DataSourceFactory.DataBaseTypes.ORACLE)) {
					throw new TransactionRuntimeException("Required DB Type of oracle for model pm1  is :" + ds3b.getDBType());
				}
				TestLogger.log("Test3 Required DS3a " + ds3a.getDBType());
				TestLogger.log("Test3 Required DS3b " + ds3b.getDBType());
				
				factory.cleanup();
			
				
		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
	}

}
