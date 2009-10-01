package org.teiid.test.framework.datasource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.exception.QueryTestFailedException;

import com.metamatrix.core.util.StringUtil;


/**
 * The DataSourceFactory is responsible for managing the datasources used during a single test.
 * It ensures the same data source is used in both the connector binding and during validation.
 * Otherwise, validation may be validating against a source for which the test didn't run.
 * 
 * A test has the option of specifying the following properties to control which data sources are / or not used
 * 
 * <li>{@link ConfigPropertyNames#USE_DATASOURCES_PROP} : is a comma delimited ordered list to control which data sources to use</li>
 * 
 * This is an ordered list because based on the model:order mapping in the config.properties file will map to the ordered list
 * respectfully.
 * 
 * Example:  in the config.properties may specify:
 * 			pm1=1
 * 			pm2=2
 * 
 * Set ConfigPropertyNames.USE_DATASOURCES_PROP=oracle,sqlserver
 * 
 * This indicates that when a datasource is requested for "pm1", then oracle datasource will be returned.  And when a data source
 * for "pm2" is requested, then a data source representing sqlserver will be returned. 
 * 
 * <li>{@link ConfigPropertyNames#EXCLUDE_DATASBASE_TYPES_PROP} : is a comma delimited list indicating which data sources not use
 * during that specific test.
 * 
 * @author vanhalbert
 *
 */
public class DataSourceFactory {

	// the DO_NO_USE_DEFAULT will be passed in when the test are run from maven and no property is passed in for UseDataSources 
	private static final String DO_NOT_USE_DEFAULT="${usedatasources}";
	private ConfigPropertyLoader configprops;

	// contains the names of the datasources when the -Dusedatasources option is used
	private Map<String, String> useDS = null;
	
	private Map<String, DataSource> useDataSources = null;

	// map of the datasources assigned to with model
	private Map<String, DataSource> modelToDatasourceMap = new HashMap<String, DataSource>(); // key
																								// =
																								// modelname
																								// +
																								// "_"
																								// +
																								// datasourceid

	// this set is use to track datasources that have already been assigned
	private Set<String> assignedDataSources = new HashSet<String>();

	private int lastassigned = 0;

	public DataSourceFactory(ConfigPropertyLoader config) {
		this.configprops = config;
		config();
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
	private void config() {
		System.out.println("Configure Datasource Factory ");
		
		Map<String, DataSource> availDatasources = DataSourceMgr.getInstance().getDataSources();
		
		useDataSources = new HashMap<String, DataSource>(availDatasources.size());
		
		String limitdsprop = configprops
				.getProperty(ConfigPropertyNames.USE_DATASOURCES_PROP);
		if (limitdsprop != null && limitdsprop.length() > 0 && ! limitdsprop.equalsIgnoreCase(DO_NOT_USE_DEFAULT)) {
			System.out.println("Use ONLY datasources: " + limitdsprop);

			List<String> dss = StringUtil.split(limitdsprop, ",");

			useDS = new HashMap<String, String>(dss.size());
			DataSource ds = null;
			int i = 1;
			for (Iterator<String> it = dss.iterator(); it.hasNext(); i++) {
				String dssName = it.next();
				useDS.put(String.valueOf(i), dssName);
				ds = availDatasources.get(dssName);
				
				useDataSources.put(dssName, ds);
				
			}

		} else {
			useDataSources.putAll(availDatasources);
		}

		String excludeprop = configprops
				.getProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP);
		
		Set<String> excludedDBTypes = null;

		if (excludeprop != null && excludeprop.length() > 0) {
			List<String> eprops = StringUtil.split(excludeprop, ",");
			excludedDBTypes = new HashSet<String>(eprops.size());
			excludedDBTypes.addAll(eprops);
			System.out.println("EXCLUDE datasources: " + excludeprop);
			
			Iterator<DataSource> it = useDataSources.values().iterator();

			// go thru all the datasources and remove those that are excluded
			while (it.hasNext()) {
				DataSource checkit = it.next();

				if (excludedDBTypes.contains(checkit.getDBType())) {
					it.remove();
				}
			}
			
		}
		
		
	}
	
	public int getNumberAvailableDataSources() {
		return this.useDataSources.size();
	}

	public synchronized DataSource getDatasource(String datasourceid,
			String modelName) throws QueryTestFailedException {
		DataSource ds = null;

		// map the datasource to the model and datasourceid
		// this is so the next time this combination is requested,
		// the same datasource is returned to ensure when consecutive calls
		// during the process
		// corresponds to the same datasource
		String key = null;

		key = modelName + "_" + datasourceid;

		// if the datasourceid represents a group name, then use the group name
		// so that all future request using that group name for a specified
		// model
		// will use the same datasource
		if (modelToDatasourceMap.containsKey(key)) {
			return modelToDatasourceMap.get(key);
		}

		if (useDS != null) {
			String dsname = useDS.get(datasourceid);
			if (dsname != null) {
				ds = useDataSources.get(dsname);
				if (ds == null) {
					throw new QueryTestFailedException("Datasource name "
							+ dsname
							+ " was not found in the allDatasources map");

				}
			} else {
				throw new QueryTestFailedException("Model:id " + modelName
						+ ":" + datasourceid
						+ " did not map to the  usedatasources: "
						+ useDS.toString());

			}

			modelToDatasourceMap.put(key, ds);
			return ds;

		}

		Iterator<DataSource> it = useDataSources.values().iterator();

		// need to go thru all the datasources to know if any has already been
		// assigned
		// because the datasourceid passed in was a group name
		while (it.hasNext()) {
			DataSource checkit = it.next();

//			if (excludedDBTypes != null
//					&& excludedDBTypes.contains(checkit.getDBType())) {
//				continue;
//			}

			if (!assignedDataSources.contains(checkit.getName())) {
				ds = checkit;
				assignedDataSources.add(ds.getName());
				break;
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

					ds = useDataSources.get(dsname);

					this.lastassigned++;
					if (lastassigned >= assignedDataSources.size()) {
						this.lastassigned = 0;
					}

					break;
				}
			}

		}

		if (ds == null) {
			throw new QueryTestFailedException(
					"Unable to assign a datasource for model:datasourceid "
							+ modelName + ":" + datasourceid);

		}

		modelToDatasourceMap.put(key, ds);
		return ds;

	}

	public void cleanup() {

		modelToDatasourceMap.clear();
		assignedDataSources.clear();

		useDS = null;

	}
	
	public static void main(String[] args) {
		//NOTE: to run this test to validate the DataSourceMgr, do the following:
		//   ---  need 3 datasources,   Oracle, SqlServer and 1 other
		
		ConfigPropertyLoader config = ConfigPropertyLoader.createInstance();
		
		DataSourceFactory factory = new DataSourceFactory(config);

		try {
			if (factory.getDatasource("1", "model1") == null) {
				throw new RuntimeException("No datasource was not found");
			}
			

		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
		
		factory.cleanup();

		
		// the following verifies that order of "use" datasources is applied to request for datasources.
		config = ConfigPropertyLoader.createInstance();
		
		config.setProperty(ConfigPropertyNames.USE_DATASOURCES_PROP, "oracle,sqlserver");
				
		factory = new DataSourceFactory(config);

		try {
			
			DataSource dsfind = factory.getDatasource("2", "model1");
			if (dsfind == null) {
				throw new RuntimeException("No datasource was not found as the 2nd datasource");
				
			}
			
			if (dsfind.getConnectorType() == null) {
				throw new RuntimeException("Connector types was not defined");
			}
			
			if (!dsfind.getName().equalsIgnoreCase("sqlserver")) {
				throw new RuntimeException("Sqlserver was not found as the 2nd datasource");
				
			}
			
			dsfind = factory.getDatasource("1", "model1");
			if (dsfind == null) {
				throw new RuntimeException("No datasource was not found as the 2nd datasource");
				
			}
			if (!dsfind.getName().equalsIgnoreCase("oracle")) {
				throw new RuntimeException("Oracle was not found as the 2nd datasource");
				
			}
			System.out.println("Datasource :" + dsfind.getName() + " was found");
			
			
			// the following test verifies that a sqlserver datasource is not
			// returned (excluded)
			factory.cleanup();
		

			config = ConfigPropertyLoader.createInstance();
			config.setProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP, "sqlserver");

			
			factory = new DataSourceFactory(config);

			int n = factory.getNumberAvailableDataSources();
			System.out.println("Num avail datasources: " + n);

			for (int i=0; i<n; i++) {
				
				String k = String.valueOf(i);
				DataSource ds1 = factory.getDatasource(k, "model" + k);
				if (ds1 == null) {
					throw new RuntimeException("No datasource was found for: model:" + k);
					
				}
			}
				
				
				DataSource reuse = factory.getDatasource(String.valueOf(n + 1), "model1");
				if (reuse != null) {
					
				} else {
					throw new RuntimeException("The process was not able to reassign an already used datasource");
					
				}

		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
	}

}
