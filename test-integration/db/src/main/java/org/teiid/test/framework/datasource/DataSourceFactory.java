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

	// contains the names of the datasources when the -Dusedatasources option is
	// used
	private Map<String, String> useDS = null;
	private Set<String> excludedDBTypes = null;

	private Set<String> unmappedds = new HashSet<String>();
	
	private Map<String, DataSource> allDatasourcesMap = null;

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
	 * @since
	 */
	private void config() {
		System.out.println("Configure Datasource Factory ");
		
		this.allDatasourcesMap = DataSourceMgr.getInstance().getDataSources();

		String limitdsprop = configprops
				.getProperty(ConfigPropertyNames.USE_DATASOURCES_PROP);
		if (limitdsprop != null && limitdsprop.length() > 0 && ! limitdsprop.equalsIgnoreCase(DO_NOT_USE_DEFAULT)) {
			System.out.println("Use ONLY datasources: " + limitdsprop);

			List<String> dss = StringUtil.split(limitdsprop, ",");

			useDS = new HashMap<String, String>(dss.size());
			int i = 1;
			for (Iterator<String> it = dss.iterator(); it.hasNext(); i++) {
				useDS.put(String.valueOf(i), it.next());
			}

		}

		String excludeprop = configprops
				.getProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP);

		if (excludeprop != null && excludeprop.length() > 0) {
			List<String> eprops = StringUtil.split(excludeprop, ",");
			excludedDBTypes = new HashSet<String>(eprops.size());
			excludedDBTypes.addAll(eprops);
			System.out.println("EXCLUDE datasources: " + excludeprop);
		}
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
				ds = allDatasourcesMap.get(dsname);
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

		Iterator<DataSource> it = allDatasourcesMap.values().iterator();

		// need to go thru all the datasources to know if any has already been
		// assigned
		// because the datasourceid passed in was a group name
		while (it.hasNext()) {
			DataSource checkit = it.next();

			if (excludedDBTypes != null
					&& excludedDBTypes.contains(checkit.getDBType())) {
				continue;
			}

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

					ds = allDatasourcesMap.get(dsname);

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
		unmappedds.clear();
		assignedDataSources.clear();

		useDS = null;
		excludedDBTypes = null;

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
		System.setProperty(ConfigPropertyNames.USE_DATASOURCES_PROP, "oracle,sqlserver");
		
		config = ConfigPropertyLoader.createInstance();
				
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
			
			
			
			System.setProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP, "sqlserver");
			
			config = ConfigPropertyLoader.createInstance();
	
			
			factory = new DataSourceFactory(config);

				
				DataSource ds1 = factory.getDatasource("1", "model1");
				if (ds1 == null) {
					throw new RuntimeException("No 1st datasource was found");
					
				}
				
				DataSource ds2 = factory.getDatasource("2", "model1");
				if (ds2 == null) {
					throw new RuntimeException("No 2nd datasource was found");
					
				}
				
				DataSource reuse = factory.getDatasource("3", "model1");
				if (reuse == ds1 || reuse == ds2) {
					
				} else {
					throw new RuntimeException("The process was not able to reassign an already used datasource");
					
				}

		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
	}

}
