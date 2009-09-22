/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.util.StringUtil;

import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;

/**
 * The DataSourceMgr is responsible for loading and managing datasources defined by the datasource
 * mapping properties file {@see #DATASOURCE_MAPPING_FILE} and the mapped
 * datasource properties files. The {@link #getDatasourceProperties(String)}
 * returns the properties defined for that datasourceid, which is mapped in the
 * mnappings file. This mapping allows the test
 * 
 * @author vanhalbert
 * 
 */
public class DataSourceMgr {
	

	static final String RELATIVE_DIRECTORY = "datasources/";
	static final String DATASOURCE_MAPPING_FILE = "datasource_mapping.xml";

	private static DataSourceMgr _instance = null;
	
	private Map<String, Map<String, DataSource>>dsGroupMap = new HashMap<String, Map<String, DataSource>>();  //key=datasourcetype

	private Map<String, DataSource> allDatasourcesMap = new HashMap<String, DataSource>();  // key=datasource name
	
	private Map<String, DataSource> modelToDatasourceMap = new HashMap<String, DataSource>();  // key=modelname + "_" + datasourcename
																								// key=modelname + "_" + groupname
	
	private Set<String> dbTypes = new HashSet<String>(); 
	// this set is use to track datasources that have already been assigned
	private Set<String> assignedDataSources = new HashSet<String>();
	
	private int lastassigned = 0;


	private DataSourceMgr() {
	}

	public static synchronized DataSourceMgr getInstance() {
		if (_instance == null) {
			_instance = new DataSourceMgr();
			try {
				_instance.loadDataSourceMappings();
			} catch (QueryTestFailedException e) {
				// TODO Auto-generated catch block
				throw new TransactionRuntimeException(e);
			} catch (TransactionRuntimeException e) {
				// TODO Auto-generated catch block
				throw e;
			}

		}
		return _instance;
	}
	
	/**
	 * reset is called when a predetermined set of datasources are going to be set
	 * This is to ensure any exclusions / inclusions are considered for the 
	 * next executed set of test.
	 * 
	 *
	 * @since
	 */
	public static synchronized void reset() {
		if (_instance == null) return;
		//
		_instance.modelToDatasourceMap.clear();
		_instance.assignedDataSources.clear();
		
	}
	
	public int numberOfAvailDataSources() {
		return allDatasourcesMap.size();
	}
	
//	public boolean hasAvailableDataSource(int numRequiredDataSources) {
//		// reset the mapping at the start of each test
//		_instance.modelToDatasourceMap.clear();		
//		
//		Set<String>excludedDBTypes = getExcludedDBTypes();
//		
//		Set<String> xSet = new HashSet<String>(dbTypes);
//		
//		xSet.removeAll(excludedDBTypes);		
//		
//		return(numRequiredDataSources <=  (xSet.size())); 
//		
//	}
	
	private Set<String> getExcludedDBTypes() {

		String excludeprop = ConfigPropertyLoader.getProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP);
		
		if (excludeprop == null || excludeprop.length() == 0) {
			return  Collections.EMPTY_SET;
		}
		Set<String> excludedDBTypes = new HashSet<String>(3);

		List<String> eprops = StringUtil.split(excludeprop, ",");
		excludedDBTypes.addAll(eprops);
		return excludedDBTypes;
	
	}
	
	
	public DataSource getDatasource(String datasourceid, String modelName)
			throws QueryTestFailedException {
		DataSource ds = null;
		
		// map the datasource to the model and datasourceid
		// this is so the next time this combination is requested,
		// the same datasource is returned to ensure when consecutive calls during the process
		// corresponds to the same datasource
		String key = null;
		
		key = modelName + "_"+datasourceid;
		
		// if the datasourceid represents a group name, then use the group name
		// so that all future request using that group name for a specified model
		// will use the same datasource
		if (modelToDatasourceMap.containsKey(key)) {
			return modelToDatasourceMap.get(key);
		} 

		Set excludedDBTypes = getExcludedDBTypes();

		if (dsGroupMap.containsKey(datasourceid)) {

			Map datasources = dsGroupMap.get(datasourceid);
			Iterator<DataSource> it= datasources.values().iterator();
			
			// need to go thru all the datasources to know if any has already been assigned
			// because the datasourceid passed in was a group name
			while(it.hasNext()) {
				DataSource checkit = it.next();
				
				if (excludedDBTypes.contains(checkit.getDBType())) {
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
				Iterator<DataSource> itds= datasources.values().iterator();
				
				// when all the datasources have been assigned, but a new model datasource id is
				// passed in, need to reassign a previously assigned datasource
				// This case will happen when more models are defined than there are defined datasources.
				while(itds.hasNext()) {
					DataSource datasource = itds.next();
					if (cnt == this.lastassigned) {
						ds = datasource;
						
						this.lastassigned++;
						if (lastassigned >= datasources.size()) {
							this.lastassigned = 0;
						}
						
						break;
					}
				}

			}

		} else {
			ds = allDatasourcesMap.get(datasourceid);
			
			if (excludedDBTypes.contains(ds.getDBType())) {
				ds = null;
			}

		}
		if (ds == null) {
			throw new QueryTestFailedException("DatasourceID " + datasourceid
					+ " is not a defined datasource in the mappings file ");

		}

		modelToDatasourceMap.put(key, ds);
		return ds;

	}


	private void loadDataSourceMappings()
			throws QueryTestFailedException {
		
		Set<String> limitds = new HashSet<String>();
        String limitdsprop = ConfigPropertyLoader.getProperty(ConfigPropertyNames.USE_DATASOURCES_PROP);
        if (limitdsprop != null && limitdsprop.length() > 0) { 
        	System.out.println("Limit datasources to: " + limitdsprop);
        	List<String> dss = StringUtil.split(limitdsprop, ",");
        	limitds.addAll(dss);
        }

		Document doc = null;
		XMLReaderWriter readerWriter = new XMLReaderWriterImpl();

		try {
			doc = readerWriter.readDocument(getInputStream());
		} catch (JDOMException e) {
			e.printStackTrace();
			throw new TransactionRuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new TransactionRuntimeException(e);
		}

		Element root = doc.getRootElement();
		List<Element> rootElements = root.getChildren();
		if (rootElements == null || rootElements.size() == 0) {
			throw new TransactionRuntimeException("No children defined under root element " + DSCONFIG);
		}
		
		for (Iterator<Element> it = rootElements.iterator(); it.hasNext();) {
			Element type = it.next();
			String groupname = type.getAttributeValue(Property.Attributes.NAME);

			List<Element> typeElements = type.getChildren();
			if (typeElements != null) {
				Map<String, DataSource> datasources = new HashMap<String, DataSource>(typeElements.size());

				for (Iterator<Element> typeit = typeElements.iterator(); typeit.hasNext();) {
					Element e = typeit.next();
					addDataSource(e, groupname, datasources, limitds);				
				}	
				dsGroupMap.put(groupname, datasources);
				allDatasourcesMap.putAll(datasources);

			}			
			
			
		}	

		if (dsGroupMap == null || dsGroupMap.isEmpty()) {
			throw new TransactionRuntimeException(
					"No Datasources were found in the mappings file");
		}
		
		System.out.println("Number of datasource groups loaded " + dsGroupMap.size());
		System.out.println("Number of total datasource mappings loaded " + allDatasourcesMap.size());



	}
	
	private void addDataSource(Element element, String group, Map<String, DataSource> datasources, Set<String> include) {
			String name = element.getAttributeValue(Property.Attributes.NAME);
			
			Properties props = getProperties(element);
			
			String dir = props.getProperty(DataSource.DIRECTORY);
			
			if (include.size() > 0) {
				if (!include.contains(dir)) {
//					System.out.println("Excluded datasource: " + name);
					return;
				}
			}
			
			String dsfile = RELATIVE_DIRECTORY + dir + "/connection.properties";
			Properties dsprops = loadProperties(dsfile);
			
			if (dsprops != null) {
				props.putAll(dsprops);
				DataSource ds = new DataSource(name,
						group,
						props);
				
				dbTypes.add(ds.getDBType());
				datasources.put(ds.getName(), ds);
				System.out.println("Loaded datasource " + ds.getName());

			} 

	}


	private static Properties loadProperties(String filename) {
			Properties props = null;
	
			try {
				InputStream in = DataSourceMgr.class.getResourceAsStream("/"
						+ filename);
				if (in != null) {
					props = new Properties();
					props.load(in);
					return props;
				}
				return null;
			} catch (IOException e) {
				throw new TransactionRuntimeException("Error loading properties from file '"
						+ filename + "'" + e.getMessage());
			}
		}

	private static Properties getProperties(Element propertiesElement) {
		Properties props = new Properties();

		List<Element> properties = propertiesElement
				.getChildren(Property.ELEMENT);
		Iterator<Element> iterator = properties.iterator();
		while (iterator.hasNext()) {
			Element propertyElement = (Element) iterator.next();
			String propertyName = propertyElement
					.getAttributeValue(Property.Attributes.NAME);
			String propertyValue = propertyElement.getText();

			props.setProperty(propertyName, propertyValue);

		}
		return props;
	}

	private static InputStream getInputStream() {

		InputStream in = DataSourceMgr.class.getResourceAsStream("/"
				+ RELATIVE_DIRECTORY + DATASOURCE_MAPPING_FILE);
		if (in != null) {

			return in;
		} else {
			throw new RuntimeException(
					"Failed to load datasource mapping file '" + RELATIVE_DIRECTORY
							+ DATASOURCE_MAPPING_FILE + "'");
		}

	}
	
	static final String DSCONFIG = "datasourceconfig";
	static final String DATASOURCETYPE = "datasourcetype";
	static final String DATASOURCE = "datasource";

	static class Property {

		/**
		 * This is the name of the Property Element.
		 */
		public static final String ELEMENT = "property"; //$NON-NLS-1$

		/**
		 * This class defines the Attributes of the Element class that contains
		 * it.
		 */
		public static class Attributes {
			public static final String NAME = "name"; //$NON-NLS-1$
		}

	}

	public static void main(String[] args) {
		
		
		DataSourceMgr mgr = DataSourceMgr.getInstance();

		try {
			DataSource ds1 = mgr.getDatasource("ds_oracle", "model1");
			
			DataSource ds2 = mgr.getDatasource("nonxa", "model1");
			if (ds1 != ds2) {
				throw new RuntimeException("Datasources are not the same");
			}
			System.out.println("Value for ds_mysql: "
					+ mgr.getDatasource("ds_oracle", "model1").getProperties());
			
//			boolean shouldbeavail = mgr.hasAvailableDataSource("nonxa", DataSource.ExclusionTypeBitMask.ORACLE);
//			if (!shouldbeavail) {
//				throw new RuntimeException("Should have found one available");
//			}
//			
//			shouldbeavail = mgr.hasAvailableDataSource("nonxa", DataSource.ExclusionTypeBitMask.ORACLE | DataSource.ExclusionTypeBitMask.MYSQL);
//			if (!shouldbeavail) {
//				throw new RuntimeException("Should have found one available");
//			}
//
//			
//			boolean shouldnot = mgr.hasAvailableDataSource("nonxa", DataSource.ExclusionTypeBitMask.ORACLE | DataSource.ExclusionTypeBitMask.MYSQL | DataSource.ExclusionTypeBitMask.SQLSERVER);
//			if (shouldnot) {
//				throw new RuntimeException("Should NOT have found one available");
//			}
		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
		
		DataSourceMgr.reset();
		
		System.setProperty(ConfigPropertyNames.USE_DATASOURCES_PROP, "ds_sqlserver");
		
		mgr = DataSourceMgr.getInstance();

		try {
			
			DataSource dsfind = mgr.getDatasource("ds_sqlserver", "model1");
			if (dsfind == null) {
				throw new RuntimeException("The special included datasource was not found");
				
			}
			System.out.println("Datasource :" + dsfind.getName() + " was found");
			
			try {
				DataSource dsnotfound = mgr.getDatasource("ds_oracle", "model1");
				if (dsnotfound != null) {
					throw new RuntimeException("The special excluded datasource was found");
					
				}
			} catch (QueryTestFailedException qtf) {
			
				System.out.println("Datasource:  ds_oracle: was not found and should not have");
			}

		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}
	}

}
