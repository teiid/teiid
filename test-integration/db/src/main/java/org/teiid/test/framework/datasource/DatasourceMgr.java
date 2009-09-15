/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.io.IOException;
import java.io.InputStream;
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
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;

/**
 * The DataSourceMgr is responsible for loading and managing the datasource
 * mapping properties file {@see #DATASOURCE_MAPPING_FILE} and the mapped
 * datasource properties files. The {@link #getDatasourceProperties(String)}
 * returns the properties defined for that datasourceid, which is mapped in the
 * mnappings file. This mapping allows the test
 * 
 * @author vanhalbert
 * 
 */
public class DataSourceMgr {

	static final String DIRECTORY = "datasources/";
	static final String DATASOURCE_MAPPING_FILE = "datasource_mapping.xml";

	private static DataSourceMgr _instance = null;
	
	private Map<String, Map<String, DataSource>>dstypeMap = new HashMap<String, Map<String, DataSource>>();  //key=datasourcetype

	private Map<String, DataSource> allDatasourcesMap = new HashMap<String, DataSource>();  // key=datasource name
	
	private Map<String, DataSource> modelToDatasourceMap = new HashMap<String, DataSource>();  // key=modelname
	
	private Set<String> usedDataSources = new HashSet<String>();


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
	
	public int numberOfAvailDataSources() {
		return allDatasourcesMap.size();
	}
	
	public DataSource getDatasource(String datasourceid, String modelName)
			throws QueryTestFailedException {
		DataSource ds = null;
		
		// map the datasource to the model and datasourceid
		// this is so the next time this combination is requested,
		// the same datasource is returned to ensure when consecutive calls during the process
		// corresponds to the same datasource
		String key = modelName + "_"+datasourceid;
		
		if (modelToDatasourceMap.containsKey(key)) {
			return modelToDatasourceMap.get(key);
		} 
		if (dstypeMap.containsKey(datasourceid)) {

			Map datasources = dstypeMap.get(datasourceid);
			Iterator<DataSource> it= datasources.values().iterator();
			while(it.hasNext()) {
				DataSource checkit = it.next();
				if (!usedDataSources.contains(checkit.getName())) {
					usedDataSources.add(checkit.getName());
					ds = checkit;
					break;
				}
			}

		} else {
			ds = allDatasourcesMap.get(datasourceid);
		}
		if (ds == null) {
			throw new QueryTestFailedException("DatasourceID " + datasourceid
					+ " is not a defined datasource in the mappings file ");

		}
		
		modelToDatasourceMap.put(key, ds);
		return ds;

	}

	public Properties getDatasourceProperties(String datasourceid, String modelname)
			throws QueryTestFailedException {
		DataSource ds = getDatasource(datasourceid, modelname);

		return ds.getProperties();
		
	}

	private void loadDataSourceMappings()
			throws QueryTestFailedException {

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
//			System.out.println("Loading ds transactional type  " + type.getName());
			String typename = type.getAttributeValue(Property.Attributes.NAME);

			List<Element> typeElements = type.getChildren();
			if (typeElements != null) {
				Map<String, DataSource> datasources = new HashMap<String, DataSource>(typeElements.size());

				for (Iterator<Element> typeit = typeElements.iterator(); typeit.hasNext();) {
					Element e = typeit.next();
//					System.out.println("Loading ds type  " + e.getName());
					addDataSource(e, typename, datasources);				
				}	
				dstypeMap.put(typename, datasources);
				allDatasourcesMap.putAll(datasources);

			}			
			
			
		}	

		if (dstypeMap == null || dstypeMap.isEmpty()) {
			throw new TransactionRuntimeException(
					"No Datasources were found in the mappings file");
		}
		
		System.out.println("Number of datasource types loaded " + dstypeMap.size());
		System.out.println("Number of total datasource mappings loaded " + allDatasourcesMap.size());



	}
	
	private static void addDataSource(Element element, String type, Map<String, DataSource> datasources) {
			String name = element.getAttributeValue(Property.Attributes.NAME);
			Properties props = getProperties(element);
			
			String dir = props.getProperty(DataSource.DIRECTORY);
			String dsfile = DIRECTORY + dir + "/connection.properties";
			Properties dsprops = loadProperties(dsfile);
			if (dsprops != null) {
				props.putAll(dsprops);
				DataSource ds = new DataSource(name,
						type,
						props);
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
				+ DIRECTORY + DATASOURCE_MAPPING_FILE);
		if (in != null) {

			return in;
		} else {
			throw new RuntimeException(
					"Failed to load datasource mapping file '" + DIRECTORY
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
			DataSource ds1 = mgr.getDatasource("ds_mysql5", "model1");
			
			DataSource ds2 = mgr.getDatasource("ds_mysql5", "model1");
			if (ds1 != ds2) {
				throw new RuntimeException("Datasources are not the same");
			}
			System.out.println("Value for ds_mysql5: "
					+ mgr.getDatasourceProperties("ds_mysql5", "model1"));
		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}

	}

}
