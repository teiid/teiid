/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.XAConnection;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.connection.ConnectionStrategyFactory;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;

/**
 * The DatasourceMgr is responsible for loading and managing the datasource
 * mapping properties file {@see #DATASOURCE_MAPPING_FILE} and the mapped
 * datasource properties files. The {@link #getDatasourceProperties(String)}
 * returns the properties defined for that datasourceid, which is mapped in the
 * mnappings file. This mapping allows the test
 * 
 * @author vanhalbert
 * 
 */
public class DatasourceMgr {

	static final String DIRECTORY = "datasources/";
	static final String DATASOURCE_MAPPING_FILE = "datasource_mapping.xml";

	private static DatasourceMgr _instance = null;
	
	private Map<String, Map<String, DataSource>>dstypeMap = new HashMap<String, Map<String, DataSource>>();

	private Map<String, DataSource> allDatasourcesMap = new HashMap<String, DataSource>();

	private DatasourceMgr() {
	}

	public static synchronized DatasourceMgr getInstance() {
		if (_instance == null) {
			_instance = new DatasourceMgr();
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
	
	public org.teiid.test.framework.datasource.DataSource getDatasource(String datasourceid)
			throws QueryTestFailedException {
		DataSource ds = null;
		if (dstypeMap.containsKey(datasourceid)) {

			Map datasources = dstypeMap.get(datasourceid);
			ds = (DataSource) datasources.values().iterator().next();

		} else {
			ds = allDatasourcesMap.get(datasourceid);
		}
		if (ds == null) {
			throw new QueryTestFailedException("DatasourceID " + datasourceid
					+ " is not a defined datasource in the mappings file ");

		}
		return ds;

	}

	public Properties getDatasourceProperties(String datasourceid)
			throws QueryTestFailedException {
		DataSource ds = null;
		if (dstypeMap.containsKey(datasourceid)) {
			
			Map datasources = dstypeMap.get(datasourceid);
			ds = (DataSource)datasources.values().iterator().next();
			
		} else {
			ds = allDatasourcesMap.get(datasourceid);
		}
		if (ds == null) {
			throw new QueryTestFailedException("DatasourceID " + datasourceid
					+ " is not a defined datasource in the mappings file ");

		}
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
//			else {
//				System.out.println("Did not load datasource " + name);
//			}

	}


	private static Properties loadProperties(String filename) {
			Properties props = null;
	
			try {
				InputStream in = DatasourceMgr.class.getResourceAsStream("/"
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

		InputStream in = DatasourceMgr.class.getResourceAsStream("/"
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
	
//	static class DS_TYPE {
//
//		/**
//		 * This is the name of the Property Element.
//		 */
//		public static final String XA_ELEMENT = "xa"; //$NON-NLS-1$
//		public static final String NONXA_ELEMENT = "nonxa"; //$NON-NLS-1$
//	
//	}

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
		DatasourceMgr mgr = DatasourceMgr.getInstance();

		try {
			System.out.println("Value for ds_mysql5: "
					+ mgr.getDatasourceProperties("ds_mysql5"));
		} catch (QueryTestFailedException e) {
			e.printStackTrace();
		}

	}

}
