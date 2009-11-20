/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.common.util.PropertiesUtils;

/**
 * The DataSourceMgr is responsible for loading and managing the datasources
 * defined by the datasource connection properties file. There's only a need to
 * load the set of available datasources once for the duration of the entire
 * test suite. And it will maintain one {@link DataSource} for each
 * connection.properties file that it finds.
 * 
 * @author vanhalbert
 * 
 */
public class DataSourceMgr {

    private static DataSourceMgr _instance = null;
    
    /**
     * Defines the default location where the datasource files will be found.
     * An override can be specified by setting the property {@link ConfigPropertyNames#OVERRIDE_DATASOURCES_LOC}.
     */
    public static final String DEFAULT_DATASOURCES_LOC="./target/classes/datasources/";
    
    /**
     * When run from maven, the {@link ConfigPropertyNames#OVERRIDE_DATASOURCES_LOC} will be assigned
     * to this value because of its a place holder for when a user does set the vm argument.
     */
    private static final String UNASSIGNEDDSLOC="${datasourceloc}";

    private Map<String, DataSource> allDatasourcesMap = new HashMap<String, DataSource>(); // key
											   // =
											   // datasource
											   // name

    // map of the datasources assigned to with model
    // because only one VDB (Transactions) is used, then this mapping can live
    // for the
    // duration of all tests
    private Map<String, DataSource> modelToDatasourceMap = new HashMap<String, DataSource>(); // key
											      // =
											      // modelname

    private DataSourceMgr() {
    }

    public static synchronized DataSourceMgr getInstance() {
	if (_instance == null) {
	    _instance = new DataSourceMgr();
	    try {
		_instance.loadDataSourceMappings();
	    } catch (QueryTestFailedException e) {
		throw new TransactionRuntimeException(e);
	    } catch (TransactionRuntimeException e) {
		throw e;
	    }

	}
	return _instance;
    }

    public Map<String, DataSource> getDataSources() {
	Map<String, DataSource> ds = new HashMap<String, DataSource>(
		allDatasourcesMap.size());
	ds.putAll(allDatasourcesMap);
	return ds;
    }

    public int numberOfAvailDataSources() {
	return allDatasourcesMap.size();
    }

    public DataSource getDataSource(String modelname) {
	if (modelToDatasourceMap.containsKey(modelname)) {
	    return modelToDatasourceMap.get(modelname);
	}
	return null;
    }

    public void setDataSource(String modelName, DataSource ds) {
	modelToDatasourceMap.put(modelName, ds);
    }

    private void loadDataSourceMappings() throws QueryTestFailedException {

	String dsloc = ConfigPropertyLoader.createInstance().getProperty(ConfigPropertyNames.OVERRIDE_DATASOURCES_LOC);
	
	if (dsloc == null || dsloc.equalsIgnoreCase(UNASSIGNEDDSLOC)) {
	    dsloc = DEFAULT_DATASOURCES_LOC;
	}
	
	File[] dirs = findAllChildDirectories(dsloc);
	if (dirs == null || dirs.length == 0) {
	    throw new TransactionRuntimeException(
		    "No datasource directories found at location "
			    + dsloc);
	}
	for (int i = 0; i < dirs.length; i++) {
	    File d = dirs[i];

	    addDataSource(d, allDatasourcesMap);

	}

	if (allDatasourcesMap == null || allDatasourcesMap.isEmpty()) {
	    throw new TransactionRuntimeException(
		    "Error: No Datasources were loaded.");
	} else if (allDatasourcesMap.size() < 2) {
	    throw new TransactionRuntimeException(
	    	"Error: Must load 2 Datasources, only 1 was found.");
 
	}

	System.out.println("Number of total datasource mappings loaded "
		+ allDatasourcesMap.size());

    }

    /**
     * Returns a <code>File</code> array that will contain all the directories
     * that exist in the directory
     * 
     * @return File[] of directories in the directory
     */
    private static File[] findAllChildDirectories(String dir) {

	// Find all files in the specified directory
	File mfile = new File(dir);

	File modelsDirFile = null;
	try {
	    modelsDirFile = new File(mfile.getCanonicalPath());
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	if (!modelsDirFile.exists()) {
	    return null;
	}
	FileFilter fileFilter = new FileFilter() {

	    public boolean accept(File file) {
		if (file.isDirectory()) {
		    return true;
		}

		return false;
	    }
	};

	File[] modelFiles = modelsDirFile.listFiles(fileFilter);

	return modelFiles;

    }

    private void addDataSource(File datasourcedir, //String dirname, String dirloc,
	    Map<String, DataSource> datasources) {
	
//	String dirname = datasourcefile.getName();
	
	File dsfile = new File(datasourcedir, "connection.properties");
	
	if (!dsfile.exists()) {
	    return;
	}

//	String dsfile = "/datasources/" + dirname + "/connection.properties";
	Properties dsprops = loadProperties(dsfile);

	if (dsprops != null) {

	    DataSource ds = new DataSource(datasourcedir.getName(), "dsgroup", dsprops);
	    datasources.put(ds.getName(), ds);
	    System.out.println("Loaded datasource " + ds.getName());

	}

    }

    private static Properties loadProperties(File dsfile) {
	Properties props = null;

	try {
	    
	    props = PropertiesUtils.load(dsfile.getAbsolutePath());
	    return props;
	    
//	    InputStream in = DataSourceMgr.class.getResourceAsStream(dsfile.getAbsolutePath());
//	    if (in != null) {
//		props = new Properties();
//		props.load(in);
//		return props;
//	    }
//	    return null;
	} catch (IOException e) {
	    throw new TransactionRuntimeException(
		    "Error loading properties from file '" + dsfile.getAbsolutePath() + "'"
			    + e.getMessage());
	}
    }

}
