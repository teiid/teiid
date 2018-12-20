/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;


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
@SuppressWarnings("nls")
public class DataSourceMgr {

    private static DataSourceMgr _instance = null;
    
    /**
     * Defines the default location where the datasource files will be found.
     * An override can be specified by setting the property {@link ConfigPropertyNames#OVERRIDE_DATASOURCES_LOC}.
     */
    public static final String DEFAULT_DATASOURCES_LOC="./src/main/resources/datasources/";
    
    /**
     * When run from maven, the {@link ConfigPropertyNames#OVERRIDE_DATASOURCES_LOC} will be assigned
     * to this value because of its a place holder for when a user does set the vm argument.
     */
    private static final String UNASSIGNEDDSLOC="${";

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
    
    private DataSourceFactory dsfactory = null;


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
    
    private static synchronized void reset() {
	_instance = null;
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

    @SuppressWarnings("deprecation")
    public DataSource getDataSource(String modelname) {
	if (modelToDatasourceMap.containsKey(modelname)) {
	    return modelToDatasourceMap.get(modelname);
	}
	
	try {
	    DataSource ds = dsfactory.getDatasource( modelname);
	    
		if (ds == null) {
		    printAllDatasources();
		    
		    try {
			Thread.sleep(100000);
		    } catch (InterruptedException e) {
		    }
		   Thread.currentThread().getThreadGroup().stop();
		   
		   
//			throw new QueryTestFailedException(
//					"Unable to assign a datasource for model "
//							+ modelname );

		}
	    modelToDatasourceMap.put(modelname, ds);
	    return ds;
	} catch (QueryTestFailedException e) {
	    throw new TransactionRuntimeException(e);
	}
    }
    
    private void printAllDatasources() {
	Iterator<String> it=allDatasourcesMap.keySet().iterator();
	while(it.hasNext()) {
	    String key = it.next();
	    DataSource ds = allDatasourcesMap.get(key);
	    TestLogger.log("DataSource: " + ds.getName());
	    
	}

    }

    
    public void shutdown() {
	 TestLogger.log("Shutting down data sources");
	 
	if (allDatasourcesMap != null && allDatasourcesMap.size() > 0) {
		Iterator<String> it=allDatasourcesMap.keySet().iterator();
		while(it.hasNext()) {
		    String key = it.next();
		    DataSource ds = allDatasourcesMap.get(key);
		    try {
			ds.shutdown();
		    } catch (Throwable t) {
			
		    }
		}
		
		allDatasourcesMap.clear();
	}
	
	if (modelToDatasourceMap != null || modelToDatasourceMap.size() > 0) {
		Iterator<String> it=modelToDatasourceMap.keySet().iterator();
		while(it.hasNext()) {
		    String key = it.next();
		    DataSource ds = modelToDatasourceMap.get(key);
		    try {
			ds.shutdown();
		    } catch (Throwable t) {
			
		    }
		}
		
		modelToDatasourceMap.clear();
	}
	
	if (dsfactory != null) dsfactory.cleanup();
	
	DataSourceMgr.reset();
	
    }

    public void setDataSource(String modelName, DataSource ds) {
	modelToDatasourceMap.put(modelName, ds);
    }

    private void loadDataSourceMappings() throws QueryTestFailedException {
	if (ConfigPropertyLoader.getInstance().isDataStoreDisabled()) {
	    TestLogger.logDebug("DataStore usage has been disabled");
	    return;
	}

	String dsloc = ConfigPropertyLoader.getInstance().getProperty(ConfigPropertyNames.OVERRIDE_DATASOURCES_LOC);
	
	if (dsloc == null || dsloc.indexOf(UNASSIGNEDDSLOC) > -1) {
	    dsloc = DEFAULT_DATASOURCES_LOC;
	    TestLogger.logDebug("Using default datasource loc: " +dsloc);
	} else {
	    TestLogger.logDebug("Using override for datasources loc: " + dsloc);
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

	TestLogger.logDebug("Number of total datasource mappings loaded "
		+ allDatasourcesMap.size());
	
	
	dsfactory = new DataSourceFactory(ConfigPropertyLoader.getInstance());
	dsfactory.config(this);

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

    private void addDataSource(File datasourcedir, 
	    Map<String, DataSource> datasources) {
	
	File dsfile = new File(datasourcedir, "connection.properties");
	
	if (!dsfile.exists()) {
	    return;
	}

	Properties dsprops = loadProperties(dsfile);

	if (dsprops != null) {

	    DataSource ds = new DataSource(datasourcedir.getName(), "dsgroup", dsprops);
	    datasources.put(ds.getName(), ds);
	    TestLogger.logDebug("Loaded datasource " + ds.getName());

	}

    }

    private static Properties loadProperties(File dsfile) {
	Properties props = null;

	try {
	    
	    props = PropertiesUtils.load(dsfile.getAbsolutePath());
	    return props;
	    
	} catch (IOException e) {
	    throw new TransactionRuntimeException(
		    "Error loading properties from file '" + dsfile.getAbsolutePath() + "'"
			    + e.getMessage());
	}
    }

}
