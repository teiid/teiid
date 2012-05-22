/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.object;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.teiid.core.util.StringUtil;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;

/**
 * The ObjectExecutionFactory is a base implementation of the ExecutionFactory.
 * For each implementor, the {@link #createProxy(Object)} method will need to be
 * implemented in order to provide the data source specific implementation for 
 * query execution.
 *  
 * 
 * @author vhalbert
 *
 */
//public abstract class ObjectExecutionFactory extends ExecutionFactory<ConnectionFactory, ObjectCacheConnection > {

public abstract class ObjectExecutionFactory extends ExecutionFactory {
	public static final int MAX_SET_SIZE = 1000;

	/*
	 * ObjectMethodManager is the cache of methods used on the objects.
	 * Each ExecutionFactory instance will maintain its own method cache.
	 */
	private ObjectMethodManager objectMethods=null;
	
	private boolean columnNameFirstLetterUpperCase = true;
	private String packageNamesOfCachedObjects = null;
	private String classNamesOfCachedObjects = null;
	private String cacheName = null;
	private boolean supportFilters = true;
	
	public ObjectExecutionFactory() {
		super();
		init();
	}
	
	protected void init() {
		this.setMaxInCriteriaSize(MAX_SET_SIZE);
		this.setMaxDependentInPredicates(1);
		this.setSourceRequired(false);
		this.setSupportsOrderBy(false);
		this.setSupportsSelectDistinct(false);
		this.setSupportsInnerJoins(true);
		this.setSupportsFullOuterJoins(false);
		this.setSupportsOuterJoins(false);

	}
		
    @Override
    public void start() throws TranslatorException {
    	super.start();
    	createObjectMethodManager();
    	
    }
   
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
    		throws TranslatorException {

    	return new ObjectExecution((Select)command, metadata, createProxy(connection), objectMethods, this);
    	  
    }    
  
	public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }
    
    public boolean supportsCompareCriteriaEquals() {
    	return true;
    }
    
    public boolean supportsInCriteria() {
    	return true;
    }	
    
    public boolean supportsOrCriteria() {
    	return true;
    }	    
	/**
	 * Get the cacheName that will be used by this factory instance to access the named cache. 
	 * However, if not specified a default configuration will be created.
	 * @return
	 * @see #setCacheName(String)
	 */
	@TranslatorProperty(display="CacheName", advanced=true)
	public String getCacheName() {
		return this.cacheName;
	}
	
	/**
	 * Set the cacheName that will be used to find the named cache.
	 * @param cacheName
	 * @see #getCacheName()
	 */
	
	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	} 
	
	
	// TODO:  implement the code that supports this option for non-annotated classes
//	/**
//	 * Get the object relationships.  
//	 * @return
//	 * @see #setObjectRelationships(String)
//	 */
//	@TranslatorProperty(display="ObjectRelationships", advanced=true)
//	public String getObjectRelationships() {
//		return this.objectRelationShips;
//	}
//	
//	/**
//	 * Set the object relationships so that the metadata relationships can be built.  Specify the 
//	 * relationships using the format:  <parent classname>.<getMethod>:<child classname> 
//	 * @param cacheName
//	 * @see #getObjectRelationships()
//	 */
//	
//	public void setObjectRelationships(String objectRelationships) {
//		this.objectRelationShips = objectRelationships;
//	} 
	
	/**
	 * <p>
	 * Call to get the indicator if object filtering will be used to help determine if
	 * an object is included in the results.  This option can be used when the criteria
	 * cannot be used by the vendor specific querying capabilities. 
	 * </p>
	 *  This doesn't apply to the primary key when used in the criteria.
	 * <p>
	 * Note, the use of this option will be slower because the filtering is done post-retrieval
	 * of the objects from the cache.
	 * </p>
	 * 
	 * @return
	 * @see #setSupportFilters(boolean)
	 */
	@TranslatorProperty(display="SupportFilters", advanced=true)
	public boolean isSupportFilters() {
		return this.supportFilters;
	}
	
	/**
	 * Set to <code>true</code> when the criteria will be used to filter the objects to be
	 * returned in the results.  
	 * @param supportFilters
	 * @see #isSupportFilters()
	 */
	
	public void setSupportFilters(boolean supportFilters) {
		this.supportFilters = supportFilters;
	} 
	
	/**
	 * <p>
	 * Returns a comma separated list of package names for the cached objects.
	 * </p>
	 * @return String comma separated package names
	 */
	@TranslatorProperty(display="PackageNamesOfCachedObjects (CSV)", advanced=true)
	public String getPackageNamesOfCachedObjects() {
		return this.packageNamesOfCachedObjects;
	}
	
	/**
	 * <p>
	 * Call to set package names for the cached objects
	 * </p>
	 * @param String commo separated list of package names
	 */
	public void setPackageNamesOfCachedObjects(String packageNamesOfCachedObjects) {
		this.packageNamesOfCachedObjects = packageNamesOfCachedObjects;
	}
	
	/**
	 * Call to get a comma separated list of class names to use. 
	 * @return
	 */
	@TranslatorProperty(display="ClassNamesOfCachedObjects (CSV)", advanced=true)
	public String getClassNamesOfCachedObjects() {
		return this.classNamesOfCachedObjects;
	}
	
	/**
	 * <p>
	 * Call to set class names for the cached objects
	 * </p>
	 * @param String commo separated list of package names
	 */
	public void setClassNamesOfCachedObjects(String classNamesOfCachedObjects) {
		this.classNamesOfCachedObjects = classNamesOfCachedObjects;
	}	

  
	/**
	 * <p>
	 * The {@link #getColumnNameFirstLetterUpperCase() option, when <code>false</code>, indicates that the column name (or nameInSource when specified)
	 * will start with a lower case.   This is an option because some users prefer (or standardize) on names being lower case or have a 
	 * different case naming structure. 
	 * <p/>
	 * <p>
	 * The case matters for the following reasons:
	 * <li>Deriving the "getter/setter" method on the object to read the value.  Because JavaBean naming convention
	 * 		is used.</li>
	 * <li>Building criteria logic for searching the datasource.  This one is functionality specific (i.e., Hibernate Search)
	 * 		as to how it maps the column name to an indexed field. </li>
	 * </p>
	 * @return boolean indicating if the case of the first letter of the column name (or nameInSource when specified), default <code>true</code>
	 */
	@TranslatorProperty(display="ColumnNameFirstLetterUpperCase", advanced=true)
	public boolean isColumnNameFirstLetterUpperCase() {
		return this.columnNameFirstLetterUpperCase;
	}
	
	/**
	 * <p>
	 * The {@link #columnNameFirstLetterUpperCase} option, when <code>false</code>, indicates that the column name (or nameInSource when specified)
	 * will start with a lower case.   This is an option because some users prefer (or standardize) on names being lower case or have a 
	 * different case naming structure. 
	 * <p/>
	 * <p>
	 * The case matters for the following reasons:
	 * <li>Deriving the "getter/setter" method on the object to read the value.  Because JavaBean naming convention
	 * 		is used.</li>
	 * <li>Building criteria logic for searching the datasource.  This one is functionality specific (i.e., Hibernate Search)
	 * 		as to how it maps the column name to an indexed field. </li>
	 * </p>
	 * @param firstLetterUpperCase indicates the case of the first letter in the column name (or nameInSource when specified), default <code>true</code>
	 * @return
	 */
	public void setColumnNameFirstLetterUpperCase(boolean firstLetterUpperCase) {
		this.columnNameFirstLetterUpperCase = firstLetterUpperCase;
	}

    
    @Override
	public void getMetadata(MetadataFactory metadataFactory, Object conn)
			throws TranslatorException {
    	createObjectMethodManager();
 		ObjectMetadataProcessor processor = new ObjectMetadataProcessor(metadataFactory, this);
		processor.processMetadata();
	}
	
	
	
	public ObjectMethodManager getObjectMethodManager() {
		return this.objectMethods;
	}
	
	/**
	 * Implement to return an instance of {@link ObjectSourceProxy proxy} that is used
	 * by {@link ObjectExecution execution} to issue requests.
	 * @param connection
	 * @return IObjectConnectionProxy
	 * @throws TranslatorException
	 */
	protected abstract ObjectSourceProxy createProxy(Object connection) throws TranslatorException ;

	protected void createObjectMethodManager() throws TranslatorException {
	    	if (objectMethods == null) {
	
	    		List<String> classes = new ArrayList<String>();
	    		if (this.classNamesOfCachedObjects != null) {
	    			classes = StringUtil.split(this.classNamesOfCachedObjects, ",");
	    			
	    		} else if (this.packageNamesOfCachedObjects != null && this.packageNamesOfCachedObjects.trim().length() > 0) {
		    		List<String> packageNames = StringUtil.split(this.packageNamesOfCachedObjects, ",");
		    		
		    		for (String packageName : packageNames) {
		    			classes.addAll(getClassesInPackage(packageName, null));
		    		}
	    		}
		    		
		    	objectMethods = ObjectMethodManager.initialize( classes, isColumnNameFirstLetterUpperCase(), this.getClass().getClassLoader());
	    	}
	}

	/**
	 * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
	 * Adapted from http://snippets.dzone.com/posts/show/4831 and extended to support use of JAR files
	 *
	 * @param packageName The base package
	 * @param regexFilter an optional class name pattern.
	 * @return The classes
	 */
	protected List<String> getClassesInPackage(String packageName, String regexFilter) throws TranslatorException{
		if (packageName == null) return Collections.EMPTY_LIST;

		Pattern regex = null;
		if (regexFilter != null)
			regex = Pattern.compile(regexFilter);

		try {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			assert classLoader != null;
			String path = packageName.replace('.', '/');
			Enumeration<URL> resources = classLoader.getResources(path);
			List<String> dirs = new ArrayList<String>();
			while (resources.hasMoreElements()) {
				URL resource = resources.nextElement();
				dirs.add(resource.getFile());
			}
			if (dirs.isEmpty()) {
				classLoader = this.getClass().getClassLoader();
				assert classLoader != null;
				resources = classLoader.getResources(path);
				while (resources.hasMoreElements()) {
					URL resource = resources.nextElement();
					dirs.add(resource.getFile());
				}
				if (dirs.isEmpty()) {				
			        throw new TranslatorException(ObjectPlugin.Util
			        		.getString(
							"ObjectExecutionFactory.noResourceFound", new Object[] { packageName })); //$NON-NLS-1$   
				}
	
		    }

			TreeSet<String> classes = new TreeSet<String>();
			for (String directory : dirs) {
				classes.addAll(findClasses(directory, packageName, regex));
			}
			ArrayList<String> classNames = new ArrayList<String>();
			for (String clazz : classes) {
				classNames.add(clazz);
			}
				        
	        return classNames;			
			
		} catch (TranslatorException te) {
			throw te;
		} catch (Exception e) {
			e.printStackTrace();
			throw new TranslatorException(e);
		}
	}

	/**
	 * Recursive method used to find all classes in a given path (directory or zip file url).  Directories
	 * are searched recursively.  (zip files are
	 * Adapted from http://snippets.dzone.com/posts/show/4831 and extended to support use of JAR files
	 *
	 * @param path   The base directory or url from which to search.
	 * @param packageName The package name for classes found inside the base directory
	 * @param regex       an optional class name pattern.  e.g. .*Test
	 * @return The classes
	 */
	private static TreeSet<String> findClasses(String path, String packageName, Pattern regex) throws Exception {
		TreeSet<String> classes = new TreeSet<String>();
		if (path.startsWith("file:") && path.contains("!")) {
			
		} else if (path.indexOf(".jar") > -1) {
			int idx =  path.indexOf(".jar") + 4;
        	path = "file:" + path.substring(0, idx) + "!" + path.substring(idx + 1) ;
        }
		if (path.startsWith("file:") && path.contains("!")) {
			String[] split = path.split("!");
			URL jar = new URL(split[0]);
			ZipInputStream zip = new ZipInputStream(jar.openStream());
			ZipEntry entry;
			while ((entry = zip.getNextEntry()) != null) {
				if (entry.getName().endsWith(".class")) {
					String className = entry.getName().replaceAll("[$].*", "").replaceAll("[.]class", "").replace('/', '.');
					if (className.startsWith(packageName) && (regex == null || regex.matcher(className).matches()))
						classes.add(className);
				}
			}
		}
		File dir = new File(path);
		if (!dir.exists()) {
			return classes;
		}
		File[] files = dir.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				assert !file.getName().contains(".");
				classes.addAll(findClasses(file.getAbsolutePath(), packageName + "." + file.getName(), regex));
			} else if (file.getName().endsWith(".class")) {
				String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);
				if (regex == null || regex.matcher(className).matches())
					classes.add(className);
			}
		}
		return classes;
	}
	

}
