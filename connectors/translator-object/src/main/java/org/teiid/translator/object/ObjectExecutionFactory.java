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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
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

public abstract class ObjectExecutionFactory extends ExecutionFactory<Object, Object> {
	public static final int MAX_SET_SIZE = 100;

	/*
	 * ObjectMethodManager is the cache of methods used on the objects.
	 * Each ExecutionFactory instance will maintain its own method cache.
	 */
	private ObjectMethodManager objectMethods=null;
	
	private boolean columnNameFirstLetterUpperCase = true;
	private String packageNamesOfCachedObjects = null;
	
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
		this.setSupportsOuterJoins(true);
	}
		
    @Override
    public void start() throws TranslatorException {
    	createObjectMethodManager();
    }
   
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
    		throws TranslatorException {
    	return new ObjectExecution((Select)command, metadata, createProxy(connection), objectMethods);
    }    
  
	public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }
    
    public boolean supportsCompareCriteriaEquals() {
    	return true;
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
    		objectMethods = ObjectMethodManager.initialize(getClassesForPackage(this.packageNamesOfCachedObjects), 
    				isColumnNameFirstLetterUpperCase(), this.getClass().getClassLoader());
    	}
	}
	
	protected List<String> getClassesForPackage(String pkgname) throws TranslatorException {
		if (pkgname == null) return Collections.EMPTY_LIST;
	    ArrayList<String> classes = new ArrayList<String>();
	    // Get a File object for the package
	    File directory = null;
	    String fullPath;
	    String relPath = pkgname.replace('.', '/');
	    URL resource = ClassLoader.getSystemClassLoader().getResource(relPath);
	    if (resource == null) {
	        throw new TranslatorException(ObjectPlugin.Util
			.getString(
					"ObjectExecutionFactory.noResourceFound", new Object[] { relPath })); //$NON-NLS-1$    

	    }
	    fullPath = resource.getFile();

	    try {
	        directory = new File(resource.toURI());
	    } catch (URISyntaxException e) {
	        throw new TranslatorException(ObjectPlugin.Util
	    			.getString(
	    					"ObjectExecutionFactory.invalidResource", new Object[] { pkgname, resource })); //$NON-NLS-1$    
	    	
	    } catch (IllegalArgumentException e) {
	        directory = null;
	    }

	    if (directory != null && directory.exists()) {
	        // Get the list of the files contained in the package
	        String[] files = directory.list();
	        for (int i = 0; i < files.length; i++) {
	            // we are only interested in .class files
	            if (files[i].endsWith(".class") && !files[i].contains("$") ) {
	                // removes the .class extension
	                String className = pkgname + '.' + files[i].substring(0, files[i].length() - 6);
                    classes.add(className);
                    
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, "ClassDiscovery", className); //$NON-NLS-1$

	            }
	        }
	    }
	    return classes;
	}

}
