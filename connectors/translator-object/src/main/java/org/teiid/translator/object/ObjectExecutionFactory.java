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

import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.cci.ConnectionFactory;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;


/**
 * The ObjectExecutionFactory is a base implementation for connecting to an
 * Object cache.  It provides the core features and behavior common to all implementations.
 * 
 * @author vhalbert
 * 
 */
public abstract class ObjectExecutionFactory extends
		ExecutionFactory<ConnectionFactory, ObjectConnection> {

	public static final int MAX_SET_SIZE = 10000;

	// rootClassName identifies the name of the class that is identified by the
	// unique key in the cache
	private String rootClassName = null;
	private Class<?> rootClass = null;
	private String cacheJndiName;


	public ObjectExecutionFactory() {

		setSourceRequiredForMetadata(false);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);

		setSupportsOrderBy(false);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(false);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(false);

	}

	@Override
	public void start() throws TranslatorException {
		super.start();

		if (this.getRootClassName() == null
				|| this.getRootClassName().trim().length() == -1) {
			String msg = ObjectPlugin.Util.getString(
					"ObjectExecutionFactory.rootClassNameNotDefined", //$NON-NLS-1$
					new Object[] {});
			throw new TranslatorException(msg); 
		}

		try {
			rootClass = Class.forName(rootClassName.trim(), true, getClass()
					.getClassLoader());

		} catch (ClassNotFoundException e) {
			String msg = ObjectPlugin.Util.getString(
					"ObjectExecutionFactory.rootClassNotFound",  //$NON-NLS-1$
					new Object[] { this.rootClassName });
			throw new TranslatorException(msg);
		}

	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {

		
		return new ObjectExecution((Select) command, metadata, this, (connection == null ? getConnection(null, executionContext) : connection) );

	}

	@Override
	public boolean supportsInnerJoins() {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) {
		return false;
	}

	public boolean supportsCompareCriteriaEquals() {
		return true;
	}

	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsLikeCriteria() {
		// at this point, i've been unable to get the Like to work.
		return false;
	}

	@Override
	public boolean supportsNotCriteria() {
		return false;
	}

	@Override
	public boolean supportsSubqueryInOn() {
		return false;
	}

	@Override
	public boolean supportsOrderBy() {
		return false;
	}
	
	/**
	 * Call to get the class name of the root object in the cache. This
	 * identifies the name of the class that is identified by the unique key in
	 * the cache
	 * 
	 * @return
	 */
	@TranslatorProperty(display = "Root ClassName of Cached Object", advanced = true)
	public String getRootClassName() {
		return this.rootClassName;
	}

	/**
	 * Call to set the root class name for the cache accessed by this factory
	 * instance.
	 * <p>
	 * If the root class name has already been set, subsequent calls will have
	 * no effect.
	 * 
	 * @param rootClassName
	 */
	public void setRootClassName(String rootClassName) {
		this.rootClassName = rootClassName;
	}
	
	 /**
     * Get the JNDI name for the  {@link Map cache}  instance that should be used as the data source.
     * 
     * @return the JNDI name of the {@link Map cache} instance that should be used,
     * @see #setCacheJndiName(String)
     */
	@TranslatorProperty(display = "CacheJndiName", advanced = true)
    public String getCacheJndiName() {
        return cacheJndiName;
    }

    /**
     * Set the JNDI name to a {@link Map cache} instance that should be used as this source.
     * 
     * @param jndiName the JNDI name of the {@link Map cache} instance that should be used
     * @see #setCacheJndiName(String)
     */
    public void setCacheJndiName( String jndiName ) {
        this.cacheJndiName = jndiName;
    }

	/**
	 * Call to get the class specified by calling
	 * {@link #setRootClassName(String)}
	 * 
	 * @return Class
	 */
	public Class<?> getRootClass() {
		return this.rootClass;
	}
	
	/**
	 * Utility method available to all implementations to find the Cache via JNDI.
	 * @return Object located via JNDI
	 * @throws TranslatorException
	 */
	protected Object findCacheUsingJNDIName() throws TranslatorException {
		  	
		    Object cache = null;
		    String jndiName = getCacheJndiName();
		    if (jndiName != null && jndiName.trim().length() != 0) {
		        try {
		            Context context = null;
	                try {
	                    context = new InitialContext();
	                } catch (NamingException err) {
	                    throw new TranslatorException(err);
	                }
		            cache = context.lookup(jndiName);
		            
		            if (cache == null) {
		    			String msg = ObjectPlugin.Util.getString(
		    					"ObjectExecutionFactory.cacheNotFoundinJNDI", //$NON-NLS-1$
		    					new Object[] { jndiName });
		    			throw new TranslatorException(msg); 
		            	
		            } 		
		        } catch (Exception err) {
		            if (err instanceof RuntimeException) throw (RuntimeException)err;
		            throw new TranslatorException(err);
		        }
		    } 
		    return cache;
	    }	
	
	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}

}
