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

import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.object.infinispan.search.SearchByKey;
import org.teiid.translator.object.util.ObjectUtil;

/**
 * The ObjectExecutionFactory is a base implementation for connecting to an
 * Object data source that's stored in a cache.
 * 
 * 
 * @author vhalbert
 * 
 */
public abstract class ObjectExecutionFactory extends
		ExecutionFactory<ConnectionFactory, Object> {

	public static final int MAX_SET_SIZE = 1000;

	/*
	 * SearchStrategy is the implementation that will perform a specific cache
	 * lookup algorithm
	 */
	private SearchStrategy searchStrategy = null;
	private String searchStrategyClassName = null;

	// rootClassName identifies the name of the class that is identified by the
	// unique key in the cache
	private String rootClassName = null;
	private Class<?> rootClass = null;

	public ObjectExecutionFactory() {
		super();

		this.setSourceRequiredForMetadata(false);
		this.setMaxInCriteriaSize(MAX_SET_SIZE);
		this.setMaxDependentInPredicates(1);

		this.setSupportsOrderBy(false);
		this.setSupportsSelectDistinct(false);
		this.setSupportsInnerJoins(false);
		this.setSupportsFullOuterJoins(false);
		this.setSupportsOuterJoins(false);

	}

	@Override
	public void start() throws TranslatorException {
		super.start();

		if (this.getRootClassName() == null
				|| this.getRootClassName().trim().length() == -1) {
			String msg = ObjectPlugin.Util.getString(
					"ObjectExecutionFactory.rootClassNameNotDefined",
					new Object[] {});
			throw new TranslatorException(msg); //$NON-NLS-1$
		}

		try {
			rootClass = Class.forName(rootClassName.trim(), true, getClass()
					.getClassLoader());

			searchStrategy = (SearchStrategy) ObjectUtil.createObject(
					searchStrategyClassName, Collections.EMPTY_LIST, getClass()
							.getClassLoader());

		} catch (ClassNotFoundException e) {
			String msg = ObjectPlugin.Util.getString(
					"ObjectExecutionFactory.rootClassNotFound",
					new Object[] { this.rootClassName });
			throw new TranslatorException(msg); //$NON-NLS-1$
		}

	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			Object connection) throws TranslatorException {

		return new ObjectExecution((Select) command, metadata, this, connection);

	}

	public List getSupportedFunctions() {
		return Collections.EMPTY_LIST;
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

	public boolean supportsOrCriteria() {
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
	 * Get the class name for the search strategy that will be used to perform
	 * object lookups in the cache.
	 * 
	 * @return String class name
	 * @see #setSearchStrategyClassName(String)
	 */
	public String getSearchStrategyClassName() {
		return this.searchStrategyClassName;
	}

	/**
	 * Set the class name for the search strategy that will be used to perform
	 * object lookups in the cache.
	 * <p>
	 * Default is {@link SearchByKey}
	 * 
	 * @param searchStrategyClassName
	 * @see #getSearchStrategyClassName()
	 */

	public void setSearchStrategyClassName(String searchStrategyClassName) {
		this.searchStrategyClassName = searchStrategyClassName;
	}

	protected SearchStrategy getSearchStrategy() {
		return this.searchStrategy;
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
	public synchronized void setRootClassName(String rootClassName) {
		if (this.rootClassName == null) {
			this.rootClassName = rootClassName;

		}
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

}
