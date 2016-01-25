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

import java.util.List;
import java.util.Map;

import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

/**
 * The ObjectMaterializeLifeCycle is used to control life cycle behavior that needs to be performed at the different stages 
 * of when object materialization is being performed.
 * 
 * @author vanhalbert
 *
 */
public class ObjectMaterializeLifeCycle {
	
	public static String BEFORE_NATIVE_QUERY_PREFIX = "truncate cache"; // example:  truncate cache personStageCache 
	public static String AFTER_NATIVE_QUERY_PREFIX = "swap cache names"; // example:  swap cache names personCache personStageCache 

	private static String BEFORE_FORMAT = "truncate cache";
	private static String AFTER_FORMAT = "swap cache names";

	private static List<String> BEFORENODES = StringUtil.getTokens( BEFORE_NATIVE_QUERY_PREFIX, " ");
	private static List<String> AFTERNODES  = StringUtil.getTokens( AFTER_NATIVE_QUERY_PREFIX, " ");
	
	
	private ObjectConnection connection;
	private CacheNameProxy proxy;
	
	public ObjectMaterializeLifeCycle(ObjectConnection connection, CacheNameProxy proxy) {
		this.connection = connection;
		this.proxy = proxy;
		
	}
	
	protected ObjectConnection getConnection() {
		return this.connection;
	}
	
	public CacheNameProxy getCacheNameProxy() {
		return this.proxy;
	}
		
	public void performLifeCycleStep(String nativeQuery) throws TranslatorException {
		if (isBefore(nativeQuery)) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectMaterializeLifeCycle: performing beforeLoad materializatiion :", nativeQuery ); //$NON-NLS-1$
			
			List<String> tokens = StringUtil.getTokens( nativeQuery.trim(), " ");
			if (tokens.get(0).equals(BEFORENODES.get(0)) && tokens.get(1).equals(BEFORENODES.get(1)) ) {
			
				if (tokens.size() != 2) {
					throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21301, new Object[] {nativeQuery, BEFORE_FORMAT}));		
				}
			} else {
				throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21301, new Object[] {nativeQuery, BEFORE_FORMAT}));						
			}
			
			beforeMaterialiationOnCache();
			
		} else if (isAfter(nativeQuery)) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectMaterializeLifeCycle: performing afterLoad materializatiion :", nativeQuery ); //$NON-NLS-1$

			List<String> tokens = StringUtil.getTokens( nativeQuery.trim(), " ");
			if (tokens.get(0).equals(AFTERNODES.get(0)) && tokens.get(1).equals(AFTERNODES.get(1)) && tokens.get(2).equals(AFTERNODES.get(2)) ) {			
				if (tokens.size() != 3) {
					throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21301, new Object[] {nativeQuery, AFTER_FORMAT}));		
				}
			} else {
				throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21301, new Object[] {nativeQuery, AFTER_FORMAT}));		
				
			}

			afterMaterialiationOnCache();
			
		} else {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21302, new Object[] {nativeQuery, "[" + BEFORE_NATIVE_QUERY_PREFIX + "," +  AFTER_NATIVE_QUERY_PREFIX + "]"}));

		}
	}

	
	/**
	 * Called before the actual materialization of the data is performed.  This is do
	 * the following:
	 * <li>Clear the staging cache
	 * <li>Adding key->value entry to AliasCache, if it doesn't exist
	 * @throws TranslatorException if the staging cache cannot be determined or is not available
	 */
	protected void beforeMaterialiationOnCache() throws TranslatorException {
		String scn = this.getCacheNameProxy().getStageCacheAliasName();
		
		this.connection.clearCache(scn);
	}
	
	/**
	 * Returns the boolean true if the native query is to be performed in the beforeLoad lifecycle step.
	 * @param nativeQuery 
	 * @return boolean true if the nativeQuery is for the beforeLoad lifecycle step
	 */
	public boolean isBefore(String nativeQuery) {
		if (nativeQuery.trim().toLowerCase().startsWith(BEFORE_NATIVE_QUERY_PREFIX)) {
			return true;
		}
		return false;
	}

	/**
	 * Call after the actual materialization is performed as the afterLoad lifecycle step.  The following will be done:
	 * <li>swap the cache names in the AliasCache so that the newly populated staging cache is now the cache used for querying
	 * <li>do NOT clear the now old cache, as it can currently be queried.
	 * @throws TranslatorException
	 */
	public void afterMaterialiationOnCache() throws TranslatorException {
		@SuppressWarnings("unchecked")
		Map<Object, Object> c = (Map<Object, Object>) this.getConnection().getCache(proxy.getAliasCacheName());
		if (c == null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21303, new Object[] {proxy.getAliasCacheName()}));
		}
		
		// now swap the values in the proxy so that the next reads will use the updated cache
		proxy.swapCacheNames(c);
	}

	/**
	 * Returns the boolean true if the native query is to be performed in the afterLoad lifecycle step.
	 * @param nativeQuery 
	 * @return boolean true if the nativeQuery is for the afterLoad lifecycle step
	 */
	public boolean isAfter(String nativeQuery) {
		if (nativeQuery.trim().toLowerCase().startsWith(AFTER_NATIVE_QUERY_PREFIX)) {
			return true;
		}
		return false;
	}
	
	/**
	 * Called at the end of this specific materialization processing
	 */
	public void cleanup() {
		this.connection = null;
		this.proxy = null;
	}
	
}
