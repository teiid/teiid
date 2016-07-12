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

import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

/**
 * The ObjectMaterializeLifeCycle is used to control life cycle behavior that needs to be performed at different stages 
 * of the materialization process being performed.   This object is designed to have a single instance per resource-adapter 
 * and used for any connection so that at any time, it can be determined at what stage the materialization process is in
 * for that particular data source.
 * 
 * @author vanhalbert
 *
 */
public class ObjectMaterializeLifeCycle {
	
	enum Stage {
		INACTIVE,
		START_BEFORE,
		END_BEFORE,
		START_LOAD,
		END_LOAD,
		START_AFTER
	}
	
	public static String BEFORE_FORMAT = "truncate cache";
	public static String AFTER_FORMAT = "swap cache names";
	public static String RESET_FORMAT = "reset";

	private static List<String> BEFORENODES = StringUtil.getTokens( BEFORE_FORMAT, " ");
	private static List<String> AFTERNODES  = StringUtil.getTokens( AFTER_FORMAT, " ");

	
	private CacheNameProxy proxy;
	private Stage currentStage=Stage.INACTIVE;
	
	public ObjectMaterializeLifeCycle(CacheNameProxy proxy) {
		this.proxy = proxy;
		
	}
	
	public CacheNameProxy getCacheNameProxy() {
		return this.proxy;
	}
	
	public Stage getCurrentStage() {
		return currentStage;
	}
	
	public boolean hasStarted() {
		return (currentStage != Stage.INACTIVE);
	}
	
	public boolean isLoading() {
		return (currentStage == Stage.START_LOAD);
	}
		
	public synchronized void performLifeCycleStep(String nativeQuery, ObjectConnection connection) throws TranslatorException {
		if (nativeQuery.trim().toLowerCase().startsWith(BEFORE_FORMAT)) {
			// its assumed the materialize process will not allow multiple executions to occur at the same time by checking
			// the status table.  Therefore, if it does execute this matview process, then the "IsBefore" step will
			// reset at the beginning

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
			
			beforeMaterialiationOnCache(connection);
			
		} else if (nativeQuery.trim().toLowerCase().startsWith(AFTER_FORMAT)) {
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

			afterMaterialiationOnCache(connection);
			
		} else if (nativeQuery.trim().toLowerCase().startsWith(RESET_FORMAT)) {
			reset();
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectMaterializeLifeCycle: performing reset "); //$NON-NLS-1$

		} else {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21302, new Object[] {nativeQuery, "[" + BEFORE_FORMAT + "," +  AFTER_FORMAT + "]"}));

		}
	}

	
	/**
	 * Called before the actual materialization of the data is performed.  This is do
	 * the following:
	 * <li>Clear the staging cache
	 * <li>Adding key->value entry to AliasCache, if it doesn't exist
	 * @param connection 
	 * @throws TranslatorException if the staging cache cannot be determined or is not avaiCommandContext lable
	 */
	private void beforeMaterialiationOnCache(ObjectConnection connection) throws TranslatorException {
		this.currentStage = Stage.START_BEFORE;
		
		proxy.ensureCacheNames();
		
		String scn = this.getCacheNameProxy().getStageCacheAliasName();
		
		connection.clearCache(scn);
		
		this.currentStage = Stage.END_BEFORE;
	}

	/**
	 * Call after the actual materialization is performed as the afterLoad lifecycle step.  The following will be done:
	 * <li>swap the cache names in the AliasCache so that the newly populated staging cache is now the cache used for querying
	 * <li>do NOT clear the now old cache, as it can currently be queried.
	 * @param connection 
	 * @throws TranslatorException
	 */
	private void afterMaterialiationOnCache(ObjectConnection connection) throws TranslatorException {
		this.currentStage = Stage.START_AFTER;
		
		// now swap the values in the proxy so that the next reads will use the updated cache
		proxy.swapCacheNames();
		
		reset();
	}

	/**
	 * Call to reset the materialization stage, especially when a failure has occured.
	 */
	public synchronized void reset() {
		this.currentStage = Stage.INACTIVE;
	}
	
}
