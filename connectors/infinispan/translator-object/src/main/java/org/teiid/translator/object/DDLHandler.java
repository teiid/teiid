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

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

/**
 * The DDLHandler
 * 
 * @author vanhalbert
 *
 */
public class DDLHandler {
	
	public static String TRUNCATE = "truncate cache"; //$NON-NLS-1$
	public static String SWAP = "swap cache names"; //$NON-NLS-1$
	private static ThreadLocal<Boolean> STAGING_TARGET = new ThreadLocal<Boolean>();

	private CacheNameProxy proxy;
	
	public DDLHandler(CacheNameProxy proxy) {
		this.proxy = proxy;
		
	}
	
	public CacheNameProxy getCacheNameProxy() {
		return this.proxy;
	}
	
	public synchronized void handleDDL(String nativeQuery, ObjectConnection connection) throws TranslatorException {
		String query = nativeQuery.trim().toLowerCase();
		if (query.equals(TRUNCATE)) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"DDLHandler: performing ", nativeQuery ); //$NON-NLS-1$
			
			truncate(connection);
		} else if (query.equals(SWAP)) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"DDLHandler: performing ", nativeQuery ); //$NON-NLS-1$

			swap(connection);
		} else {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21302, new Object[] {nativeQuery, "[" + TRUNCATE + "tbl," +  SWAP + "]"})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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
	private void truncate(ObjectConnection connection) throws TranslatorException {
		proxy.ensureCacheNames(connection);
		
		String scn = this.getCacheNameProxy().getStageCacheAliasName(connection);
		
		connection.clearCache(scn);
	}

	/**
	 * Call after the actual materialization is performed as the afterLoad lifecycle step.  The following will be done:
	 * <li>swap the cache names in the AliasCache so that the newly populated staging cache is now the cache used for querying
	 * <li>do NOT clear the now old cache, as it can currently be queried.
	 * @param connection 
	 * @throws TranslatorException
	 */
	private void swap(ObjectConnection connection) throws TranslatorException {
		proxy.swapCacheNames(connection);
	}

	public boolean isStagingTarget() {
		Boolean b = STAGING_TARGET.get();
		return (b != null && b);
	}
	
	public void setStagingTarget(boolean target) {
		STAGING_TARGET.set(target);
	}

}
