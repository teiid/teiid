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

package org.teiid.dqp.internal.cache;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.common.buffer.impl.SizeUtility;
import com.metamatrix.dqp.message.RequestMessage;

public class ResultSetCacheUtil {
	private static final char DELIMITOR = '.'; 
	
	public static CacheID createCacheID(RequestMessage request, ResultSetCache rsCache){
		String scopeID = null;
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		if(ResultSetCache.RS_CACHE_SCOPE_VDB.equalsIgnoreCase(rsCache.getCacheScope())){
			scopeID = workContext.getVdbName() + DELIMITOR + workContext.getVdbVersion();
		}else{
			scopeID = workContext.getConnectionID();
		}
		return new CacheID(scopeID, request.getCommandString(), request.getParameterValues());
	}
	
//	public static boolean isQuery(String sql){
//		return !SqlUtil.isUpdateSql(sql);
//	}

	public static long getResultsSize(Object[] results, boolean useEstimate){
		if(results == null || results.length == 0){
			return 0;
		}
		if(useEstimate){
			//calculate the first row. Estimate the total
			//by multiply the row count
			Object row = results[0];
			long rowSize = SizeUtility.getSize(row);
			return rowSize * results.length;
		}
		return SizeUtility.getSize(results);
	}

}
