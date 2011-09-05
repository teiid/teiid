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

package org.teiid.query.tempdata;

import java.io.Serializable;
import java.util.List;

import org.teiid.Replicated;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;

public interface GlobalTableStore {
	
	TempMetadataID getGlobalTempTableMetadataId(Object groupID) throws QueryMetadataException, TeiidComponentException, QueryResolverException, QueryValidatorException;
	
	TempMetadataID getCodeTableMetadataId(String codeTableName,
			String returnElementName, String keyElementName,
			String matTableName) throws TeiidComponentException,
			QueryMetadataException;

	MatTableInfo getMatTableInfo(String matTableName);
	
	TempTableStore getTempTableStore();

	Serializable getLocalAddress();
	
	List<?> updateMatViewRow(String matTableName, List<?> tuple, boolean delete) throws TeiidComponentException;

	TempTable createMatTable(String tableName, GroupSymbol group)
	throws TeiidComponentException, QueryMetadataException, TeiidProcessingException;
	
	@Replicated
	void failedLoad(String matTableName);
	
	@Replicated(asynch=false, timeout=5000)
	boolean needsLoading(String matTableName, Serializable loadingAddress,
			boolean firstPass, boolean refresh, boolean invalidate);
	
	@Replicated(replicateState=true)
	void loaded(String matTableName, TempTable table);

}
