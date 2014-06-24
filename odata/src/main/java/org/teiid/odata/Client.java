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
package org.teiid.odata;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.QueryInfo;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;

public interface Client {
	VDBMetaData getVDB();
		
	MetadataStore getMetadataStore();
	
	BaseResponse executeCall(String sql, List<SQLParam> sqlParams, EdmType returnType);

	EntityList executeSQL(Query query, List<SQLParam> parameters, EdmEntitySet entitySet, LinkedHashMap<String, Boolean> projectedColumns, QueryInfo queryInfo);
	
	CountResponse executeCount(Query query, List<SQLParam> parameters);
	
	UpdateResponse executeUpdate(Command command, List<SQLParam> parameters);	
	
	EdmDataServices getMetadata();
}

interface UpdateResponse {
	Map<String, Object> getGeneratedKeys();
	int getUpdateCount();
}
