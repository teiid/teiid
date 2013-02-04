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

import java.util.List;
import java.util.Map;

import org.odata4j.core.OFunctionParameter;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;

public interface Client {
	String getVDBName();
	
	int getVDBVersion();
	
	MetadataStore getMetadataStore();
	
	BaseResponse executeCall(String sql, Map<String, OFunctionParameter> parameters, EdmType returnType);

	EntityList executeSQL(Query query, List<SQLParam> parameters, EdmEntitySet entitySet, Map<String, Boolean> projectedColumns, boolean useSkipToken, String skipToken);
	
	CountResponse executeCount(Query query, List<SQLParam> parameters);
	
	int executeUpdate(Command command, List<SQLParam> parameters);	
	
	EdmDataServices getMetadata();
}
