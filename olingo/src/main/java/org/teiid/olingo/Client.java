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
package org.teiid.olingo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidException;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Expression;

public interface Client {

    VDBMetaData getVDB();

    MetadataStore getMetadataStore();

    BaseResponse executeCall(String sql, List<SQLParam> sqlParams, SingletonPrimitiveType returnType);

    void executeSQL(Query query, List<SQLParam> parameters, boolean countQuery, Integer skip, Integer top, QueryResponse respose);

    CountResponse executeCount(Query query, List<SQLParam> parameters);

    UpdateResponse executeUpdate(Command command, List<SQLParam> parameters);

    String getProperty(String name);
}

interface UpdateResponse {
    Map<String, Object> getGeneratedKeys();
    int getUpdateCount();
}

interface CountResponse {
    long getCount();
}

interface BaseResponse {
}

interface ProjectedColumn {
    Expression getExpression();
    boolean isVisible();
}

interface QueryResponse {
    void addRow(ResultSet rs) throws SQLException, TeiidException;
    long size();
    void setCount(long count);
    void setNext(long row);
}