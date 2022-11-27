/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.teiid.core.util.StringUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;

public class PostgreSQLMetadataProcessor
        extends JDBCMetadataProcessor {

    @Override
    protected String getRuntimeType(int type, String typeName, int precision) {
        //pg will otherwise report a 1111/other type for geometry
        if ("geometry".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
            return TypeFacility.RUNTIME_NAMES.GEOMETRY;
        }
        if ("geography".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
            return TypeFacility.RUNTIME_NAMES.GEOGRAPHY;
        }
        if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) { //$NON-NLS-1$ //$NON-NLS-2$
            return TypeFacility.RUNTIME_NAMES.JSON;
        }
        if (PostgreSQLExecutionFactory.UUID_TYPE.equalsIgnoreCase(typeName)) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        }
        return super.getRuntimeType(type, typeName, precision);
    }

    @Override
    protected String getNativeComponentType(String typeName) {
        if (typeName.startsWith("_")) { //$NON-NLS-1$
            return typeName.substring(1);
        }
        return super.getNativeComponentType(typeName);
    }

    @Override
    protected Column addColumn(ResultSet columns, Table table,
            MetadataFactory metadataFactory, int rsColumns)
            throws SQLException {
        Column result = super.addColumn(columns, table, metadataFactory, rsColumns);
        if (PostgreSQLExecutionFactory.UUID_TYPE.equalsIgnoreCase(result.getNativeType())) {
            result.setLength(36); //pg reports max int
            result.setCaseSensitive(false);
        }
        return result;
    }

    @Override
    protected String getGeographyMetadataTableName() {
        return "public.geography_columns"; //$NON-NLS-1$
    }

    @Override
    protected String getGeometryMetadataTableName() {
        return "public.geometry_columns"; //$NON-NLS-1$
    }

    @Override
    protected ResultSet executeSequenceQuery(Connection conn)
            throws SQLException {
        String query = "select null::varchar as sequence_catalog, nspname as sequence_schema, relname as sequence_name " //$NON-NLS-1$
                + "from pg_class, pg_namespace where relkind='S' and pg_namespace.oid = relnamespace " //$NON-NLS-1$
                + "and nspname like ? escape '' and relname like ? escape ''"; //$NON-NLS-1$
        PreparedStatement ps = conn.prepareStatement(query);
        ps.setString(1, getSchemaPattern()==null?"%":getSchemaPattern()); //$NON-NLS-1$
        ps.setString(2, getSequenceNamePattern()==null?"%":getSequenceNamePattern()); //$NON-NLS-1$
        return ps.executeQuery();
    }

    @Override
    protected String getSequenceNextSQL(String fullyQualifiedName) {
        return "nextval('" + StringUtil.replaceAll(fullyQualifiedName, "'", "''") + "')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Override
    protected Table addTable(MetadataFactory metadataFactory,
            String tableCatalog, String tableSchema, String tableName,
            String remarks, String fullName, ResultSet tables)
            throws SQLException {
        String type = tables.getString(4);
        if (type == null || type.contains("INDEX") //$NON-NLS-1$
                || type.equalsIgnoreCase("TYPE") //$NON-NLS-1$
                || type.equalsIgnoreCase("SEQUENCE")) { //$NON-NLS-1$
            return null;
        }
        return super.addTable(metadataFactory, tableCatalog, tableSchema, tableName,
                remarks, fullName, tables);
    }
}