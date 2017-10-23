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
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;

public final class PostgreSQLMetadataProcessor
        extends JDBCMetadataProcessor {
    
    @Override
    protected String getRuntimeType(int type, String typeName, int precision) {
        //pg will otherwise report a 1111/other type for geometry
    	if ("geometry".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
            return TypeFacility.RUNTIME_NAMES.GEOMETRY;
        }                
    	if (PostgreSQLExecutionFactory.UUID_TYPE.equalsIgnoreCase(typeName)) { 
    	    return TypeFacility.RUNTIME_NAMES.STRING;
    	}
        return super.getRuntimeType(type, typeName, precision);                    
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
    protected void getGeometryMetadata(Column c, Connection conn,
    		String tableCatalog, String tableSchema, String tableName,
    		String columnName) {
    	PreparedStatement ps = null;
    	ResultSet rs = null;
    	try {
    		if (tableCatalog == null) {
    			tableCatalog = conn.getCatalog();
    		}
        	ps = conn.prepareStatement("select coord_dimension, srid, type from public.geometry_columns where f_table_catalog=? and f_table_schema=? and f_table_name=? and f_geometry_column=?"); //$NON-NLS-1$
        	ps.setString(1, tableCatalog);
        	ps.setString(2, tableSchema);
        	ps.setString(3, tableName);
        	ps.setString(4, columnName);
        	rs = ps.executeQuery();
        	if (rs.next()) {
        		c.setProperty(MetadataFactory.SPATIAL_URI + "coord_dimension", rs.getString(1)); //$NON-NLS-1$
        		c.setProperty(MetadataFactory.SPATIAL_URI + "srid", rs.getString(2)); //$NON-NLS-1$
        		c.setProperty(MetadataFactory.SPATIAL_URI + "type", rs.getString(3)); //$NON-NLS-1$
        	}
    	} catch (SQLException e) {
    		LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not get geometry metadata for column", tableSchema, tableName, columnName); //$NON-NLS-1$
    	} finally {
    		if (rs != null) {
    			try {
    				rs.close();
    			} catch (SQLException e) {
    			}
    		}
    		if (ps != null) {
    			try {
    				ps.close();
    			} catch (SQLException e) {
    			}
    		}
    	}
    }
    
    @Override
    protected String getSequenceQuery() {
        return "select null::varchar as sequence_catalog, nspname as sequence_schema, relname as sequence_name from pg_class, pg_namespace where relkind='S' and pg_namespace.oid = relnamespace " //$NON-NLS-1$
                + "and nspname like ? escape '' and relname like ? escape ''"; //$NON-NLS-1$
    }
    
    @Override
    protected String getSequenceNextSQL(String fullyQualifiedName) {
        return "nextval('" + StringUtil.replaceAll(fullyQualifiedName, "'", "''") + "')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$  
    }
}