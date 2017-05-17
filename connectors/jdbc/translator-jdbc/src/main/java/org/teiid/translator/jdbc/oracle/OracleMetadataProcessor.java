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

package org.teiid.translator.jdbc.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;

public final class OracleMetadataProcessor extends
		JDBCMetdataProcessor {

	private boolean useGeometryType;
	private boolean useIntegralTypes;
	
	@Override
	protected String getRuntimeType(int type, String typeName, int precision,
			int scale) {
		//overrides for varchar2 and nvarchar2
		if (type == 12 || (type == Types.OTHER && typeName != null && StringUtil.indexOfIgnoreCase(typeName, "char") > -1)) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.STRING;
		}
	    if (useGeometryType && "SDO_GEOMETRY".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
	        return TypeFacility.RUNTIME_NAMES.GEOMETRY;
	    }
	    if (useIntegralTypes && scale == 0 && (type == Types.NUMERIC || type == Types.DECIMAL)) {
	    	if (precision <= 2) {
	    		return TypeFacility.RUNTIME_NAMES.BYTE;
	    	}
	    	if (precision <= 4) {
	    		return TypeFacility.RUNTIME_NAMES.SHORT;
	    	}
	    	if (precision <= 9) {
	    		return TypeFacility.RUNTIME_NAMES.INTEGER;
	    	}
	    	if (precision <= 18) {
	    		return TypeFacility.RUNTIME_NAMES.LONG;
	    	}
	    	return TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
	    }
		return super.getRuntimeType(type, typeName, precision, scale);
	}

	@Override
	protected void getTableStatistics(Connection conn, String catalog, String schema, String name, Table table) throws SQLException {
	    PreparedStatement stmt = null;
	    ResultSet rs = null;
	    try {
	        stmt = conn.prepareStatement("select num_rows from ALL_TABLES where owner = ? AND table_name = ?");  //$NON-NLS-1$
	        stmt.setString(1, schema);
	        stmt.setString(2, name);
	        rs = stmt.executeQuery();
	        if(rs.next()) {
	        	int cardinality = rs.getInt(1);
	        	if (!rs.wasNull()) {
	        		table.setCardinality(cardinality);
	        	}
	        }
	    } finally { 
	        if(rs != null) {
	            rs.close();
	        }
	        if(stmt != null) {
	            stmt.close();
	        }
	    }
	}

	@Override
	protected boolean getIndexInfoForTable(String catalogName,
			String schemaName, String tableName, boolean uniqueOnly,
			boolean approximateIndexes, String tableType) {
		//oracle will throw an exception if we import non approximate with a view
		if (!approximateIndexes && "VIEW".equalsIgnoreCase(tableType)) { //$NON-NLS-1$
			return false;
		}
		return true;
	}
	
	@TranslatorProperty (display="Use Geometry Type", category=PropertyType.IMPORT, description="Use Teiid Geometry Type rather than an Object/Struct for SDO_GEOMETRY")
	public boolean isUseGeometryType() {
		return useGeometryType;
	}
	
	public void setUseGeometryType(boolean useGeometryType) {
		this.useGeometryType = useGeometryType;
	}
	
	@Override
	protected void getGeometryMetadata(Column c, Connection conn,
			String tableCatalog, String tableSchema, String tableName,
			String columnName) {
    	PreparedStatement ps = null;
    	ResultSet rs = null;
    	try {
        	ps = conn.prepareStatement("select coord_dimension, srid from ALL_GEOMETRY_COLUMNS where f_table_schema=? and f_table_name=? and f_geometry_column=?"); //$NON-NLS-1$
        	ps.setString(1, tableSchema);
        	ps.setString(2, tableName);
        	ps.setString(3, columnName);
        	rs = ps.executeQuery();
        	if (rs.next()) {
        		c.setProperty(MetadataFactory.SPATIAL_URI + "coord_dimension", rs.getString(1)); //$NON-NLS-1$
        		c.setProperty(MetadataFactory.SPATIAL_URI + "srid", rs.getString(2)); //$NON-NLS-1$
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
	protected String getFullyQualifiedName(String catalogName,
			String schemaName, String objectName, boolean quoted) {
		if (catalogName != null && catalogName.length() > 0) {
			//must be a package name
			if (quoted) {
				//name in source is required to be package.objectName
				String fullName = quoteName(catalogName) + AbstractMetadataRecord.NAME_DELIM_CHAR + quoteName(objectName);
				if (this.isUseQualifiedName() && schemaName != null && schemaName.length() > 0) {
					fullName = (quoted?quoteName(schemaName):schemaName) + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
				}
				return fullName;
			}
			return super.getFullyQualifiedName(schemaName, catalogName, objectName, quoted);
		}
		return super.getFullyQualifiedName(catalogName, schemaName, objectName, quoted);
	}
	
	@TranslatorProperty (display="Use Integral Types", category=PropertyType.IMPORT, description="Use integral types rather than decimal when the scale is 0.")
	public boolean isUseIntegralTypes() {
		return useIntegralTypes;
	}
	
	public void setUseIntegralTypes(boolean useIntegralTypes) {
		this.useIntegralTypes = useIntegralTypes;
	}

}