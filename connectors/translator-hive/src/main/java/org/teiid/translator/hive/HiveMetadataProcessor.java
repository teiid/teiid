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
package org.teiid.translator.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.*;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;

public class HiveMetadataProcessor extends JDBCMetdataProcessor implements MetadataProcessor<Connection>{

	private boolean trimColumnNames;

    @Override
	public void process(MetadataFactory metadataFactory, Connection conn)	throws TranslatorException {
		try {
		    getConnectorMetadata(conn, metadataFactory);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
	}
    
    @Override
    public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
            throws SQLException {
        List<String> tables = getTables(conn);
        for (String table:tables) {
            if (shouldExclude(table)) {
                continue;
            }
            addTable(table, conn, metadataFactory);
        }
    }
    
	private List<String> getTables(Connection conn) throws SQLException {
		ArrayList<String> tables = new ArrayList<String>();
		Statement stmt = conn.createStatement();
		ResultSet rs =  stmt.executeQuery("SHOW TABLES"); //$NON-NLS-1$
		while (rs.next()){
			tables.add(rs.getString(1));
		}
		rs.close();
		return tables;
	}

	private String getRuntimeType(String type) {
		if (type.equalsIgnoreCase("int")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.INTEGER;
		}
		else if (type.equalsIgnoreCase("tinyint")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.BYTE;
		}
		else if (type.equalsIgnoreCase("smallint")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.SHORT;
		}
		else if (type.equalsIgnoreCase("bigint")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.LONG;
		}
		else if (type.equalsIgnoreCase("string")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.STRING;
		}
		else if (type.equalsIgnoreCase("float")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.FLOAT;
		}
		else if (type.equalsIgnoreCase("double")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.DOUBLE;
		}
		else if (type.equalsIgnoreCase("boolean")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.BOOLEAN;
		}
		else if (type.equalsIgnoreCase("decimal")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.BIG_DECIMAL;
		}
		else if (type.equalsIgnoreCase("timestamp")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.TIMESTAMP;
		}
		else if (type.equalsIgnoreCase("date")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.DATE;
		}
		else if (type.equalsIgnoreCase("BINARY")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.VARBINARY;
		}
		else if (type.equalsIgnoreCase("varchar")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.STRING;
		}
		return TypeFacility.RUNTIME_NAMES.STRING;
	}

	private void addTable(String tableName, Connection conn, MetadataFactory metadataFactory) throws SQLException {
		Table table = addTable(metadataFactory, null, null, tableName, null, tableName);
		if (table == null) {
			return;
		}
		Statement stmt = conn.createStatement();
		ResultSet rs =  stmt.executeQuery("DESCRIBE "+tableName); //$NON-NLS-1$
		while (rs.next()){
			String name = rs.getString(1);
			if (this.trimColumnNames) {
				name = name.trim();
			}
			String type = rs.getString(2);
			if (type != null) {
				type = type.trim();
			}
			String runtimeType = getRuntimeType(type);

			Column column = metadataFactory.addColumn(name, runtimeType, table);
			column.setNameInSource(name);
			column.setUpdatable(true);
		}
		rs.close();
	}

	public void setTrimColumnNames(boolean trimColumnNames) {
		this.trimColumnNames = trimColumnNames;
	}
	
	@TranslatorProperty(display="Trim Columns", category=PropertyType.IMPORT, description="Trim column names read from the database")
    public boolean isTrimColumnNames() {
        return trimColumnNames;
    }	
}
