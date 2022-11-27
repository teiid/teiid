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
package org.teiid.translator.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;

public class HiveMetadataProcessor extends JDBCMetadataProcessor implements MetadataProcessor<Connection>{

    private boolean trimColumnNames;
    private boolean useDatabaseMetaData;

    public HiveMetadataProcessor() {
        setImportKeys(false);
        setQuoteString("`"); //$NON-NLS-1$
    }

    @Override
    public void process(MetadataFactory metadataFactory, Connection conn)    throws TranslatorException {
        try {
            getConnectorMetadata(conn, metadataFactory);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
            throws SQLException, TranslatorException {
        if (useDatabaseMetaData) {
            super.getConnectorMetadata(conn, metadataFactory);
            return;
        }
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
        else if (StringUtil.startsWithIgnoreCase(type, "decimal")) { //$NON-NLS-1$
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
            column.setNameInSource(quoteName(name));
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

    @TranslatorProperty(display="Use DatabaseMetaData", category=PropertyType.IMPORT, description= "Use DatabaseMetaData (typical JDBC logic) for importing")
    public boolean isUseDatabaseMetaData() {
        return useDatabaseMetaData;
    }

    public void setUseDatabaseMetaData(boolean useDatabaseMetaData) {
        this.useDatabaseMetaData = useDatabaseMetaData;
    }

}
