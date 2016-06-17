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
package org.teiid.translator.jdbc.pi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;

public class PIMetadataProcessor extends JDBCMetdataProcessor {
    static Pattern guidPattern = Pattern.compile(Pattern.quote("guid"), Pattern.CASE_INSENSITIVE);
    
    @ExtensionMetadataProperty(applicable= {Table.class, Procedure.class}, 
            datatype=String.class, display="Is Table Value Function", 
            description="Marks the table as Table Value Function")
    public static final String TVF = MetadataFactory.PI_URI+"TVF"; //$NON-NLS-1$
    
    public PIMetadataProcessor() {
        setStartQuoteString("[");
        setEndQuoteString("]");
    }
    
    protected String getRuntimeType(int type, String typeName, int precision, int scale) {
        String rtType = super.getRuntimeType(type, typeName, precision);
        if (typeName != null && guidPattern.matcher(typeName).find()) {
            rtType = TypeFacility.RUNTIME_NAMES.STRING;
        }
        return rtType;
    } 
    
    public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
            throws SQLException {
        super.getConnectorMetadata(conn, metadataFactory);
        for (String name:metadataFactory.getSchema().getTables().keySet()) {
            if (name.startsWith("ft_")) {
                Table table = metadataFactory.getSchema().getTable(name);
                table.setProperty(TVF, "true");
            }
        }
        for (String name:metadataFactory.getSchema().getProcedures().keySet()) {
            Procedure proc = metadataFactory.getSchema().getProcedure(name);
            proc.setProperty(TVF, "true");
        }         
    }
}
