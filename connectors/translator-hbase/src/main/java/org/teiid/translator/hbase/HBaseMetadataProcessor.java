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
package org.teiid.translator.hbase;

import java.sql.Connection;
import java.sql.SQLException;

import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.hbase.phoenix.PhoenixUtils;

public class HBaseMetadataProcessor implements MetadataProcessor<Connection> {
    
    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Family", description="Column Family.  Needed when auto generating the HBase tables.", required=true)    
    public static final String COLUMN_FAMILY = MetadataFactory.HBASE_URI + "COLUMN_FAMILY"; //$NON-NLS-1$

    private boolean createTables = true;
    
    @Override
    public void process(MetadataFactory metadataFactory, Connection connection)
    		throws TranslatorException {
    	if (!createTables) {
    		return;
    	}
    	for (Table t : metadataFactory.getSchema().getTables().values()) {
    		if (t.getTableType() != Type.Table || t.isVirtual()) {
    			continue;
    		}
            try {
                PhoenixUtils.executeUpdate(connection, PhoenixUtils.hbaseTableMappingDDL(t));
            } catch (SQLException e) {
                throw new TranslatorException(HBasePlugin.Event.TEIID27005, HBasePlugin.Util.gs(HBasePlugin.Event.TEIID27015, e.getMessage()));
            }
    	}
    }
    
    @TranslatorProperty(display="Create Tables", category=PropertyType.IMPORT, description="Create tables based upon the source metadata.")
    public boolean isCreateTables() {
		return createTables;
	}
    
    public void setCreateTables(boolean createTables) {
		this.createTables = createTables;
	}

}
