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
package org.teiid.translator.simpledb;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection.SimpleDBAttribute;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

public class SimpleDBMetadataProcessor implements MetadataProcessor<SimpleDBConnection> {
    public static final String ITEM_NAME = SimpleDBConnection.ITEM_NAME;
    private static final String DISPLAY_ITEM_NAME = "ItemName"; //$NON-NLS-1$
    
    /**
     * As SimpleDB does not provide any way to obtain all attribute names for
     * given domain (one can query only attribute names for single item) and
     * querrying all items in domain to get complete set of attribute names
     * would be very slow and resource consuming, this approach has been
     * selected: For each domain only first item is queried for attribute names
     * and metadata are created using this information. Thus first item in
     * domain should have as much attributes as possible.
     * @see org.teiid.translator.MetadataProcessor#process(org.teiid.metadata.MetadataFactory, java.lang.Object)
     */
    @Override
    public void process(MetadataFactory metadataFactory, SimpleDBConnection connection) throws TranslatorException {
        List<String> domains = connection.getDomains();
        for (String domain : domains) {
            Table table = metadataFactory.addTable(domain);
            table.setSupportsUpdate(true);
            
            Column itemName = metadataFactory.addColumn(DISPLAY_ITEM_NAME, TypeFacility.RUNTIME_NAMES.STRING, table);
            itemName.setUpdatable(true);
            itemName.setNameInSource(ITEM_NAME);
            itemName.setNullType(NullType.No_Nulls);
            metadataFactory.addPrimaryKey("PK0", Arrays.asList(DISPLAY_ITEM_NAME), table); //$NON-NLS-1$
            
            for (SimpleDBAttribute attribute : connection.getAttributeNames(domain)) {
                Column column = null;
                if (attribute.hasMultipleValues()) {
                    column = metadataFactory.addColumn(attribute.getName(), TypeFacility.RUNTIME_NAMES.STRING+"[]", table); //$NON-NLS-1$
                }
                else {
                    column = metadataFactory.addColumn(attribute.getName(), TypeFacility.RUNTIME_NAMES.STRING, table);
                }
                column.setUpdatable(true);
                column.setNullType(NullType.Nullable);
            }
        }        
    }
    
    public static String getName(AbstractMetadataRecord record) {
        return SQLStringVisitor.getShortName(SQLStringVisitor.getRecordName(record));
    }
    
    public static boolean isItemName(Column column) {
        return SimpleDBMetadataProcessor.getName(column).equalsIgnoreCase(SimpleDBMetadataProcessor.ITEM_NAME);        
    }
    public static boolean isItemName(String name) {
        return name.equalsIgnoreCase(SimpleDBMetadataProcessor.ITEM_NAME);        
    }    
}
