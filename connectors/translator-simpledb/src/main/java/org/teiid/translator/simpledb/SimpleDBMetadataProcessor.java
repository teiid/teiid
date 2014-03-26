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

import java.util.List;
import java.util.Map;

import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

public class SimpleDBMetadataProcessor implements MetadataProcessor<SimpleDBConnection> {

    /**
     * As SimpleDB does not provide any way to obtain all attribute names for
     * given domain (one can query only attribute names for single item) and
     * querrying all items in domain to get complete set of attribute names
     * would be very slow and resource consuming, this approach has been
     * selected: For each domain only firt item is queried for attribute names
     * and metadata are created using this information. Thus first item in
     * domain should have as much attributes as possible.
     * @see org.teiid.translator.MetadataProcessor#process(org.teiid.metadata.MetadataFactory, java.lang.Object)
     */
    @Override
    public void process(MetadataFactory metadataFactory, SimpleDBConnection connection) throws TranslatorException {
        List<String> domains = connection.getAPIClass().getDomains();
        for (String domain : domains) {
            Table table = metadataFactory.addTable(domain);
            table.setSupportsUpdate(true);
            Column itemName = new Column();
            itemName.setName("itemName()"); //$NON-NLS-1$
            itemName.setUpdatable(true);
            itemName.setNullType(NullType.No_Nulls);
            Map<String, Datatype> datatypes = metadataFactory.getDataTypes();
            itemName.setDatatype(datatypes.get("String")); //$NON-NLS-1$
            table.addColumn(itemName);
            for (String attributeName : connection.getAPIClass().getAttributeNames(domain)) {
                Column column = new Column();
                column.setUpdatable(true);
                column.setName(attributeName);
                column.setNullType(NullType.Nullable);
                column.setDatatype(datatypes.get("String")); //$NON-NLS-1$
                table.addColumn(column);
            }
        }        
    }

}
