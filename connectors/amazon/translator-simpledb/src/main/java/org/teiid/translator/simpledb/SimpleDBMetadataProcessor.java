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
package org.teiid.translator.simpledb;

import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.simpledb.api.SimpleDBConnection;
import org.teiid.translator.simpledb.api.SimpleDBConnection.SimpleDBAttribute;

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

    private static String quote(String name) {
        return '`' + StringUtil.replaceAll(name, "`", "``") + '`'; //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static String getName(AbstractMetadataRecord record) {
        return SQLStringVisitor.getRecordName(record);
    }

    public static String getQuotedName(AbstractMetadataRecord record) {
        //don't quote itemname()
        String name = getName(record);
        if (record instanceof Column && isItemName(name)) {
            return name;
        }
        return quote(name);
    }

    public static boolean isItemName(Column column) {
        return isItemName(SimpleDBMetadataProcessor.getName(column));
    }
    public static boolean isItemName(String name) {
        return name.equalsIgnoreCase(SimpleDBMetadataProcessor.ITEM_NAME);
    }
}
