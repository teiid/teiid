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
package org.teiid.translator.solr;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.LukeResponse.FieldInfo;
import org.apache.solr.common.luke.FieldFlag;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

public class SolrMetadataProcessor implements MetadataProcessor<SolrConnection>{

    @Override
    public void process(MetadataFactory metadataFactory,
            SolrConnection connection) throws TranslatorException {
        getConnectorMetadata(connection, metadataFactory);
    }

    public void getConnectorMetadata(SolrConnection conn, MetadataFactory metadataFactory) throws TranslatorException {
        int count = 0;
        LukeRequest request = new LukeRequest();
        request.setShowSchema(true);
        LukeResponse response = conn.metadata(request);

        Map<String, FieldInfo> fields = response.getFieldInfo();

        Table table = metadataFactory.addTable(conn.getCoreName());
        table.setSupportsUpdate(true);

        for (String name:fields.keySet()) {
            FieldInfo field = fields.get(name);
            EnumSet<FieldFlag> flags = field.getFlags();
            if ((!name.startsWith("_") && !name.endsWith("_")) || name.startsWith("*") || name.endsWith("*")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                if (flags.contains(FieldFlag.INDEXED) && flags.contains(FieldFlag.STORED)) {
                    Column column = null;
                    // array type
                    if (flags.contains(FieldFlag.MULTI_VALUED)) {
                        column = metadataFactory.addColumn(field.getName(), resolveType(field.getType())+"[]", table); //$NON-NLS-1$
                    }
                    else {
                        column = metadataFactory.addColumn(field.getName(), resolveType(field.getType()), table);
                    }
                    column.setUpdatable(true);
                    column.setSearchType(SearchType.Searchable);

                    // create primary key; and unique keys
                    if (field.getDistinct() > 0 || field.getName().equals("id")) { //$NON-NLS-1$
                        if (table.getPrimaryKey() == null) {
                            metadataFactory.addPrimaryKey("PK0", Arrays.asList(field.getName()), table); //$NON-NLS-1$
                        }
                        else {
                            metadataFactory.addIndex("UI"+count, true, Arrays.asList(field.getName()), table); //$NON-NLS-1$
                            count++;
                        }
                    }
                }
            }
        }
    }

    private String resolveType(String solrType) {
        if (solrType.equals("string") || solrType.startsWith("text_") || solrType.equals("alphaOnlySort") || solrType.equals("phonetic") || solrType.equals("payloads") ||solrType.equals("lowercase")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            return DataTypeManager.DefaultDataTypes.STRING;
        }
        else if (solrType.equals("int") || solrType.equals("tint") || solrType.equals("pint")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return DataTypeManager.DefaultDataTypes.INTEGER;
        }
        else if (solrType.equals("boolean") || solrType.equals("tboolean") || solrType.equals("pboolean")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return DataTypeManager.DefaultDataTypes.BOOLEAN;
        }
        else if (solrType.equals("binary")) { //$NON-NLS-1$
            return DataTypeManager.DefaultDataTypes.BLOB;
        }
        else if (solrType.equals("date") || solrType.equals("tdate") || solrType.equals("pdate")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return DataTypeManager.DefaultDataTypes.TIMESTAMP;
        }
        else if (solrType.equals("float") || solrType.equals("tfloat") || solrType.equals("pfloat")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return DataTypeManager.DefaultDataTypes.FLOAT;
        }
        else if (solrType.equals("long") || solrType.equals("tlong") || solrType.equals("plong")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return DataTypeManager.DefaultDataTypes.LONG;
        }
        else if (solrType.equals("double") || solrType.equals("tdouble") || solrType.equals("pdouble") || solrType.equals("currency")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            return DataTypeManager.DefaultDataTypes.DOUBLE;
        }
        return DataTypeManager.DefaultDataTypes.STRING;
    }
}
