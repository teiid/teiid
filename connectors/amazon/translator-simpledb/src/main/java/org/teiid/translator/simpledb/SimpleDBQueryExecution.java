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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.api.SimpleDBConnection;
import org.teiid.translator.simpledb.api.SimpleDBDataTypeManager;

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectResult;

public class SimpleDBQueryExecution implements ResultSetExecution {
    private static final int MAX_PAGE_SIZE = 2500;
    private Class<?>[] expectedColumnTypes;
    @SuppressWarnings("unused")
    private ExecutionContext executionContext;
    @SuppressWarnings("unused")
    private RuntimeMetadata metadata;
    protected SimpleDBConnection connection;
    private SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
    private String nextToken;
    protected Iterator<Item> listIterator;

    public SimpleDBQueryExecution(final Select command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            final SimpleDBConnection connection) throws TranslatorException {

        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;

        if (command != null) {
            this.visitor.append(command);
            this.visitor.checkExceptions();
            this.expectedColumnTypes  = command.getColumnTypes();
        }
    }

    @Override
    public void execute() throws TranslatorException {
        executeDirect(getSQL(), null);
    }

    protected String getSQL() {
        return this.visitor.toString() + " LIMIT " + Math.min(this.executionContext.getBatchSize(), MAX_PAGE_SIZE); //$NON-NLS-1$
    }

    protected void executeDirect(String sql, String next) throws TranslatorException {
        SelectResult result = connection.performSelect(sql, next);
        this.nextToken = result.getNextToken();
        this.listIterator = result.getItems().iterator();
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (this.listIterator.hasNext()) {
            return buildRow(this.listIterator.next());
        }
        if (this.nextToken != null) {
            executeDirect(getSQL(), this.nextToken);
            if (this.listIterator.hasNext()) {
                return buildRow(this.listIterator.next());
            }
        }
        return null;
    }

    protected List<?> buildRow(Item item) throws TranslatorException {
        Map<String, List<String>> valueMap = createAttributeMap(item.getAttributes());
        List row = new ArrayList();
        for (int i = 0; i < visitor.getProjectedColumns().size(); i++) {
            String columnName = visitor.getProjectedColumns().get(i);
            if (SimpleDBMetadataProcessor.isItemName(columnName)) {
                row.add(SimpleDBDataTypeManager.convertFromSimpleDBType(Arrays.asList(item.getName()), expectedColumnTypes[i]));
                continue;
            }
            row.add(SimpleDBDataTypeManager.convertFromSimpleDBType(valueMap.get(columnName), expectedColumnTypes[i]));
        }
        return row;
    }

    protected Map<String, List<String>> createAttributeMap(List<Attribute> attributes) {
        Map<String, List<String>> map = new TreeMap<String, List<String>>();
        for (Attribute attribute : attributes) {
            if (map.get(attribute.getName()) == null) {
                List<String> list = new ArrayList<String>();
                list.add(attribute.getValue());
                map.put(attribute.getName(), list);
            }
            else {
                map.get(attribute.getName()).add(attribute.getValue());
            }
        }
        return map;
    }
}
