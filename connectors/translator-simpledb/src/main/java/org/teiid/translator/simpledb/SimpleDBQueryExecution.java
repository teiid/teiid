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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.resource.adpter.simpledb.SimpleDBDataTypeManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectResult;

public class SimpleDBQueryExecution implements ResultSetExecution {
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
        return this.visitor.toString();
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
