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
package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.NonReserved.KEY;
import static org.teiid.language.SQLConstants.NonReserved.RETURNING;
import static org.teiid.language.SQLConstants.NonReserved.USE;
import static org.teiid.language.SQLConstants.Reserved.INTO;
import static org.teiid.language.SQLConstants.Reserved.VALUE;
import static org.teiid.language.SQLConstants.Reserved.VALUES;
import static org.teiid.language.SQLConstants.Reserved.UPDATE;
import static org.teiid.language.SQLConstants.Reserved.AS;
import static org.teiid.language.SQLConstants.Reserved.SET;
import static org.teiid.language.SQLConstants.Tokens.COMMA;
import static org.teiid.language.SQLConstants.Tokens.SPACE;
import static org.teiid.language.SQLConstants.Tokens.LPAREN;
import static org.teiid.language.SQLConstants.Tokens.RPAREN;
import static org.teiid.language.SQLConstants.Tokens.QUOTE;
import static org.teiid.language.SQLConstants.Tokens.LSBRACE;
import static org.teiid.language.SQLConstants.Tokens.RSBRACE;
import static org.teiid.language.SQLConstants.Tokens.EQ;
import static org.teiid.translator.couchbase.CouchbaseProperties.PK;
import static org.teiid.translator.couchbase.CouchbaseProperties.SOURCE_SEPARATOR;
import static org.teiid.translator.couchbase.CouchbaseProperties.SQUARE_BRACKETS;
import static org.teiid.translator.couchbase.CouchbaseProperties.KEYS;

import java.util.List;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Iterator;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class N1QLUpdateVisitor extends N1QLVisitor {
    
    private int index = 0;
    private int dimension = 0;
    private List<CBColumnData> rowCache;
    
    private String keyspace;
    private String setAttr;

    public N1QLUpdateVisitor(CouchbaseExecutionFactory ef) {
        super(ef);
    }
    
    @Override
    public void visit(Insert obj) {
        
        append(obj.getTable());
        
        if(isArrayTable) {
            buffer.append(UPDATE).append(SPACE).append(this.keyspace);
            buffer.append(SPACE).append(USE).append(SPACE).append(KEYS).append(SPACE);
            
            append(obj.getColumns());
            append(obj.getValueSource());
            
            String documentID = null;
            JsonArray array = JsonArray.create();
            List<CBColumnData> idxList = new ArrayList<>(dimension);

            
            for(int i = 0 ; i < rowCache.size() ; i ++) {
                
                CBColumnData columnData = rowCache.get(i);
                
                if(columnData.getCBColumn().isPK()) {
                    documentID = (String)ef.retrieveValue(columnData.getColumnType(), columnData.getValue());
                } else if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else {
                    if(columnData.getCBColumn().hasLeaf()) {
                        String attr = columnData.getCBColumn().getLeafName();
                        String path = columnData.getCBColumn().getNameInSource();
                        JsonObject nestedObj = findObject(array, path);
                        if(nestedObj == null) {
                            nestedObj = JsonObject.create();
                            array.add(nestedObj);
                        }
                        ef.setValue(nestedObj, attr, columnData.getColumnType(), columnData.getValue());
                    } else {
                        ef.setValue(array, columnData.getColumnType(), columnData.getValue());
                    }
                }
            }
            
            if (idxList.size() != dimension) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29006, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29006, obj));
            }
            
            if(null == documentID) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29007, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29007, obj));
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append(setAttr);
            for(int i = 0 ; i < dimension -1 ; i ++) {
                sb.append(LSBRACE).append(idxList.get(i).getValue()).append(RSBRACE);
            }
            String setKey = sb.toString();
            
            buffer.append(QUOTE).append(escapeString(documentID, QUOTE)).append(QUOTE).append(SPACE);
            buffer.append(SET).append(SPACE).append(setKey).append(SPACE);
            buffer.append(EQ).append(SPACE).append("ARRAY_CONCAT").append(LPAREN); //$NON-NLS-1$
            buffer.append("IFMISSINGORNULL").append(LPAREN).append(setKey).append(COMMA).append(SPACE).append(SQUARE_BRACKETS); //$NON-NLS-1$
            buffer.append(RPAREN).append(COMMA).append(SPACE);
            buffer.append(array).append(RPAREN).append(SPACE);
            
        } else {
            buffer.append(getInsertKeyword()).append(SPACE);
            buffer.append(INTO).append(SPACE);
            buffer.append(keyspace); 
            buffer.append(SPACE).append(LPAREN);
            buffer.append(KEY).append(COMMA).append(SPACE);
            buffer.append(VALUE).append(RPAREN).append(SPACE);
            
            append(obj.getColumns());
            append(obj.getValueSource());
            
            String documentID = null;
            JsonObject json = JsonObject.create();
            
            for(int i = 0 ; i < rowCache.size() ; i ++) {
                
                CBColumnData columnData = rowCache.get(i);
                
                if(columnData.getCBColumn().isPK()) {
                    documentID = (String)ef.retrieveValue(columnData.getColumnType(), columnData.getValue());
                } else {
                    String attr = columnData.getCBColumn().getLeafName();
                    String path = columnData.getCBColumn().getNameInSource();
                    JsonObject nestedObj = findObject(json, path);
                    ef.setValue(nestedObj, attr, columnData.getColumnType(), columnData.getValue());
                }
            }
            
            if(null == documentID) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29007, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29007, obj));
            }
            
            buffer.append(VALUES).append(SPACE).append(LPAREN);
            buffer.append(QUOTE).append(escapeString(documentID, QUOTE)).append(QUOTE);
            buffer.append(COMMA).append(SPACE);
            buffer.append(json).append(RPAREN).append(SPACE);
        }
        
        buffer.append(RETURNING).append(SPACE).append(buildMeta(this.keyspace)).append(SPACE);
        buffer.append(AS).append(SPACE).append(PK);
    }
    
    @Override
    public void visit(NamedTable obj) {
        
        retrieveTableProperty(obj);
        
        String tableNameInSource = obj.getMetadataObject().getNameInSource();
        
        if(isArrayTable) {    
            this.keyspace = tableNameInSource.substring(0, tableNameInSource.indexOf(SOURCE_SEPARATOR));
            
            dimension = 0;
            String baseName = tableNameInSource;
            while(baseName.endsWith(SQUARE_BRACKETS)) {
                dimension ++;
                baseName = baseName.substring(0, baseName.length() - SQUARE_BRACKETS.length());
            }
            
            this.setAttr = baseName.substring(keyspace.length() + 1);
        } else {
            this.keyspace = tableNameInSource;
        }
    }

    
    private JsonObject findObject(JsonObject json, String path) {
        String[] array = path.split(Pattern.quote(SOURCE_SEPARATOR));
        return findObject(json, array);
    }
    
    private JsonObject findObject(JsonArray json, String path) {
        
        JsonObject result = null;
        for(Iterator<Object> it = json.iterator() ; it.hasNext() ;) {
            Object obj = it.next();
            if(obj instanceof JsonObject) {
                result = (JsonObject)obj;
                break;
            }
        }
        
        String nestedObjPath = path.substring(path.lastIndexOf(SQUARE_BRACKETS) + SQUARE_BRACKETS.length() + SOURCE_SEPARATOR.length());
        String[] array = nestedObjPath.split(Pattern.quote(SOURCE_SEPARATOR));
        
        return findObject(result, array);
    }
    
    private JsonObject findObject(JsonObject json, String[] array) {
        
        if(json == null) {
            return null;
        }
        
        JsonObject result = json;
        for (int i = 1; i < array.length -1 ; i ++) {
            String interPath = trimWave(array[i]);
            if(result.get(interPath) == null) {
                result.put(interPath, JsonObject.create());
            }
            result = result.getObject(interPath);
        }
        return result;
    }
    
    @Override
    public void visit(ColumnReference obj) {
        
        if(obj.getTable() != null) {    
            CBColumn column = formCBColumn(obj);
            CBColumnData cacheData = new CBColumnData(obj.getType(), column);
            this.addRowCache(cacheData);
        } else {
            super.visit(obj);
        }
    }
    
    @Override
    protected void append(List<? extends LanguageObject> items) {
        for (int i = 0; i < items.size(); i++) {
            append(items.get(i));
        }
    }
    
    @Override
    public void visit(ExpressionValueSource obj) {
        
        List<Expression> values = obj.getValues();
        
        for (int i = 0; i < values.size(); i++) {
            index = i;
            append(values.get(i));
        }
        
        index = -1;
    }
    
    @Override
    public void visit(Literal obj) {
        CBColumnData columnData = this.rowCache.get(index);
        columnData.setValue(obj.getValue());
    }
    
    public List<CBColumnData> getRowCache() {
        return rowCache;
    }

    public void addRowCache(CBColumnData data) {
        if(this.rowCache == null) {
            this.rowCache = new ArrayList<>();
        }
        this.rowCache.add(data);
    }
    
    @Override
    public void visit(Delete obj) {
        super.visit(obj);
    }

    private class CBColumnData {
        
        private Class<?> columnType;
        private CBColumn column;
        private Object value;
        
        private CBColumnData(Class<?> columnType, CBColumn column) {
            this.columnType = columnType;
            this.column = column;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Class<?> getColumnType() {
            return columnType;
        }
        
        public CBColumn getCBColumn() {
            return this.column;
        }

    }
}
