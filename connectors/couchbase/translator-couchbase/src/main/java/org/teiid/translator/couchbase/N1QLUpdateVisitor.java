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
import static org.teiid.language.SQLConstants.Reserved.DELETE;
import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.WHERE;
import static org.teiid.language.SQLConstants.Reserved.AS;
import static org.teiid.language.SQLConstants.Reserved.SET;
import static org.teiid.language.SQLConstants.Reserved.AND;
import static org.teiid.language.SQLConstants.Tokens.COMMA;
import static org.teiid.language.SQLConstants.Tokens.SPACE;
import static org.teiid.language.SQLConstants.Tokens.LPAREN;
import static org.teiid.language.SQLConstants.Tokens.RPAREN;
import static org.teiid.language.SQLConstants.Tokens.QUOTE;
import static org.teiid.language.SQLConstants.Tokens.LSBRACE;
import static org.teiid.language.SQLConstants.Tokens.RSBRACE;
import static org.teiid.language.SQLConstants.Tokens.EQ;
import static org.teiid.language.SQLConstants.Tokens.COLON;
import static org.teiid.translator.couchbase.CouchbaseProperties.PK;
import static org.teiid.translator.couchbase.CouchbaseProperties.SOURCE_SEPARATOR;
import static org.teiid.translator.couchbase.CouchbaseProperties.SQUARE_BRACKETS;
import static org.teiid.translator.couchbase.CouchbaseProperties.KEYS;

import java.util.List;
import java.util.regex.Pattern;

import java.util.ArrayList;
import java.util.Iterator;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class N1QLUpdateVisitor extends N1QLVisitor {
    
    private int dimension = 0;
    private List<CBColumnData> rowCache;
    private List<Object> literalStack;
    
    private List<String> comparisonStack;
    private List<String> conditionStack;
    
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
            literalValueMapping();
            
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
            
            String setKey = buildSetKey(setAttr, dimension, idxList);
            
            buffer.append(QUOTE).append(escapeString(documentID, QUOTE)).append(QUOTE).append(SPACE);
            
            StringBuilder left = new StringBuilder();
            left.append("IFMISSINGORNULL").append(LPAREN).append(setKey).append(COMMA).append(SPACE).append(SQUARE_BRACKETS).append(RPAREN);
            appendConcat(setKey, left, array);
            buffer.append(SPACE);
        } else {
            buffer.append(getInsertKeyword()).append(SPACE);
            buffer.append(INTO).append(SPACE);
            buffer.append(keyspace); 
            buffer.append(SPACE).append(LPAREN);
            buffer.append(KEY).append(COMMA).append(SPACE);
            buffer.append(VALUE).append(RPAREN).append(SPACE);
            
            append(obj.getColumns());
            append(obj.getValueSource());
            literalValueMapping();
            
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
    
    private void appendConcat(String setKey, Object left, Object right) {
        buffer.append(SET).append(SPACE).append(setKey).append(SPACE);
        buffer.append(EQ).append(SPACE).append("ARRAY_CONCAT").append(LPAREN); //$NON-NLS-1$
        buffer.append(left);
        buffer.append(COMMA).append(SPACE);
        buffer.append(right).append(RPAREN);
    }
    
    private String buildSetKey(String setAttr, int dimension, List<CBColumnData> idxList) {
        StringBuilder sb = new StringBuilder();
        sb.append(setAttr);
        for(int i = 0 ; i < dimension -1 ; i ++) {
            sb.append(LSBRACE).append(idxList.get(i).getValue()).append(RSBRACE);
        }
        return sb.toString();
    }
    
    private void literalValueMapping() { 
        for(int i = 0 ; i < this.rowCache.size() ; i ++) {
            CBColumnData columnData = rowCache.get(i);
            columnData.setValue(this.literalStack.get(i));
        }
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
            append(values.get(i));
        }
        
    }
    
    @Override
    public void visit(Literal obj) {
        this.addLiteral(obj.getValue());
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
    
    public List<Object> getLiterals() {
        return literalStack;
    }

    public void addLiteral(Object value) {
        if(this.literalStack == null) {
            this.literalStack = new ArrayList<>();
        }
        this.literalStack.add(value);
    }

    public List<String> getComparisons() {
        return comparisonStack;
    }

    public void addComparison(String operator) {
        if(this.comparisonStack == null) {
            this.comparisonStack = new ArrayList<>();
        }
        this.comparisonStack.add(operator);
    }

    public List<String> getConditions() {
        return conditionStack;
    }

    public void addCondition(String operator) {
        if(this.conditionStack == null) {
            this.conditionStack = new ArrayList<>();
        }
        this.conditionStack.add(operator);
    }

    @Override
    public void visit(Delete obj) {
        
        append(obj.getTable());
        if (obj.getWhere() != null) {
            append(obj.getWhere());
        }
        literalValueMapping();

        if(isArrayTable) {// delete array depend on array index, and optional docuemntID     
            buffer.append(UPDATE).append(SPACE).append(this.keyspace).append(SPACE);
            
            CBColumnData pk = null;
            List<CBColumnData> idxList = new ArrayList<>(dimension);
            for(CBColumnData columnData : rowCache) {
                if(columnData.getCBColumn().isPK()) {
                    pk = columnData;
                } else if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else {
                    //todo--, support comparison via array content 
                }
            }
            
            if (idxList.size() != dimension) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29005, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29005, obj));
            }
            
            if(pk == null) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29017, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29017, obj));
            }
            
            buffer.append(USE).append(SPACE).append(KEYS).append(SPACE).append(setValue(pk.getColumnType(), pk.getValue())).append(SPACE);
            
            String setKey = buildSetKey(setAttr, dimension, idxList);
            String left = SQUARE_BRACKETS;
            String right = SQUARE_BRACKETS;
            int idx = (int)idxList.get(idxList.size() -1).getValue();
            if(idx > 0) {
                left = setKey + LSBRACE + 0 + COLON + idx + RSBRACE;
                right = setKey + LSBRACE + (idx + 1) + COLON + RSBRACE;
            } else {
                right = setKey + LSBRACE + 1 + COLON + RSBRACE;
            }
            appendConcat(setKey, left, right);
        } else {       
            buffer.append(DELETE).append(SPACE).append(FROM).append(SPACE);
            buffer.append(this.keyspace).append(SPACE);
            
            String useKey = this.appendUseKey();
            if(useKey != null) {
                buffer.append(useKey);
            } else {
                buffer.append(this.appendWhere());
            }
        }
    }
    
    private String setValue(Class<?> type, Object value) {
        if(type.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            return QUOTE + value + QUOTE;
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.INTEGER) || type.equals(TypeFacility.RUNTIME_TYPES.LONG) 
                || type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE) || type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)
                || type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN) || type.equals(TypeFacility.RUNTIME_TYPES.BIG_INTEGER)
                || type.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            return String.valueOf(value);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.NULL)) {
            return null;
        }
        return null;
    }
    
    @Override
    public void visit(AndOr obj) {
        appendNestedCondition(obj, obj.getLeftCondition());
        this.addCondition(obj.getOperator().toString());
        appendNestedCondition(obj, obj.getRightCondition());
    }
    
    @Override
    protected void appendNestedCondition(AndOr parent, Condition condition) {
        append(condition);
    }
    
    @Override
    public void visit(Comparison obj) {
        append(obj.getLeftExpression());
        this.addComparison(obj.getOperator().toString());
        appendRightComparison(obj);
    }
    
    @Override
    public void visit(Update obj) {
        
        append(obj.getTable());
        buffer.append(UPDATE).append(SPACE).append(this.keyspace).append(SPACE);
        
        append(obj.getChanges());
        literalValueMapping();
        
        if(isArrayTable) {
            List<CBColumnData> setList = new ArrayList<>(this.rowCache.size());
            for(int i = 0 ; i < this.rowCache.size() ; i ++) {
                if(rowCache.get(i).getCBColumn().isPK() || rowCache.get(i).getCBColumn().isIdx()) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29018, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29018, obj));
                }
                setList.add(rowCache.get(i));
            }
            
            this.rowCache.clear();
            this.literalStack.clear();
            if (obj.getWhere() != null) {
                append(obj.getWhere());
            }
            literalValueMapping();
            
            CBColumnData pk = null;
            List<CBColumnData> idxList = new ArrayList<>(dimension);
            for(CBColumnData columnData : rowCache) {
                if(columnData.getCBColumn().isPK()) {
                    pk = columnData;
                } else if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else {
                    // todo-- array content comparison
                }
            }
            
            if (idxList.size() != dimension) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29020, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29020, obj));
            }
            
            if(pk == null) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29019, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29019, obj));
            }
            
            buffer.append(USE).append(SPACE).append(KEYS).append(SPACE).append(setValue(pk.getColumnType(), pk.getValue())).append(SPACE);
            
            String setKey = buildSetKey(setAttr, dimension, idxList) + LSBRACE + idxList.get(idxList.size() - 1).getValue() + RSBRACE;
            buffer.append(SET).append(SPACE).append(setKey).append(SPACE).append(EQ).append(SPACE);
            
            JsonObject nestedObj = null;
            for(CBColumnData columnData : setList) {
                if(columnData.getCBColumn().hasLeaf()) {
                    if(nestedObj == null) {
                        nestedObj = JsonObject.create();
                    }
                    String attr = columnData.getCBColumn().getLeafName();
                    String path = columnData.getCBColumn().getNameInSource();
                    path = path.substring(path.lastIndexOf(SQUARE_BRACKETS) + SQUARE_BRACKETS.length() + SOURCE_SEPARATOR.length());
                    JsonObject json = this.findObject(nestedObj, path);
                    ef.setValue(json, attr, columnData.getColumnType(), columnData.getValue());
                }
            }
            
            if(nestedObj != null) {
                buffer.append(nestedObj);
            } else {
                buffer.append(this.setValue(setList.get(0).getColumnType(), setList.get(0).getValue()));
            }
            
        } else {
            StringBuilder setBuffer = new StringBuilder();
            setBuffer.append(SET).append(SPACE);
            boolean comma = false;
            for(int i = 0 ; i < this.rowCache.size() ; i ++) {
                if(rowCache.get(i).getCBColumn().isPK()) {
                    continue;
                }
                if(comma) {
                    setBuffer.append(COMMA).append(SPACE);
                }
                comma = true;
                setBuffer.append(rowCache.get(i).getCBColumn().getNameInSource()).append(SPACE).append(EQ).append(SPACE).append(setValue(rowCache.get(i).getColumnType(), rowCache.get(i).getValue()));
            }
            
            this.rowCache.clear();
            this.literalStack.clear();
            if (obj.getWhere() != null) {
                append(obj.getWhere());
            }
            literalValueMapping();

            String useKey = this.appendUseKey();
            String where = this.appendWhere();
            
            if(useKey != null) {
                buffer.append(useKey).append(SPACE).append(setBuffer);
            } else {
                buffer.append(setBuffer).append(SPACE).append(where);
            }
            
        }
    }
    
    private String appendUseKey() {
        CBColumnData pk = null;
        for(int i = 0 ; i < this.rowCache.size() ; i++) {
            if(rowCache.get(i).getCBColumn().isPK()) {
                pk = rowCache.get(i);
                break;
            }
        }
        
        if(pk != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(USE).append(SPACE).append(KEYS).append(SPACE);
            sb.append(setValue(pk.getColumnType(), pk.getValue()));
            return sb.toString();
        } else {
            return null;
        }
    }
    
    private String appendWhere() {
        
        StringBuilder whereBuffer = new StringBuilder();
        whereBuffer.append(WHERE);
        
        boolean isTypedInProjection = false;
        
        for(int i = 0 ; i < this.rowCache.size() ; i++) {

            CBColumnData columnData = rowCache.get(i);
            
            whereBuffer.append(SPACE);
            
            if(i > 0) {
                whereBuffer.append(this.conditionStack.get(i - 1)).append(SPACE);
            }
            
            if(i > 1 && !this.conditionStack.get(i - 1).equals(this.conditionStack.get(i - 2))) {
                whereBuffer.append(LPAREN);
            }
            
            if(typedName != null && typedName.equals(nameInSource(columnData.getCBColumn().getLeafName()))) {
                isTypedInProjection = true;
            }
            
            whereBuffer.append(columnData.getCBColumn().getNameInSource()).append(SPACE);
            whereBuffer.append(this.comparisonStack.get(i)).append(SPACE);
            whereBuffer.append(setValue(columnData.getColumnType(), columnData.getValue()));
            
            if(i > 1 && !this.conditionStack.get(i - 1).equals(this.conditionStack.get(i - 2))) {
                whereBuffer.append(RPAREN);
            }
        }
        
        if(!isTypedInProjection && typedName != null && typedValue != null) {
            if(this.rowCache.size() == 0) {
                whereBuffer.append(SPACE).append(keyspace).append(SOURCE_SEPARATOR).append(typedName).append(SPACE).append(EQ).append(SPACE).append(typedValue);
            } else {
                whereBuffer.append(SPACE).append(AND).append(SPACE).append(keyspace).append(SOURCE_SEPARATOR).append(typedName).append(SPACE).append(EQ).append(SPACE).append(typedValue);
            }
        }
        
        return whereBuffer.toString();
    }
    
    @Override
    public void visit(SetClause clause) {
        append(clause.getSymbol());
        append(clause.getValue());
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
