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

import static org.teiid.language.SQLConstants.NonReserved.*;
import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.translator.couchbase.CouchbaseProperties.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class N1QLUpdateVisitor extends N1QLVisitor {
    
    private int dimension = 0;
    
    private String keyspace;
    private String setAttr;

    public N1QLUpdateVisitor(CouchbaseExecutionFactory ef) {
        super(ef);
    }
    
    @Override
    public void visit(Insert obj) {
        
        visit(obj.getTable());
        
        List<CBColumnData> rowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();
        
        for (ColumnReference col : obj.getColumns()) {
            CBColumn column = formCBColumn(col);
            CBColumnData cacheData = new CBColumnData(col.getType(), column);
            rowCache.add(cacheData);
        }
        
        ExpressionValueSource evs = (ExpressionValueSource)obj.getValueSource();
        for (int i = 0; i < evs.getValues().size(); i++) {
            Literal l = (Literal)evs.getValues().get(i);
            rowCache.get(i).setValue(l.getValue());
        }
        
        if(isArrayTable) {
            buffer.append(UPDATE).append(SPACE).append(this.keyspace).append(SPACE);

            appendDocumentID(obj, rowCache);
            String arrayIDX = buildNestedArrayIdx(obj, rowCache);
            
            JsonArray array = JsonArray.create();
            for(int i = 0 ; i < rowCache.size() ; i ++) {
                CBColumnData columnData = rowCache.get(i);   
                if(!columnData.getCBColumn().isPK() && !columnData.getCBColumn().isIdx()) {
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

            StringBuilder left = new StringBuilder();
            left.append("IFMISSINGORNULL").append(LPAREN).append(arrayIDX).append(COMMA).append(SPACE).append(SQUARE_BRACKETS).append(RPAREN);
            appendConcat(arrayIDX, left, array);
        } else {
            buffer.append(getInsertKeyword()).append(SPACE).append(INTO).append(SPACE).append(keyspace).append(SPACE); 
            buffer.append(LPAREN).append(KEY).append(COMMA).append(SPACE).append(VALUE).append(RPAREN).append(SPACE);
            
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
            
            buffer.append(VALUES).append(SPACE).append(LPAREN).append(SQLConstants.Tokens.QUOTE).append(escapeString(documentID, SQLConstants.Tokens.QUOTE)).append(SQLConstants.Tokens.QUOTE);
            buffer.append(COMMA).append(SPACE).append(json).append(RPAREN);
        }
        
        appendRetuning();
    }
    
    private void appendRetuning() {
        buffer.append(SPACE).append(RETURNING).append(SPACE).append(buildMeta(this.keyspace)).append(SPACE);
        buffer.append(AS).append(SPACE).append(PK);
    }
    
    private void appendConcat(String setKey, Object left, Object right) {
        buffer.append(SPACE).append(SET).append(SPACE).append(setKey).append(SPACE);
        buffer.append(EQ).append(SPACE).append("ARRAY_CONCAT").append(LPAREN); //$NON-NLS-1$
        buffer.append(left);
        buffer.append(COMMA).append(SPACE);
        buffer.append(right).append(RPAREN);
    }
    
    private void appendDocumentID(LanguageObject obj, List<CBColumnData> rowCache) {
        
        CBColumnData pk = null;
        for(CBColumnData columnData : rowCache) {
            if(columnData.getCBColumn().isPK()) {
                pk = columnData;
                break;
            }
        }
        
        if(pk == null) {
            throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29006, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29006, obj));
        } 
        
        buffer.append(getUsesKeyString(rowCache));
    }
    
    private String buildNestedArrayIdx(LanguageObject obj, List<CBColumnData> rowCache) {
        
        List<CBColumnData> idxList = new ArrayList<>(dimension);
        for(CBColumnData columnData : rowCache) {
            if(columnData.getCBColumn().isIdx()) {
                idxList.add(columnData);
            }
        }
        
        return buildNestedArrayIdx(setAttr, dimension, idxList, obj);
    }
    
    private String buildNestedArrayIdx(String setAttr, int dimension, List<CBColumnData> idxList, LanguageObject obj) {
        
        if (idxList.size() != dimension) {
            throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29005, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29005, obj));
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(setAttr);
        for(int i = 0 ; i < dimension -1 ; i ++) {
            sb.append(LSBRACE).append(idxList.get(i).getValue()).append(RSBRACE);
        }
        return sb.toString();
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
            buffer.append(column.getNameInSource());
        } else {
            super.visit(obj);
        }
    }
    
    @Override
    public void visit(Delete obj) {
        
        visit(obj.getTable());
        
        Condition where = obj.getWhere();
        
        List<CBColumnData> rowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();
        List<Condition> conditions = findEqualityPredicates(where, rowCache);

        if(isArrayTable) {// delete array depend on array index, and optional docuemntID
            if (!conditions.isEmpty()) {
                throw new AssertionError();
            }
            buffer.append(UPDATE).append(SPACE).append(this.keyspace).append(SPACE);
            
            appendDocumentID(obj, rowCache);
            
            List<CBColumnData> idxList = new ArrayList<>(dimension);
            for(CBColumnData columnData : rowCache) {
                if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else if (!columnData.getCBColumn().isPK()) {
                    throw new AssertionError();
                }
            }
            
            String setKey = buildNestedArrayIdx(setAttr, dimension, idxList, obj);
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
            appendRetuning();
        } else {       
            buffer.append(DELETE).append(SPACE).append(FROM).append(SPACE);
            buffer.append(this.keyspace);
            appendClauses(rowCache, conditions, null);
        }
    }

    /**
     * Add equality predicates to the rowCache as CBColumnData entries and return
     * a pruned list of remaining predicates
     * @param where
     * @param rowCache
     * @return
     */
    private List<Condition> findEqualityPredicates(Condition where,
            List<CBColumnData> rowCache) {
        List<Condition> conditions = LanguageUtil.separateCriteriaByAnd(where);
        for (Iterator<Condition> iter = conditions.iterator(); iter.hasNext();) {
            Condition c = iter.next();
            if (!(c instanceof Comparison)) {
                continue;
            }
            Comparison comp = (Comparison)c;
            if (comp.getOperator() == Comparison.Operator.EQ 
                    && comp.getLeftExpression() instanceof ColumnReference) {
                ColumnReference col =  (ColumnReference) comp.getLeftExpression();
                CBColumn column = formCBColumn(col);
                CBColumnData cacheData = new CBColumnData(col.getType(), column);
                cacheData.setValue(((Literal)comp.getRightExpression()).getValue());
                rowCache.add(cacheData);
                iter.remove();
            }
        }
        return conditions;
    }
    
    private String getValueString(Class<?> type, Object value) {
        if (value == null) {
            return null;
        }
        if(type.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            return SQLConstants.Tokens.QUOTE + StringUtil.replace(value.toString(), SQLConstants.Tokens.QUOTE, "\\u0027") + SQLConstants.Tokens.QUOTE; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.INTEGER) || type.equals(TypeFacility.RUNTIME_TYPES.LONG) 
                || type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE) || type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)
                || type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN) || type.equals(TypeFacility.RUNTIME_TYPES.BIG_INTEGER)
                || type.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            return String.valueOf(value);
        }
        throw new AssertionError("Unknown literal type: " + type); //$NON-NLS-1$
    }
    
    @Override
    public void visit(Update obj) {
        
        visit(obj.getTable());
        buffer.append(UPDATE).append(SPACE).append(this.keyspace);
        
        Condition where = obj.getWhere();
        
        List<CBColumnData> whereRowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();
        List<Condition> conditions = findEqualityPredicates(where, whereRowCache);
        
        if(isArrayTable) {
            if (!conditions.isEmpty()) {
                throw new AssertionError();
            }
            List<CBColumnData> rowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();
            for (SetClause clause : obj.getChanges()) {
                ColumnReference col = clause.getSymbol();
                CBColumn column = formCBColumn(col);
                CBColumnData cacheData = new CBColumnData(col.getType(), column);
                if (!(clause.getValue() instanceof Literal)) {
                    throw new AssertionError();
                }
                cacheData.setValue(((Literal)clause.getValue()).getValue());
                rowCache.add(cacheData);
            }
            
            List<CBColumnData> setList = new ArrayList<>(rowCache.size());
            for(int i = 0 ; i < rowCache.size() ; i ++) {
                if(rowCache.get(i).getCBColumn().isPK() || rowCache.get(i).getCBColumn().isIdx()) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29018, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29018, obj));
                }
                setList.add(rowCache.get(i));
            }
            
            buffer.append(SPACE);
            appendDocumentID(obj, whereRowCache);
            buffer.append(SPACE);
            
            List<CBColumnData> idxList = new ArrayList<>(dimension);
            for(CBColumnData columnData : whereRowCache) {
                if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else if (!columnData.getCBColumn().isPK()) {
                    throw new AssertionError();
                }
            }

            String setKey = buildNestedArrayIdx(setAttr, dimension, idxList, obj) + LSBRACE + idxList.get(idxList.size() - 1).getValue() + RSBRACE;
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
                buffer.append(getValueString(setList.get(0).getColumnType(), setList.get(0).getValue()));
            }
            
        } else {
            appendClauses(whereRowCache, conditions, obj.getChanges());
        }
        
        appendRetuning();
    }
    
    private String getUsesKeyString(List<CBColumnData> rowCache) {
        CBColumnData pk = null;
        for(int i = 0 ; i < rowCache.size() ; i++) {
            if(rowCache.get(i).getCBColumn().isPK()) {
                pk = rowCache.get(i);
                break;
            }
        }
        
        if(pk != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(USE).append(SPACE).append(KEYS).append(SPACE);
            sb.append(getValueString(pk.getColumnType(), pk.getValue()));
            return sb.toString();
        } else {
            return null;
        }
    }
    
    
    private void appendClauses(List<CBColumnData> rowCache, List<Condition> otherConditions, List<SetClause> setClauses) {
        if (rowCache.isEmpty() && otherConditions.isEmpty()) {
            return;
        }

        String useKey = this.getUsesKeyString(rowCache);

        boolean isTypedInProjection = false;

        if (useKey != null) {
            buffer.append(SPACE).append(useKey);
            isTypedInProjection = true;
        }
        
        if (setClauses != null) {
            buffer.append(SPACE).append(SET).append(SPACE);
            append(setClauses);
        }
        
        boolean hasPredicate = false;
        for (CBColumnData columnData : rowCache) {
            if(typedName != null && typedName.equals(nameInSource(columnData.getCBColumn().getLeafName()))) {
                isTypedInProjection = true;
            }
            if (columnData.getCBColumn().isPK()) {
                continue;
            }
            
            if (!hasPredicate) {
                buffer.append(SPACE).append(WHERE);
                hasPredicate = true;
            } else {
                buffer.append(SPACE).append(AND);
            }
            
            buffer.append(SPACE).append(columnData.getCBColumn().getNameInSource()).append(SPACE);
            buffer.append(Tokens.EQ).append(SPACE);
            buffer.append(getValueString(columnData.getColumnType(), columnData.getValue()));
        }
        
        if (!otherConditions.isEmpty()) {
            if (!hasPredicate) {
                buffer.append(SPACE).append(WHERE);
                hasPredicate = true;
            } else {
                buffer.append(SPACE).append(AND);
            }
            buffer.append(SPACE);
            append(LanguageUtil.combineCriteria(otherConditions));
        }
        
        if(!isTypedInProjection && typedName != null && typedValue != null) {
            if (hasPredicate) {
                buffer.append(SPACE).append(AND);
            } else {
                buffer.append(SPACE).append(WHERE);
            }
            buffer.append(SPACE).append(keyspace).append(SOURCE_SEPARATOR).append(typedName).append(SPACE).append(EQ).append(SPACE).append(typedValue);
        }
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
