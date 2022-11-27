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
package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.NonReserved.*;
import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.*;
import static org.teiid.translator.couchbase.CouchbaseProperties.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Column;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class N1QLUpdateVisitor extends N1QLVisitor {

    private static final String ESCAPED_QUOTE = "\\u0027"; //$NON-NLS-1$

    private int dimension = 0;

    private String keyspace;
    private String setAttr;
    private String setAttrArray;

    private String[] bulkCommands = null;

    public N1QLUpdateVisitor(CouchbaseExecutionFactory ef) {
        super(ef);
    }

    @Override
    public void visit(Insert obj) {

        visit(obj.getTable());

        List<CBColumnData> rowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();

        Integer typeIndex = null;
        for (int i = 0; i < obj.getColumns().size(); i++) {
            ColumnReference col = obj.getColumns().get(i);
            CBColumn column = formCBColumn(col);
            CBColumnData cacheData = new CBColumnData(col.getType(), column);
            rowCache.add(cacheData);
            if(typedName != null && typedName.equals(nameInSource(cacheData.getCBColumn().getLeafName()))) {
                typeIndex = i;
            }
        }

        Map<Integer, Parameter> preparedValues = new HashMap<Integer, Parameter>();
        ExpressionValueSource evs = (ExpressionValueSource)obj.getValueSource();
        for (int i = 0; i < evs.getValues().size(); i++) {
            Expression exp = evs.getValues().get(i);
            if(exp instanceof Literal) {
                Literal l = (Literal)exp;
                rowCache.get(i).setValue(l.getValue());
                if (typeIndex != null && typeIndex == i && !typedValue.equals(getValueString(l.getType(), l.getValue()))) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29022, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29022, typedValue));
                }
            } else if(exp instanceof Parameter) {
                Parameter p = (Parameter) exp;
                preparedValues.put(i, p);
                if (typeIndex != null && typeIndex == i) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29022, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29022, typedValue));
                }
            } else {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29024, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29024));
            }
        }

        if (typedName != null && typeIndex == null) {
            //add type
            boolean added = false;
            for (Column c : obj.getTable().getMetadataObject().getColumns()) {
                if (!nameInSource(c.getSourceName()).endsWith(typedName)) {
                    continue;
                }
                ColumnReference cr = new ColumnReference(obj.getTable(), c.getName(), c, c.getJavaType());
                CBColumn column = formCBColumn(cr);
                CBColumnData cacheData = new CBColumnData(TypeFacility.RUNTIME_TYPES.STRING, column);
                cacheData.setValue(getRawValue(typedValue));
                rowCache.add(cacheData);
                added = true;
                break;
            }
            if (!added) {
                //TODO: could support this without requiring a type column
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29023, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29023, typedName));
            }
        }

        if(isArrayTable) {
            if(preparedValues.size() > 0) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29017, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29017, obj));
            }

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
            if (obj.isUpsert()) {
                buffer.append(getUpsertKeyword());
            } else {
                buffer.append(getInsertKeyword());;
            }
            buffer.append(SPACE).append(INTO).append(SPACE).append(keyspace).append(SPACE);
            buffer.append(LPAREN).append(KEY).append(COMMA).append(SPACE).append(VALUE).append(RPAREN);

            if(preparedValues.size() > 0) {
                appendBulkValues(preparedValues, rowCache, obj);
                return;
            }

            String documentID = null;
            JsonObject json = JsonObject.create();

            for(int i = 0 ; i < rowCache.size() ; i ++) {

                CBColumnData columnData = rowCache.get(i);

                if(columnData.getCBColumn().isPK()) {
                    Object value = columnData.getValue();
                    if (value != null) {
                        documentID = value.toString();
                    }
                } else {
                    String attr = columnData.getCBColumn().getLeafName();
                    String path = columnData.getCBColumn().getNameInSource();
                    JsonObject nestedObj = findObject(json, path);
                    ef.setValue(nestedObj, attr, columnData.getColumnType(), columnData.getValue());
                }
            }

            if(null == documentID) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29006, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29006, obj));
            }

            buffer.append(SPACE).append(VALUES).append(SPACE).append(LPAREN).append(SQLConstants.Tokens.QUOTE).append(escapeString(documentID, SQLConstants.Tokens.QUOTE)).append(SQLConstants.Tokens.QUOTE);
            buffer.append(COMMA).append(SPACE).append(json).append(RPAREN);
        }

        appendRetuning();
    }

    private void appendBulkValues(Map<Integer, Parameter> preparedValues, List<CBColumnData> rowCache, Insert command) {

        BatchedCommand batchCommand = command;
        Iterator<? extends List<?>> vi = batchCommand.getParameterValues();

        int maxBulkSize = ef.getMaxBulkInsertSize();
        int cursor = 0;
        List<String> n1qlList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        boolean comma = false;
        while(vi != null && vi.hasNext()) {

            if(cursor == 0) {
                sb.append(buffer);
            }

            cursor ++;
            List<?> row = vi.next();
            String documentID = null;
            JsonObject json = JsonObject.create();
            for(int i = 0 ; i < rowCache.size() ; i ++) {
                CBColumnData columnData = rowCache.get(i);
                Parameter p = preparedValues.get(i);
                Object value = null;
                if (p != null) {
                    value = row.get(p.getValueIndex());
                } else {
                    value = columnData.getValue();
                }
                if(columnData.getCBColumn().isPK()) {
                    if (value != null) {
                        documentID = value.toString();
                    }
                } else {
                    String attr = columnData.getCBColumn().getLeafName();
                    String path = columnData.getCBColumn().getNameInSource();
                    JsonObject nestedObj = findObject(json, path);
                    ef.setValue(nestedObj, attr, columnData.getColumnType(), value);
                }
            }

            if(null == documentID) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29006, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29006, command));
            }

            if(comma) {
                sb.append(COMMA);
            } else {
                comma = true;
            }
            sb.append(SPACE).append(VALUES).append(SPACE).append(LPAREN).append(SQLConstants.Tokens.QUOTE).append(escapeString(documentID, SQLConstants.Tokens.QUOTE)).append(SQLConstants.Tokens.QUOTE);
            sb.append(COMMA).append(SPACE).append(json).append(RPAREN);

            if(cursor == maxBulkSize) {
                sb.append(SPACE).append(RETURNING).append(SPACE).append(buildMeta(this.keyspace)).append(SPACE);
                sb.append(AS).append(SPACE).append(PK);
                n1qlList.add(sb.toString());
                cursor = 0;
                sb.delete(0, sb.length());
                comma = false;
            }
        }

        if(cursor > 0 && cursor < maxBulkSize) {
            sb.append(SPACE).append(RETURNING).append(SPACE).append(buildMeta(this.keyspace)).append(SPACE);
            sb.append(AS).append(SPACE).append(PK);
            n1qlList.add(sb.toString());
        }

        this.bulkCommands = n1qlList.toArray(new String[n1qlList.size()]);
    }

    public String[] getBulkCommands() {
        return bulkCommands;
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

        String tableNameInSource = obj.getMetadataObject().getSourceName();

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
            if(isArrayTable) {
                String arrayRef = buildNestedAttrRef(this.setAttrArray, column);
                if(column.isPK()) {
                    arrayRef = this.buildMeta(this.keyspace);
                }
                buffer.append(arrayRef);
            } else {
                String ref = column.getNameInSource();
                if(column.isPK()) {
                    ref = this.buildMeta(this.keyspace);
                }
                buffer.append(ref);
            }
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
            if(rowCache.size() < (dimension + 1)) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29019, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29019, obj));
            }
            buffer.append(UPDATE).append(SPACE).append(this.keyspace).append(SPACE);

            appendDocumentID(obj, rowCache);

            List<CBColumnData> idxList = new ArrayList<>(dimension);
            List<CBColumnData> equalityWhereList = new ArrayList<>(rowCache.size());
            for(CBColumnData columnData : rowCache) {
                if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else if (columnData.getCBColumn().isPK()) {
                    continue;
                } else {
                    equalityWhereList.add(columnData);
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

            this.setAttrArray = setKey + LSBRACE + idx + RSBRACE;
            boolean hasPredicate = false;
            for(CBColumnData columnData : equalityWhereList) {
                if (!hasPredicate) {
                    buffer.append(SPACE).append(WHERE);
                    hasPredicate = true;
                } else {
                    buffer.append(SPACE).append(AND);
                }

                String whereRef = buildNestedAttrRef(setAttrArray, columnData.getCBColumn());
                buffer.append(SPACE).append(whereRef).append(SPACE).append(EQ).append(SPACE).append(getValueString(columnData.getColumnType(), columnData.getValue()));
            }

            for(Condition condition : conditions) {
                if (!hasPredicate) {
                    buffer.append(SPACE).append(WHERE).append(SPACE);
                    hasPredicate = true;
                } else {
                    buffer.append(SPACE).append(AND).append(SPACE);
                }
                append(condition);
            }

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

        if(value instanceof LanguageObject) {
            visitNode((LanguageObject)value);
            return EMPTY_STRING;
        }

        if(type.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            return SQLConstants.Tokens.QUOTE + StringUtil.replace(value.toString(), SQLConstants.Tokens.QUOTE, ESCAPED_QUOTE) + SQLConstants.Tokens.QUOTE;
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.INTEGER) || type.equals(TypeFacility.RUNTIME_TYPES.LONG)
                || type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE) || type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)
                || type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN) || type.equals(TypeFacility.RUNTIME_TYPES.BIG_INTEGER)
                || type.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            return String.valueOf(value);
        }
        throw new AssertionError("Unknown literal type: " + type); //$NON-NLS-1$
    }

    private String getRawValue(String value) {
        if (value.startsWith(SQLConstants.Tokens.QUOTE) && value.endsWith(SQLConstants.Tokens.QUOTE)) {
            return StringUtil.replace(value.substring(1, value.length() - 1), ESCAPED_QUOTE, SQLConstants.Tokens.QUOTE);
        }
        return value;
    }

    @Override
    public void visit(Update obj) {

        visit(obj.getTable());
        buffer.append(UPDATE).append(SPACE).append(this.keyspace);

        Condition where = obj.getWhere();

        List<CBColumnData> whereRowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();
        List<Condition> conditions = findEqualityPredicates(where, whereRowCache);

        if(isArrayTable) {
            if(whereRowCache.size() < (dimension + 1)) {
                throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29019, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29019, obj));
            }
            List<CBColumnData> rowCache = new ArrayList<N1QLUpdateVisitor.CBColumnData>();
            for (SetClause clause : obj.getChanges()) {
                ColumnReference col = clause.getSymbol();
                if (isPKColumn(col)) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29025, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29025, typedValue));
                }
                CBColumn column = formCBColumn(col);
                CBColumnData cacheData = new CBColumnData(col.getType(), column);
                Object value = clause.getValue();
                if (value instanceof Literal) {
                    value = ((Literal)value).getValue();
                }
                if(typedName != null && typedName.equals(nameInSource(cacheData.getCBColumn().getLeafName()))) {
                    if (!typedValue.equals(getValueString(cacheData.getColumnType(), value))) {
                        throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29022, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29022, typedValue));
                    }
                    continue;
                }
                cacheData.setValue(value);
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
            List<CBColumnData> equalityWhereList = new ArrayList<>(whereRowCache.size());
            for(CBColumnData columnData : whereRowCache) {
                if(columnData.getCBColumn().isIdx()) {
                    idxList.add(columnData);
                } else if (columnData.getCBColumn().isPK()) {
                    continue;
                } else {
                    equalityWhereList.add(columnData);
                }
            }

            this.setAttrArray = buildNestedArrayIdx(setAttr, dimension, idxList, obj) + LSBRACE + idxList.get(idxList.size() - 1).getValue() + RSBRACE;
            buffer.append(SET);

            boolean comma = false;
            for(CBColumnData columnData : setList) {
                if(comma){
                    buffer.append(COMMA).append(SPACE);
                } else {
                    buffer.append(SPACE);
                    comma = true;
                }

                String setRef = buildNestedAttrRef(setAttrArray, columnData.getCBColumn());
                buffer.append(setRef).append(SPACE).append(EQ).append(SPACE).append(getValueString(columnData.getColumnType(), columnData.getValue()));
            }

            boolean hasPredicate = false;
            for(CBColumnData columnData : equalityWhereList) {
                if (!hasPredicate) {
                    buffer.append(SPACE).append(WHERE);
                    hasPredicate = true;
                } else {
                    buffer.append(SPACE).append(AND);
                }

                String whereRef = buildNestedAttrRef(setAttrArray, columnData.getCBColumn());
                buffer.append(SPACE).append(whereRef).append(SPACE).append(EQ).append(SPACE).append(getValueString(columnData.getColumnType(), columnData.getValue()));
            }

            for(Condition condition : conditions) {
                if (!hasPredicate) {
                    buffer.append(SPACE).append(WHERE).append(SPACE);
                    hasPredicate = true;
                } else {
                    buffer.append(SPACE).append(AND).append(SPACE);
                }
                append(condition);
            }

        } else {
            appendClauses(whereRowCache, conditions, obj.getChanges());
        }

        appendRetuning();
    }

    /**
     * Use to build the update/delete reference of nested JSON Object in an array object.
     * @param prefix
     * @param column
     * @return
     */
    private String buildNestedAttrRef(String prefix, CBColumn column) {
        if(column.hasLeaf()){
            String sourceName = column.getNameInSource();
            return prefix + sourceName.substring(sourceName.lastIndexOf(SQUARE_BRACKETS) + SQUARE_BRACKETS.length());
        } else {
            return prefix;
        }
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
        String useKey = this.getUsesKeyString(rowCache);

        boolean isTyped = false;

        if (useKey != null) {
            buffer.append(SPACE).append(useKey);
        }

        if (setClauses != null) {
            buffer.append(SPACE).append(SET).append(SPACE);
            for (SetClause clause : setClauses) {
                ColumnReference col = clause.getSymbol();
                if (isPKColumn(col)) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29025, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29025, typedValue));
                }
                CBColumn column = formCBColumn(col);
                CBColumnData cacheData = new CBColumnData(col.getType(), column);
                Object value = clause.getValue();
                if (clause.getValue() instanceof Literal) {
                    value = ((Literal)clause.getValue()).getValue();
                }
                if(typedName != null && typedName.equals(nameInSource(cacheData.getCBColumn().getLeafName()))
                        && (value == null || !typedValue.equals(getValueString(cacheData.getColumnType(), value)))) {
                    throw new TeiidRuntimeException(CouchbasePlugin.Event.TEIID29022, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29022, typedValue));
                }
            }
            append(setClauses);
        }

        boolean hasPredicate = false;
        for (CBColumnData columnData : rowCache) {
            if(typedName != null && typedName.equals(nameInSource(columnData.getCBColumn().getLeafName()))) {
                if (typedValue.equals(getValueString(columnData.getColumnType(), columnData.getValue()))) {
                    isTyped = true;
                }
                //else no rows affected
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

        boolean hasTypedValue = !isTyped && typedName != null && typedValue != null;
        if (!otherConditions.isEmpty()) {
            if (!hasPredicate) {
                buffer.append(SPACE).append(WHERE);
                hasPredicate = true;
            } else {
                buffer.append(SPACE).append(AND);
            }
            buffer.append(SPACE);

            if(hasTypedValue){
                buffer.append(LPAREN);
                append(LanguageUtil.combineCriteria(otherConditions));
                buffer.append(RPAREN);
            } else {
                append(LanguageUtil.combineCriteria(otherConditions));
            }
        }

        if(hasTypedValue) {
            if (hasPredicate) {
                buffer.append(SPACE).append(AND);
            } else {
                buffer.append(SPACE).append(WHERE);
            }
            buffer.append(SPACE).append(keyspace).append(SOURCE_SEPARATOR).append(typedName).append(SPACE).append(EQ).append(SPACE).append(typedValue);
        }
    }

    private static class CBColumnData {

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
