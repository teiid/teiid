package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.NonReserved.KEY;
import static org.teiid.language.SQLConstants.NonReserved.RETURNING;
import static org.teiid.language.SQLConstants.NonReserved.USE;
import static org.teiid.language.SQLConstants.Reserved.INTO;
import static org.teiid.language.SQLConstants.Reserved.VALUE;
import static org.teiid.language.SQLConstants.Reserved.VALUES;
import static org.teiid.language.SQLConstants.Reserved.UPDATE;
import static org.teiid.language.SQLConstants.Reserved.AS;
import static org.teiid.language.SQLConstants.Tokens.COMMA;
import static org.teiid.language.SQLConstants.Tokens.SPACE;
import static org.teiid.language.SQLConstants.Tokens.LPAREN;
import static org.teiid.language.SQLConstants.Tokens.RPAREN;
import static org.teiid.language.SQLConstants.Tokens.QUOTE;
import static org.teiid.translator.couchbase.CouchbaseProperties.PK;
import static org.teiid.translator.couchbase.CouchbaseProperties.SOURCE_SEPARATOR;
import static org.teiid.translator.couchbase.CouchbaseProperties.SQUARE_BRACKETS;
import static org.teiid.translator.couchbase.CouchbaseProperties.KEYS;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Pattern;
import java.util.ArrayList;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonObject;

public class N1QLUpdateVisitor extends N1QLVisitor {
    
    private int index = -1;
    private List<CBColumnData> rowCache;
    
    private String keyspace;
    private String arrayAttr;

    public N1QLUpdateVisitor(CouchbaseExecutionFactory ef) {
        super(ef);
    }
    
    @Override
    public void visit(Insert obj) {
        
        append(obj.getTable());
        
        if(isArrayTable) {
            buffer.append(UPDATE).append(SPACE).append(this.keyspace);
            buffer.append(SPACE).append(USE).append(SPACE).append(KEYS);
            
            append(obj.getColumns());
            append(obj.getValueSource());
            
            
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
                    appendAttr(json, columnData);
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
            this.arrayAttr = tableNameInSource.substring(tableNameInSource.indexOf(SOURCE_SEPARATOR) + 1, tableNameInSource.lastIndexOf(SQUARE_BRACKETS));
        } else {
            this.keyspace = tableNameInSource;
        }
    }
    
    private void appendAttr(JsonObject json, CBColumnData columnData ) {
        
        if(isArrayTable) {
            
        } else {
            String attr = columnData.getCBColumn().getLeafName();
            String path = columnData.getCBColumn().getNameInSource();
            JsonObject obj = findObject(json, path);
            setValue(obj, attr, columnData.getColumnType(), columnData.getValue());
        }
    }
    
    private JsonObject findObject(JsonObject json, String path) {
        JsonObject result = json;
        String[] array = path.split(Pattern.quote(SOURCE_SEPARATOR));
        for (int i = 1; i < array.length -1 ; i ++) {
            String interPath = trimWave(array[i]);
            if(result.get(interPath) == null) {
                result.put(interPath, JsonObject.create());
            }
            result = result.getObject(interPath);
        }
        return result;
    }
    
    private void setValue(JsonObject json, String attr, Class<?> type, Object value) {
        
        Object attrValue = ef.retrieveValue(type, value);
        
        if(type.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            json.put(attr, (String)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.INTEGER)) {
            json.put(attr, (Integer)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.LONG)) {
            json.put(attr, (Long)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)) {
            json.put(attr, (Double)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
            json.put(attr, (String)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BIG_INTEGER)) {
            json.put(attr, (BigInteger)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            json.put(attr, (BigDecimal)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.NULL)) {
            json.putNull(attr);
        }
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
