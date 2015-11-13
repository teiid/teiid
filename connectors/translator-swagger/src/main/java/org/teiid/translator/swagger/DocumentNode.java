package org.teiid.translator.swagger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Join.JoinType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;

public class DocumentNode {
    private Table table;
    private boolean collection;
    
    public DocumentNode() {
    }
    
    public DocumentNode(Table t, boolean collection) {
        this.table = t;
        this.collection = collection;
    }
    
   
    
    public Table getTable() {
        return this.table;
    }
    
    public String getName() {
        if (this.table.getNameInSource() != null) {
            return this.table.getNameInSource();
        }
        return this.table.getName();
    }
    
    public boolean isCollection() {
        return this.collection;
    }
    
    public List<String> getIdentityColumns(){
        if (this.table.getPrimaryKey() == null) {
            return Collections.emptyList();
        }
        ArrayList<String> keys = new ArrayList<String>();
        for (Column column:this.table.getPrimaryKey().getColumns()) {
            keys.add(column.getName());
        }
        return keys;
    }


}