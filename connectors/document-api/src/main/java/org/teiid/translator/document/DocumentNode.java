package org.teiid.translator.document;

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
    protected DocumentJoinNode joinNode;

    public DocumentNode() {
    }

    public DocumentNode(Table t, boolean collection) {
        this.table = t;
        this.collection = collection;
    }

    public DocumentJoinNode joinWith(JoinType joinType, DocumentNode right) {
        this.joinNode = new DocumentJoinNode(this, joinType, right);
        return this.joinNode;
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

    public List<Map<String, Object>> tuples(Document document) {
        List<Map<String, Object>> joined = new ArrayList<Map<String, Object>>();

        Map<String, Object> row = new LinkedHashMap<String, Object>();
        Map<String, Object> properties = document.getProperties();
        if (properties != null && !properties.isEmpty()) {
            row.putAll(properties);
        }

        if (this.joinNode == null) {
            joined.add(row);
        } else {
            joined = this.joinNode.mergeTuples(Arrays.asList(row), document);
        }
        return joined;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getName());
        if (joinNode != null) {
            sb.append(this.joinNode);
        }
        return sb.toString();
    }
}