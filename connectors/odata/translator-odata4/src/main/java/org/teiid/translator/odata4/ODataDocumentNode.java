package org.teiid.translator.odata4;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.olingo.client.api.uri.QueryOption;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.Table;
import org.teiid.translator.document.DocumentNode;

public class ODataDocumentNode extends DocumentNode {
    enum ODataDocumentType {PRIMARY, COMPLEX, EXPAND};
    private final ODataDocumentType type;
    private LinkedHashSet<String> columns = new LinkedHashSet<String>();
    private String filterStr;

    public ODataDocumentNode(Table t, ODataDocumentType type, boolean collection) {
        super(t, collection);
        this.type = type;
    }

    boolean isComplexType() {
        return type == ODataDocumentType.COMPLEX;
    }

    boolean isExpandType() {
        return type == ODataDocumentType.EXPAND;
    }

    boolean isPrimaryType() {
        return type == ODataDocumentType.PRIMARY;
    }

    void appendSelect(String columnName) {
        if (isComplexType()) {
            this.columns.add(getName());
        } else {
            this.columns.add(columnName);
        }
    }

    Set<String> getSelects(){
        return this.columns;
    }

    Map<QueryOption, Object> getOptions() {
        Map<QueryOption, Object> options = new LinkedHashMap<QueryOption, Object>();

        if (isExpandType()) {
            if (!columns.isEmpty()) {
                options.put(QueryOption.SELECT, StringUtil.join(this.columns, ","));
            }
            if (this.filterStr != null) {
                options.put(QueryOption.FILTER, this.filterStr);
            }
        }
        return options;
    }

    void addFilter(String string) {
        if (this.filterStr == null) {
            this.filterStr = string;
        } else {
            this.filterStr = this.filterStr +" and " + string; //$NON-NLS-1$
        }
    }

    String getFilter() {
        return this.filterStr;
    }
}
