package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.function.source.XMLHelper;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.xquery.XQueryExpression;

public class XMLTable extends TableFunctionReference {

    public static class XMLColumn extends ProjectedColumn {
        private boolean ordinal;
        private String path;
        private Expression defaultExpression;

        public XMLColumn(String name) {
            super(name, DataTypeManager.DefaultDataTypes.INTEGER);
            this.ordinal = true;
        }

        public XMLColumn(String name, String type, String path, Expression defaultExpression) {
            super(name, type);
            this.path = path;
            this.defaultExpression = defaultExpression;
        }

        protected XMLColumn() {

        }

        public Expression getDefaultExpression() {
            return defaultExpression;
        }

        public void setDefaultExpression(Expression defaultExpression) {
            this.defaultExpression = defaultExpression;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public boolean isOrdinal() {
            return ordinal;
        }

        public void setOrdinal(boolean ordinal) {
            this.ordinal = ordinal;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof XMLColumn)) {
                return false;
            }
            XMLColumn other = (XMLColumn)obj;
            return this.ordinal == other.ordinal
                && EquivalenceUtil.areEqual(this.path, other.path)
                && EquivalenceUtil.areEqual(this.defaultExpression, other.defaultExpression);
        }

        @Override
        public XMLColumn clone() {
            XMLColumn clone = new XMLColumn();
            super.copyTo(clone);
            clone.ordinal = this.ordinal;
            clone.path = this.path;
            if (this.defaultExpression != null) {
                clone.defaultExpression = (Expression)this.defaultExpression.clone();
            }
            return clone;
        }
    }

    private List<XMLColumn> columns = new ArrayList<XMLColumn>();
    private XMLNamespaces namespaces;
    private String xquery;
    private List<DerivedColumn> passing = new ArrayList<DerivedColumn>();
    private boolean usingDefaultColumn;

    private XQueryExpression xqueryExpression;

    public List<DerivedColumn> getPassing() {
        return passing;
    }

    public void compileXqueryExpression() throws TeiidProcessingException {
        this.xqueryExpression = XMLHelper.getInstance().compile(xquery, namespaces, passing, this.columns);
    }

    public XQueryExpression getXQueryExpression() {
        return xqueryExpression;
    }

    public void setPassing(List<DerivedColumn> passing) {
        this.passing = passing;
    }

    public String getXquery() {
        return xquery;
    }

    public void setXquery(String xquery) {
        this.xquery = xquery;
    }

    public List<XMLColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<XMLColumn> columns) {
        if (columns.isEmpty()) {
            usingDefaultColumn = true;
            columns.add(new XMLColumn("OBJECT_VALUE", DataTypeManager.DefaultDataTypes.XML, ".", null)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        this.columns = columns;
    }

    public boolean isUsingDefaultColumn() {
        return usingDefaultColumn;
    }

    public XMLNamespaces getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(XMLNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected XMLTable cloneDirect() {
        XMLTable clone = new XMLTable();
        this.copy(clone);
        for (XMLColumn column : columns) {
            clone.getColumns().add(column.clone());
        }
        if (this.namespaces != null) {
            clone.namespaces = this.namespaces.clone();
        }
        if (this.passing != null) {
            for (DerivedColumn col : this.passing) {
                clone.passing.add(col.clone());
            }
        }
        clone.xquery = this.xquery;
        if (this.xqueryExpression != null) {
            clone.xqueryExpression = this.xqueryExpression.clone();
        }
        clone.usingDefaultColumn = usingDefaultColumn;
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof XMLTable)) {
            return false;
        }
        XMLTable other = (XMLTable)obj;
        return this.columns.equals(other.columns)
            && EquivalenceUtil.areEqual(this.namespaces, other.namespaces)
            && this.xquery.equals(other.xquery)
            && this.passing.equals(other.passing);
    }

}
