package org.teiid.query.sql.symbol;

import java.util.ArrayList;
import java.util.List;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.source.XMLHelper;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.query.xquery.XQueryExpression;

public class XMLQuery implements Expression {

    private XMLNamespaces namespaces;
    private String xquery;
    private List<DerivedColumn> passing = new ArrayList<DerivedColumn>();
    private Boolean emptyOnEmpty;

    private XQueryExpression xqueryExpression;

    @Override
    public Class<?> getType() {
        return DataTypeManager.DefaultDataClasses.XML;
    }

    public Boolean getEmptyOnEmpty() {
        return emptyOnEmpty;
    }

    public void setEmptyOnEmpty(Boolean emptyOnEmpty) {
        this.emptyOnEmpty = emptyOnEmpty;
    }

    public List<DerivedColumn> getPassing() {
        return passing;
    }

    //TODO: display the analysis record info
    public void compileXqueryExpression() throws QueryResolverException {
        this.xqueryExpression = XMLHelper.getInstance().compile(xquery, namespaces, passing, null);
        this.xqueryExpression.useDocumentProjection(null, new AnalysisRecord(false, false));
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
    public XMLQuery clone() {
        XMLQuery clone = new XMLQuery();
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
        clone.emptyOnEmpty = this.emptyOnEmpty;
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLQuery)) {
            return false;
        }
        XMLQuery other = (XMLQuery)obj;
        return EquivalenceUtil.areEqual(this.namespaces, other.namespaces)
              && this.passing.equals(other.passing)
              && this.xquery.equals(other.xquery)
              && EquivalenceUtil.areEqual(this.emptyOnEmpty, other.emptyOnEmpty);
    }

    @Override
    public int hashCode() {
        return this.xquery.hashCode();
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
