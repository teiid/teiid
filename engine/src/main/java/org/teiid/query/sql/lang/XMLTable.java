package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import net.sf.saxon.sxpath.XPathExpression;

import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression;

public class XMLTable extends TableFunctionReference {
	
	public static class XMLColumn extends ProjectedColumn {
		private boolean ordinal;
		private String path;
		private Expression defaultExpression;
		
		private XPathExpression pathExpression;
		
		public XMLColumn(String name) {
			super(name, DataTypeManager.DefaultDataTypes.STRING);
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
		
		public void setPathExpression(XPathExpression pathExpression) {
			this.pathExpression = pathExpression;
		}
		
		public XPathExpression getPathExpression() {
			return pathExpression;
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
			super.copy(clone);
			clone.ordinal = this.ordinal;
			clone.path = this.path;
			if (this.defaultExpression != null) {
				clone.defaultExpression = (Expression)this.defaultExpression.clone();
			}
			clone.pathExpression = this.pathExpression;
			return clone;
		}
	}
	
    private List<XMLColumn> columns = new ArrayList<XMLColumn>();
    private XMLNamespaces namespaces;
    private String xquery;
    private List<DerivedColumn> passing = new ArrayList<DerivedColumn>();
    private ElementSymbol defaultColumn;
    
    private SaxonXQueryExpression xqueryExpression;
    
    public List<DerivedColumn> getPassing() {
		return passing;
	}
    
    public void compileXqueryExpression() throws TeiidProcessingException {
    	this.xqueryExpression = new SaxonXQueryExpression(xquery, namespaces, passing, columns);
    }
    
    public SaxonXQueryExpression getXqueryExpression() {
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
		this.columns = columns;
	}
    
    public XMLNamespaces getNamespaces() {
		return namespaces;
	}
    
    public void setNamespaces(XMLNamespaces namespaces) {
		this.namespaces = namespaces;
	}
    
    @Override
    public List<ElementSymbol> getProjectedSymbols() {
    	if (!columns.isEmpty()) {
        	return super.getProjectedSymbols();
    	}
    	if (defaultColumn == null) {
    		defaultColumn = new ElementSymbol("COLUMN_VALUE"); //$NON-NLS-1$
    		defaultColumn.setType(DataTypeManager.DefaultDataClasses.XML);
    	}
    	return Arrays.asList(defaultColumn);
    }
    
	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public XMLTable clone() {
		XMLTable clone = new XMLTable();
		for (XMLColumn column : columns) {
			clone.getColumns().add(column.clone());
		}
		if (defaultColumn != null) {
			clone.defaultColumn = this.defaultColumn;
		}
		return clone;
	}

	@Override
	public void collectGroups(Collection groups) {
		groups.add(getGroupSymbol());
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
		return this.columns.equals(other.columns);
	}
	
}
