package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.Arrays;
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
			super.copyTo(clone);
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
    private XMLColumn defaultColumn;
    
    private SaxonXQueryExpression xqueryExpression;
    
    public List<DerivedColumn> getPassing() {
		return passing;
	}
    
    public void compileXqueryExpression() throws TeiidProcessingException {
    	List<XMLColumn> cols = this.columns;
    	if (cols.isEmpty()) {
    		cols = Arrays.asList(defaultColumn);
    	}
    	this.xqueryExpression = new SaxonXQueryExpression(xquery, namespaces, passing, cols);
    }
    
    public SaxonXQueryExpression getXQueryExpression() {
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
    		defaultColumn = new XMLColumn("OBJECT_VALUE", DataTypeManager.DefaultDataTypes.XML, ".", null); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	return Arrays.asList(defaultColumn.getSymbol());
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
		if (defaultColumn != null) {
			clone.defaultColumn = this.defaultColumn;
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

	public void rewriteDefaultColumn() {
		if (this.columns.isEmpty() && defaultColumn != null) {
			this.columns.add(defaultColumn);
			defaultColumn = null;
		}
	}
	
}
