package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;

public class TextTable extends FromClause {
	
	public static class TextColumn {
		private String name;
		private String type;
		private Integer width;
		
		public TextColumn(String name, String type, Integer width) {
			this.name = name;
			this.type = type;
			this.width = width;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public String getType() {
			return type;
		}
		
		public void setType(String type) {
			this.type = type;
		}
		
		public Integer getWidth() {
			return width;
		}
		
		public void setWidth(Integer width) {
			this.width = width;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof TextColumn)) {
				return false;
			}
			TextColumn other = (TextColumn)obj;
			return this.name.equalsIgnoreCase(other.name) && this.type.equalsIgnoreCase(other.type)
				&& EquivalenceUtil.areEqual(width, other.width);
		}
		
		public int hashCode() {
			return name.hashCode();
		}
		
		@Override
		public TextColumn clone() {
			return new TextColumn(name, type, width);
		}
	}
	
    private GroupSymbol symbol;
    private List<ElementSymbol> projectedSymbols;
    private SymbolMap correlatedReferences;
    
    private Expression file;
    private List<TextColumn> columns = new ArrayList<TextColumn>();
	private Character delimiter;
	private Character quote;
    private boolean escape;
    private Integer header;
    private Integer skip;
    
    private boolean fixedWidth;
    
    public Character getQuote() {
		return quote;
	}
    
    public void setQuote(Character quote) {
		this.quote = quote;
	}
    
    public SymbolMap getCorrelatedReferences() {
		return correlatedReferences;
	}
    
    public void setCorrelatedReferences(SymbolMap correlatedReferences) {
		this.correlatedReferences = correlatedReferences;
	}
    
    public boolean isEscape() {
		return escape;
	}
    
    public void setEscape(boolean escape) {
		this.escape = escape;
	}
    
    public boolean isFixedWidth() {
		return fixedWidth;
	}
    
    public void setFixedWidth(boolean fixedWidth) {
		this.fixedWidth = fixedWidth;
	}
    
    public List<TextColumn> getColumns() {
		return columns;
	}
    
    public void setColumns(List<TextColumn> columns) {
		this.columns = columns;
	}
    
    public Character getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(Character delimiter) {
		this.delimiter = delimiter;
	}

	public Integer getHeader() {
		return header;
	}

	public void setHeader(Integer header) {
		this.header = header;
	}

	public Integer getSkip() {
		return skip;
	}

	public void setSkip(Integer skip) {
		this.skip = skip;
	}
    
    public Expression getFile() {
		return file;
	}
    
    public void setFile(Expression file) {
		this.file = file;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public TextTable clone() {
		TextTable clone = new TextTable();
		clone.setDelimiter(this.delimiter);
		clone.setFile((Expression)this.file.clone());
		clone.setHeader(this.header);
		clone.setSkip(this.skip);
		clone.setQuote(this.quote);
		clone.escape = this.escape;
		clone.symbol = (GroupSymbol)this.symbol.clone();
		for (TextColumn column : columns) {
			clone.getColumns().add(column.clone());
		}
		if (projectedSymbols != null) {
			clone.projectedSymbols = LanguageObject.Util.deepClone(this.projectedSymbols, ElementSymbol.class);
		}
		if (correlatedReferences != null) {
			clone.correlatedReferences = correlatedReferences.clone();
		}
		clone.fixedWidth = this.fixedWidth;
		return clone;
	}

	@Override
	public void collectGroups(Collection groups) {
		groups.add(getGroupSymbol());
	}
	
    /**
     * Get name of this clause.
     * @return Name of clause
     */
    public String getName() {
        return this.symbol.getName();   
    }
    
    public String getOutputName() {
        return this.symbol.getOutputName();
    }

    /**
     * Get GroupSymbol representing the named subquery 
     * @return GroupSymbol representing the subquery
     */
    public GroupSymbol getGroupSymbol() {
        return this.symbol;    
    }
    
    /** 
     * Reset the alias for this subquery from clause and it's pseudo-GroupSymbol.  
     * WARNING: this will modify the hashCode and equals semantics and will cause this object
     * to be lost if currently in a HashMap or HashSet.
     * @param name New name
     * @since 4.3
     */
    public void setName(String name) {
        this.symbol = new GroupSymbol(name);
    }
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof TextTable)) {
			return false;
		}
		TextTable other = (TextTable)obj;
		return this.columns.equals(other.columns) 
			&& EquivalenceUtil.areEqual(file, other.file)
			&& EquivalenceUtil.areEqual(symbol, other.symbol)
			&& EquivalenceUtil.areEqual(delimiter, other.delimiter)
			&& EquivalenceUtil.areEqual(escape, other.escape)
			&& EquivalenceUtil.areEqual(quote, other.quote)
			&& EquivalenceUtil.areEqual(header, other.header)
			&& EquivalenceUtil.areEqual(skip, other.skip);
			
	}
	
	@Override
	public int hashCode() {
		return this.symbol.hashCode();
	}

	public List<ElementSymbol> getProjectedSymbols() {
		if (projectedSymbols == null) {
			projectedSymbols = new ArrayList<ElementSymbol>(columns.size());
			for (TextColumn column : columns) {
				ElementSymbol elementSymbol = new ElementSymbol(column.getName());
				elementSymbol.setType(DataTypeManager.getDataTypeClass(column.getType()));
				projectedSymbols.add(elementSymbol);
			}
		}
		return projectedSymbols;
	}
	
}
