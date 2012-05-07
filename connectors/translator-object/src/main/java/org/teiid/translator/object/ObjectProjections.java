package org.teiid.translator.object;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ObjectProjections {
	// Columns that are in the select clause
	protected Column[] columns = null;  // columnNameToUse, Column in select
	// The columnNameToUse could be either
	// -  name in source
	// -  if its a child column, then it will be  {FK name in source}.{name in source}
	protected String[] columnNamesToUse = null;
	
	protected List<String>[] nameNodes = null;
	protected int[] nodeDepth = null;  // values are zero based, anything greater than zero indicates there children involved
	
	// this is the number of children deep this query is requesting information
	protected int childrenDepth = -1;  // 
	// this is the path of method calls to traverse the children
	protected List<String> childrenNodes = null;
	
	protected List<String> exceptionMessages = new ArrayList<String>(2);
	
	protected String rootNodeClassName = null;

	
	public ObjectProjections(Select query) {
		parse(query);
	}
	
	public Column[] getColumns() {
		return this.columns;
	}
	
	public String[] getColumnNamesToUse() {
		return this.columnNamesToUse;
	}
	
	public boolean hasChildren() {
		return (childrenDepth > -1);
	}
	
	@SuppressWarnings("unchecked")
	private void parse(Select query) {
		columns = getSelectableColumns(query);
		columnNamesToUse = new String[columns.length];
		nameNodes = new ArrayList[columns.length];
		nodeDepth = new int[columns.length];  
		
		String maxDepthColumnNameToUse = null;
		
		for (int i=0; i<columns.length; ++i) {
			columnNamesToUse[i] = getColumnNameToUse(columns[i]);
			
			nameNodes[i] = StringUtil.getTokens(columnNamesToUse[i], ".");
			nodeDepth[i] = nameNodes[i].size() - 1;  // if one node name, then depth is zero, and incremented from there
			
			// only when there are multiple node names will a container/child be involved
			if (nodeDepth[i] > 0) {
				if (childrenDepth == -1) {
					childrenDepth = nodeDepth[i];
					// strip off the the child node names (excluding the last node, which is the value call)
					maxDepthColumnNameToUse = columnNamesToUse[i].substring(0,  columnNamesToUse[i].lastIndexOf("."));
					childrenNodes = nameNodes[i];
				} else {
					// if the columns are not on the same path, then this is an error,
					// can only support one child path per query
					if (!columnNamesToUse[i].startsWith(maxDepthColumnNameToUse)) {
						addException(maxDepthColumnNameToUse, columnNamesToUse[i], columns[i].getParent().getName());
					}
					
					if ( nodeDepth[i] > childrenDepth) {
						childrenDepth = nodeDepth[i];
						childrenNodes = nameNodes[i];
					}
					
				}
			}
					
		}
		
	}

	private Column[] getSelectableColumns(Select query) {
		Column[] interimColumns =  new Column[query.getDerivedColumns().size()];
		
		Iterator<DerivedColumn> selectSymbolItr = query.getDerivedColumns().iterator();
		int i=0;
		while(selectSymbolItr.hasNext()) {
			Column c = getColumnFromSymbol(selectSymbolItr.next());
			if (!c.isSelectable()) continue;
			
			interimColumns[i] = c;
			++i;
		}
		
		// if all columns are included, then return, no need to rebuild the array
		if (interimColumns.length == i+1) {
			return interimColumns;
		}
		
		Column[] columns =  new Column[i];
		for (int x=0; x<i; ++x) {
			columns[x] = interimColumns[x];
		}
		return columns;
	
	}

	private void addException(String columnNameToUse1,
			String columnNameToUse2, String table) {
		
		exceptionMessages.add(
				ObjectPlugin.Util
    			.getString(
    					"ObjectProjections.unsupportedMultipleContainers", new Object[] { columnNameToUse1, columnNameToUse2, table }));

	}
	
	protected void throwExceptionIfFound() throws TranslatorException {
		if (!exceptionMessages.isEmpty())
			throw new TranslatorException("ObjectProjections Exception: " + exceptionMessages.toString());
	}
	
    public String getColumnNameToUse(Column column) {
    	String nis = getNameInSourceForObjectHierarchy(column);
    	if (nis == null) return column.getName();
    	return nis;
    }
    
	/** 
	 * Method to build the nameInSource nodes to be used to lookup a value, starting with the root object.
	 * If the column is associated with a table that has a foreign key (i.e, is contained within), use the nameInSource 
	 * of the foreign key as a prefix node in the nameInSource.  The process will climb the foreign key hierarchy tree to
	 * combine to make the NameInSource for the column.
	 * Example:  Object hierarchy:   "A" = getLegs => "B" = getTransactions => "C"
	 * The column object would start with "C" and find a foreign key to "B", having nameInSource of "Transactions"
	 *     current nodes in the name are:   Transactions."C"
	 *    
	 * The process then takes object "B" and determines it has a foreign key to "A", its nameInSource is "Legs", which is prefixed on the nodes names.
	 *     current nodes in the name are:   Legs.Transactions."C"
	 *     
	 * The node name structure will allow the object reflection process to traverse the root object to get to "C" by taking the first node, Legs, 
	 * and calling "A".getLegs(), and then processing each object in the collection by calling "B".getTransations().
	 *     
	 * @param e the supplied Element
	 * @return the name
	 */
    // GHH 20080326 - found that code to fall back on Name if NameInSource
	// was null wasn't working properly, so replaced with tried and true
	// code from another custom connector.
	protected String getNameInSourceForObjectHierarchy(Column e) {
		String nis = getNameInSourceFromColumn(e);

		Object p = e.getParent();
		// if the column comes from a table that has a foreign key, then 
		// preprend the nameInSource with the foreign key nameInSource
		//NOTE: the foreign key NIS should be the name of the container method to find the column
		String parentNodeName = null;
		if (p instanceof Table) {
			parentNodeName = getForeignKeyNodeName((Table) p);
		}
		
		return (parentNodeName != null ? parentNodeName + "." : "") + nis;
	}
	
	protected void setRootClassName(Table t) {
		if (this.rootNodeClassName != null) return;
		
		if (t.getNameInSource() != null) {
			this.rootNodeClassName = t.getNameInSource();
		}
	}
	
	  
	protected  String getNameInSourceFromColumn(Column c) {
		String name = c.getNameInSource();
		if(name == null || name.equals("")) {  //$NON-NLS-1$
			return c.getName();
		}
		return name;
	}   
	
	
	protected  String getForeignKeyNodeName(Table t) {
		if (t == null) return null;
		
		setRootClassName(t);
	
		if (t.getForeignKeys() != null && !t.getForeignKeys().isEmpty()) {
			ForeignKey fk = (ForeignKey)  t.getForeignKeys().get(0);
			String fk_nis = fk.getNameInSource();
			
			KeyRecord kr = fk.getPrimaryKey();
			if (kr.getParent() != null) {
				String parentNIS = getForeignKeyNodeName(kr.getParent());
				
				return (parentNIS != null ? parentNIS + "." : "") + fk_nis;
			}
			return fk_nis;
		}
		
		return null;
	}
	
    /**
     * Helper method for getting {@link org.teiid.metadata.Column} from a
     * {@link org.teiid.language.DerivedColumn}.
     * @param symbol Input ISelectSymbol
     * @return Element returned metadata runtime Element
     */
    protected  Column getColumnFromSymbol(DerivedColumn symbol) {
        ColumnReference expr = (ColumnReference) symbol.getExpression();
        return expr.getMetadataObject();
    }
}
