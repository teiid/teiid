package org.teiid.translator.object;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.core.util.StringUtil;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ObjectVisitor extends HierarchyVisitor  {
	// Columns that are in the select clause
	protected Column[] columns = null;  
	// The columnNameToUse could be either
	// -  name in source
	// -  if its a child column, then it will be  {FK name in source}.{name in source}
	protected String[] columnNamesToUse = null;
	
	// tokenized version of the column name
	protected List<String>[] nameNodes = null;
	protected int[] nodeDepth = null;  // values are zero based, anything greater than zero indicates there children involved
	
	// this is the number of children deep this query is requesting information
	protected int childrenDepth = -1;  // 
	// this is the longest path of method calls to traverse the children
	protected List<String> childrenNodes = null;
	
	protected List<String> exceptionMessages = new ArrayList<String>(2);
	
	protected boolean isRootTableInSelect = false;
	
	private Table rootTable = null;
	
	private Map<String, String> fkNames; // tablename, childNodePath
	
	// key search criteria
    private SearchCriterion criterion;
    // non-key search criteria
    private Map<String, SearchCriterion> filters; // columnName, criteria
    private boolean useFilters;
    
    private RuntimeMetadata metadata;


    public ObjectVisitor(ObjectExecutionFactory factory, RuntimeMetadata metadata) {
    	this.useFilters = factory.isSupportFilters();
    	this.metadata = metadata;
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
	
	public boolean hasFilters() {
		return (useFilters && filters != null && filters.size() > 0);
	}
	
	public Table getRootTable() {
		return this.rootTable;
	}
	
	public String getRootNodeClassName() {
		return this.rootTable.getNameInSource();
	}
	
	public String getRootNodePrimaryKeyColumnName() {
		if (this.rootTable.getPrimaryKey()!= null) {
			return this.rootTable.getPrimaryKey().getColumns().get(0).getName();
		}
		return null;
	}
	
	public boolean isRootTableInFrom() {
		return this.isRootTableInSelect;
	}
	
    public SearchCriterion getCriterion() {
		if (this.criterion == null) {
			this.criterion = new SearchCriterion();
		}
		
		this.criterion.setRootTableInSelect(isRootTableInFrom());

    	return this.criterion;
    }
	
    public Map<String, SearchCriterion> getFilters() {
    	return this.filters;
    }
    
	@Override
	public void visit(Select query) {
		columns = getSelectableColumns(query);
		

		columnNamesToUse = new String[columns.length];
		nameNodes = new ArrayList[columns.length];
		nodeDepth = new int[columns.length];  
		
		this.fkNames = new HashMap(query.getFrom().size());
		
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
		
		
		List <TableReference> tables = query.getFrom();
		for (TableReference t:tables) {
			if(t instanceof NamedTable) {
				Table group = ((NamedTable)t).getMetadataObject();
				if (group.equals(this.rootTable)) {
					this.isRootTableInSelect = true;
				}
			}
		}
		
		if (this.useFilters) {
			this.filters = new HashMap<String, SearchCriterion>(getColumns().length);
		}

		super.visit(query);

		
	}
	
	   public void visit(Comparison obj) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing Comparison criteria."); //$NON-NLS-1$
			Comparison.Operator op = ((Comparison) obj).getOperator();
	       
			Expression lhs = ((Comparison) obj).getLeftExpression();
			Expression rhs = ((Comparison) obj).getRightExpression();
			
			// comparison between the ojbects is not usable, because the nameInSource and its parent(s) 
			// will be how the child objects are obtained
			if ((lhs instanceof ColumnReference && rhs instanceof ColumnReference) ||
					(lhs instanceof Literal && rhs instanceof Literal)	) {
				return;
			}
			
			String value = null;
			Column mdIDElement = null;
			Literal literal = null;
			if(lhs instanceof ColumnReference) {
				mdIDElement = ((ColumnReference)lhs).getMetadataObject();
				literal = (Literal) rhs;
				value = literal.getValue().toString();	
				
			} else  {
				mdIDElement = ((ColumnReference)rhs).getMetadataObject();
				literal = (Literal) lhs;
				value = literal.getValue().toString();			
			}
			
			if(mdIDElement == null || value == null) {
	            final String msg = ObjectPlugin.Util.getString("ObjectVisitor.missingComparisonExpression"); //$NON-NLS-1$
	            addException(msg); 
			}

			value =  escapeReservedChars(value);
			
			addCompareCriteria(mdIDElement, escapeReservedChars(value), op, literal.getType());
	
	    }
	   
	    public void visit(In obj) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$
//			isNegated = ((In) criteria).isNegated();
				
			Expression lhs = ((In)obj).getLeftExpression();
			
			Column mdIDElement = ((ColumnReference)lhs).getMetadataObject();
			
			List<Expression> rhsList = ((In)obj).getRightExpressions();
	
			Class<?> type = lhs.getType();
			List parms = new ArrayList(rhsList.size());
	        Iterator iter = rhsList.iterator();
	        while(iter.hasNext()) {
	  
	            Expression expr = (Expression) iter.next();
		        if(expr instanceof Literal) {
		            Literal literal = (Literal) expr;
		            
		            parms.add(literal.getValue());
		            
		            type = literal.getType();
		  
		        } else {
		        	this.addException("ObjectVisitor.Unsupported_expression " + expr);
		        }
	            
	        }
	        addInCriteria(mdIDElement, parms, type);
 
	    }  
	    
	    private void addCompareCriteria(Column column, Object value, Operator op, Class<?> type) {
			SearchCriterion sc = new SearchCriterion(column, value, op.toString(), SearchCriterion.Operator.EQUALS, type);
			
			addSearchCriterion(sc);	
	    }
	    
		private void addInCriteria(Column column,
				List<Object> parms, Class<?> type) {
			SearchCriterion sc = new SearchCriterion(
					column, parms, "in", SearchCriterion.Operator.IN, type);
			
			addSearchCriterion( sc);

		}
		
	   private void addSearchCriterion(SearchCriterion searchCriteria) {
	    	// only searching on primary key is part of the criteria sent for cache lookup
	    	// all other criteria will be used to filter the rows
		   assert(searchCriteria.getTableName() != null);
		   assert(getRootTable() != null);
		   assert(searchCriteria.getField() != null);
		   assert(getRootNodePrimaryKeyColumnName() != null);
		   if (searchCriteria.getTableName().equalsIgnoreCase(getRootTable().getName()) &&
	    			searchCriteria.getField().equalsIgnoreCase(getRootNodePrimaryKeyColumnName())) {
	   		
	    		if (this.criterion != null) {
	    			searchCriteria.addOrCondition(this.criterion);
	    		}
	    		
	    		this.criterion = searchCriteria;
	    	} else if (useFilters) {
	    		 		
	    		if (this.filters.containsKey(searchCriteria.getColumn().getFullName())) {
	    			SearchCriterion sc = this.filters.get(searchCriteria.getColumn().getFullName());
	    			sc.addOrCondition(searchCriteria);
	    		} else {
	    			this.filters.put(searchCriteria.getColumn().getFullName(), searchCriteria);
	    		}
	    		
	    	}
	    }		
	      
		protected static String escapeReservedChars(final String expr) {
			StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < expr.length(); i++) {
	            char curChar = expr.charAt(i);
	            switch (curChar) {
	                case '\\':
	                    sb.append("\\5c"); //$NON-NLS-1$
	                    break;
	                case '*':
	                    sb.append("\\2a"); //$NON-NLS-1$
	                    break;
	                case '(':
	                    sb.append("\\28"); //$NON-NLS-1$
	                    break;
	                case ')':
	                    sb.append("\\29"); //$NON-NLS-1$
	                    break;
	                case '\u0000': 
	                    sb.append("\\00"); //$NON-NLS-1$
	                    break;
	                default:
	                    sb.append(curChar);
	            }
	        }
	        return sb.toString();
		}	
		

	private Column[] getSelectableColumns(Select query) {
		int s = query.getDerivedColumns().size();
		Column[] interimColumns =  new Column[s];
		
		Iterator<DerivedColumn> selectSymbolItr = query.getDerivedColumns().iterator();
		int i=0;
		while(selectSymbolItr.hasNext()) {
			Column c = getColumnFromSymbol(selectSymbolItr.next());
			if (!c.isSelectable()) continue;
			
			interimColumns[i] = c;
			++i;
		}
		
		// if all columns are included, then return, no need to rebuild the array
		if (s == i) {
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
	
	private void addException(String message) {
		
		exceptionMessages.add(
				ObjectPlugin.Util
    			.getString(
    					"ObjectProjections.errorProcessingVisitor", new Object[] { message }));

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
		// prepend the nameInSource with the foreign key nameInSource
		//NOTE: the foreign key NIS should be the name of the container method to find the column
		String parentNodeName = null;
		if (p instanceof Table) {
			parentNodeName = getForeignKeyNodeName((Table) p);
		}
		
		return (parentNodeName != null ? parentNodeName + "." : "") + nis;
	}
	
	protected void setRootClassName(Table t) {
		if (this.rootTable != null) return;
		
		if (t.getNameInSource() != null) {
			this.rootTable = t;
		}
	}
	
	  
	protected  String getNameInSourceFromColumn(Column c) {
		String name = c.getNameInSource();
		if(name == null || name.equals("")) {  //$NON-NLS-1$
			return c.getName();
		}
		return name;
	}   
	
	
	protected String getForeignKeyNodeName(Table t) {		
		String fkName = buildForeignKeyName(t);
		if (fkName != null) {
			this.fkNames.put(t.getName(), fkName);
		}
		
		return fkName;
	}
	
	protected  String buildForeignKeyName(Table t) {
		if (t == null) return null;
			
		if (this.fkNames.containsKey(t.getName()) ) {
			return this.fkNames.get(t.getName());
		}

		if (t.getForeignKeys() != null && !t.getForeignKeys().isEmpty()) {
			ForeignKey fk = (ForeignKey)  t.getForeignKeys().get(0);
			String fk_nis = fk.getNameInSource();
			
			if (fk.getPrimaryKey() != null && fk.getPrimaryKey().getParent() != t) {
				String parentNIS = buildForeignKeyName(fk.getPrimaryKey().getParent());
				
				return (parentNIS != null ? parentNIS + "." : "") + fk_nis;
			}
			return fk_nis;
		} else {
			setRootClassName(t);

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
    
    protected void cleanup() {
    	metadata = null;
    	columns = null;  
    	columnNamesToUse = null;
    	nameNodes = null;
    	nodeDepth = null;  
    	childrenNodes = null;
    	exceptionMessages = null;
    	rootTable = null;
	
    }
}
