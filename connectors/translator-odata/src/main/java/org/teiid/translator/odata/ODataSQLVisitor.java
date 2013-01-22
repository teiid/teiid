/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.odata;

import static org.teiid.language.SQLConstants.Reserved.DESC;
import static org.teiid.language.SQLConstants.Reserved.NOT;
import static org.teiid.language.SQLConstants.Reserved.NULL;

import java.net.URI;
import java.util.*;

import javax.ws.rs.core.UriBuilder;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ODataSQLVisitor extends HierarchyVisitor {
    private static Map<String, String> infixFunctions = new HashMap<String, String>();
    static {
    	infixFunctions.put("%", "mod");//$NON-NLS-1$ //$NON-NLS-2$
    	infixFunctions.put("+", "add");//$NON-NLS-1$ //$NON-NLS-2$
    	infixFunctions.put("-", "sub");//$NON-NLS-1$ //$NON-NLS-2$
    	infixFunctions.put("*", "mul");//$NON-NLS-1$ //$NON-NLS-2$
    	infixFunctions.put("/", "div");//$NON-NLS-1$ //$NON-NLS-2$
    }
	
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	protected QueryExpression command;
	protected ODataExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	protected TreeSet<Column> selectColumns = new TreeSet<Column>();
	protected StringBuilder filter = new StringBuilder();
	private Map<Expression, Boolean> ignoreExpression = new HashMap<Expression, Boolean>();
	private EntitiesInQuery entities = new EntitiesInQuery();
	private Integer skip;
	private Integer top;
	private StringBuilder orderBy = new StringBuilder();
	private boolean count = false;
	
	public Column[] getSelect(){
		return this.selectColumns.toArray(new Column[this.selectColumns.size()]);
	}
	
	public boolean isCount() {
		return this.count;
	}
	
	public boolean isKeyLookup() {
		return this.entities.isKeyLookup();
	}
	
	public Table getEnityTable() {
		return this.entities.getFinalEntity();
	}
	
	public String buildURL() {
    	StringBuilder url = new StringBuilder();
    	this.entities.append(url);
    	if (this.count) {
    		url.append("/$count"); //$NON-NLS-1$
    	}
    	UriBuilder uriBuilder = UriBuilder.fromPath(url.toString());    	
    	
    	if (this.filter.length() > 0) {
    		uriBuilder.queryParam("$filter", this.filter.toString()); //$NON-NLS-1$
    	}
    	
    	if (this.orderBy.length() > 0) {
    		uriBuilder.queryParam("$orderby", this.orderBy.toString()); //$NON-NLS-1$
    	}
    	
    	if (!this.selectColumns.isEmpty()) {
    		HashSet<String> select = new HashSet<String>();
    		for (Column column:this.selectColumns) {
    			select.add(getColumnName(column));
    		}
    		StringBuilder sb = new StringBuilder();
    		Iterator<String> it = select.iterator();
    		while(it.hasNext()) {
    			sb.append(it.next());
    			if (it.hasNext()) {
    				sb.append(Tokens.COMMA);
    			}
    		}
    		uriBuilder.queryParam("$select", sb.toString()); //$NON-NLS-1$
    	}
    	if (this.skip != null) {
    		uriBuilder.queryParam("$skip", this.skip); //$NON-NLS-1$
    	}
    	if (this.top != null) {
    		uriBuilder.queryParam("$top", this.top); //$NON-NLS-1$
    	}
    	//if (!this.count) {
    	//	uriBuilder.queryParam("$format", "atom"); //$NON-NLS-1$ //$NON-NLS-2$
    	//}
    	
    	URI uri = uriBuilder.build();    	
        return uri.toString();		
	}

	public ODataSQLVisitor(ODataExecutionFactory executionFactory,
			RuntimeMetadata metadata) {
		this.executionFactory = executionFactory;
		this.metadata = metadata;
	}

	@Override
    public void visit(Comparison obj) {
		Boolean ignore = this.ignoreExpression.get(obj);
		if (ignore != null && ignore) {
			return;
		}
		
        append(obj.getLeftExpression());
        filter.append(Tokens.SPACE);
        switch(obj.getOperator()) {
        case EQ:
        	filter.append("eq"); //$NON-NLS-1$
        	break;
        case NE:
        	filter.append("ne"); //$NON-NLS-1$
        	break;
        case LT:
        	filter.append("lt"); //$NON-NLS-1$
        	break;
        case LE:
        	filter.append("le"); //$NON-NLS-1$
        	break;
        case GT:
        	filter.append("gt"); //$NON-NLS-1$
        	break;
        case GE:
        	filter.append("ge"); //$NON-NLS-1$
        	break;
        }
        filter.append(Tokens.SPACE);
        appendRightComparison(obj);
    }

	protected void appendRightComparison(Comparison obj) {
		append(obj.getRightExpression());
	}
	
	@Override
    public void visit(AndOr obj) {
        String opString = obj.getOperator().toString();
        Boolean ignore = this.ignoreExpression.get(obj);
        appendNestedCondition(obj, obj.getLeftCondition());
        if (ignore == null || !ignore) {
		    filter.append(Tokens.SPACE)
		          .append(opString)
		          .append(Tokens.SPACE);
        }
        appendNestedCondition(obj, obj.getRightCondition());
    }
    
    protected void appendNestedCondition(AndOr parent, Condition condition) {
    	if (condition instanceof AndOr) {
    		AndOr nested = (AndOr)condition;
    		if (nested.getOperator() != parent.getOperator()) {
    			filter.append(Tokens.LPAREN);
    			append(condition);
    			filter.append(Tokens.RPAREN);
    			return;
    		}
    	}
    	append(condition);
    }
    
	@Override
    public void visit(Delete obj) {
		
	}
	
	@Override
    public void visit(ColumnReference obj) {
		filter.append(obj.getMetadataObject().getName());
	}
	
    protected boolean isInfixFunction(String function) {
    	return infixFunctions.containsKey(function);
    }

	@Override
    public void visit(Function obj) {
        String name = obj.getMetadataObject().getName();
        List<Expression> args = obj.getParameters();
        if(isInfixFunction(name)) { 
            filter.append(Tokens.LPAREN); 
            if(args != null) {
                for(int i=0; i<args.size(); i++) {
                    append(args.get(i));
                    if(i < (args.size()-1)) {
                        filter.append(Tokens.SPACE);
                        filter.append(infixFunctions.get(name));
                        filter.append(Tokens.SPACE);
                    }
                }
            }
            filter.append(Tokens.RPAREN);
        } 
        else {
        	FunctionMethod method = obj.getMetadataObject();
        	if (name.startsWith(method.getCategory())) {
        		name = name.substring(method.getCategory().length()+1);
        	}
            filter.append(name)
                  .append(Tokens.LPAREN);
            if (args != null && args.size() != 0) {
                for (int i = 0; i < args.size(); i++) {
                    append(args.get(i));
                    if (i < args.size()-1) {
                    	filter.append(Tokens.COMMA);
                    }
                }
            }
            filter.append(Tokens.RPAREN);
        }		
	}
	
	@Override
    public void visit(NamedTable obj) {
		this.entities.addEntity(obj.getMetadataObject());
	}
	
	@Override
    public void visit(IsNull obj) {
        if (obj.isNegated()) {
            filter.append(NOT).append(Tokens.LPAREN);
        }
    	appendNested(obj.getExpression());
        filter.append(Tokens.SPACE);
        filter.append("eq").append(Tokens.SPACE); //$NON-NLS-1$
        filter.append(NULL);
        if (obj.isNegated()) {
            filter.append(Tokens.RPAREN);
        }
	}
	
	private void appendNested(Expression ex) {
		boolean useParens = ex instanceof Condition;
    	if (useParens) {
    		filter.append(Tokens.LPAREN);
    	}
        append(ex);
        if (useParens) {
        	filter.append(Tokens.RPAREN);
        }
	}	

	@Override
    public void visit(Join obj) {
		// joins are not used currently
		if (obj.getLeftItem() instanceof NamedTable && obj.getRightItem() instanceof NamedTable) {
			this.entities.addEntity(((NamedTable)obj.getLeftItem()).getMetadataObject());
			this.entities.addEntity(((NamedTable)obj.getRightItem()).getMetadataObject());
			buildEntityKey(obj.getCondition());
			visitNode(obj.getCondition());
		}
		else {
	        visitNode(obj.getLeftItem());
	        visitNode(obj.getRightItem());
	        visitNode(obj.getCondition());
		}
	}
	
	@Override
    public void visit(Limit obj) {
		if (obj.getRowOffset() != 0) {
			this.skip = new Integer(obj.getRowOffset());
		}
		if (obj.getRowLimit() != 0) {
			this.top = new Integer(obj.getRowLimit());
		}
	}
	
	@Override
    public void visit(Literal obj) {
    	appendLiteral(obj, this.filter);		
	}

	private void appendLiteral(Literal obj, StringBuilder sb) {
		if (obj.getValue() == null) {
            sb.append(NULL);
        } else {
            Class<?> type = obj.getType();
            String val = obj.getValue().toString();
            if(Number.class.isAssignableFrom(type)) {
                sb.append(val);
            } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            	sb.append(obj.getValue().equals(Boolean.TRUE) ? true : false);
            } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                sb.append("datetime'") //$NON-NLS-1$
                      .append(val)
                      .append("'"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                sb.append("time'") //$NON-NLS-1$
                      .append(val)
                      .append("'"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                sb.append("date'") //$NON-NLS-1$
                      .append(val)
                      .append("'"); //$NON-NLS-1$
            } else if (type.equals(DataTypeManager.DefaultDataClasses.VARBINARY)) {
            	sb.append("X'") //$NON-NLS-1$
            		  .append(val)
            		  .append("'"); //$NON-NLS-1$
            } else {
                sb.append(Tokens.QUOTE)
                      .append(escapeString(val, Tokens.QUOTE))
                      .append(Tokens.QUOTE);
            }
        }
	}
	
    protected String escapeString(String str, String quote) {
        return StringUtil.replaceAll(str, quote, quote + quote);
    }
	
	@Override
    public void visit(Not obj) {
        filter.append(NOT)
        .append(Tokens.SPACE)
        .append(Tokens.LPAREN);
        append(obj.getCriteria());
        filter.append(Tokens.RPAREN);
	}

	@Override
    public void visit(OrderBy obj) {
		 append(obj.getSortSpecifications());
	}

	@Override
    public void visit(SortSpecification obj) {
		if (orderBy.length() > 0) {
			orderBy.append(Tokens.COMMA);
		}
		ColumnReference column = (ColumnReference)obj.getExpression();
		orderBy.append(column.getMetadataObject().getName());
		// default is ascending
        if (obj.getOrdering() == Ordering.DESC) {
        	orderBy.append(Tokens.SPACE).append(DESC);
        } 
	}
	
	@Override
    public void visit(Select obj) {
        visitNodes(obj.getFrom());
        buildEntityKey(obj.getWhere());
        visitNode(obj.getWhere());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNodes(obj.getDerivedColumns());
	}


	private void buildEntityKey(Condition obj) {
		Collection<AndOr> andOrObjs = CollectorVisitor.shallowCollectObjects(AndOr.class, obj);
        if (!andOrObjs.isEmpty()) {
        	ArrayList<AndOr> ands = new ArrayList<AndOr>();
        	
        	for (AndOr andOr:andOrObjs) {
        		deepCollectAndOrs(andOr, ands);
        	}
        	
        	for(AndOr and:ands) {
        		if (and.getLeftCondition() instanceof Comparison) {
        			Comparison left = (Comparison) and.getLeftCondition();
        			boolean leftAdded = this.entities.addEnityKey(left);
        			this.ignoreExpression.put(left, Boolean.valueOf(leftAdded));
        			if (leftAdded) {
        				this.ignoreExpression.put(and, Boolean.TRUE);
        			}
        		}
        		if (and.getRightCondition() instanceof Comparison) {
        			Comparison right = (Comparison) and.getRightCondition();
        			boolean rightAdded = this.entities.addEnityKey(right);
        			this.ignoreExpression.put(right, Boolean.valueOf(rightAdded));
        			if (rightAdded) {
        				this.ignoreExpression.put(and, Boolean.TRUE);
        			}
        		}        		
        	}
        }
        else {
        	Collection<Comparison> compares = CollectorVisitor.collectObjects(Comparison.class, obj);
			for (Comparison c:compares) {
				this.ignoreExpression.put(c, Boolean.valueOf(this.entities.addEnityKey(c)));
			}
        }
        
        if (!this.entities.valid()) {
        	this.ignoreExpression.clear();
        }
	}
	
	private void deepCollectAndOrs(AndOr andOr, ArrayList<AndOr> ands) {
		if (andOr.getOperator().equals(AndOr.Operator.AND)) {
			ands.add(andOr);
		    
			Condition condition = andOr.getLeftCondition();
	    	if (condition instanceof AndOr) {
	    		AndOr nested = (AndOr)condition;
	    		deepCollectAndOrs(nested, ands);
	    	}
	    	
		    condition = andOr.getRightCondition();
	    	if (condition instanceof AndOr) {
	    		AndOr nested = (AndOr)condition;
	    		deepCollectAndOrs(nested, ands);
	    	}	    	
		}
	}
	
	@Override
    public void visit(DerivedColumn obj) {
		if (obj.getExpression() instanceof ColumnReference) {
			Column column = ((ColumnReference)obj.getExpression()).getMetadataObject();
			String joinColumn = column.getProperty(ODataMetadataProcessor.JOIN_COLUMN, false);
			if (joinColumn != null && Boolean.valueOf(joinColumn)) {
				this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17006, column.getName())));
			}
			this.selectColumns.add(column);
		}
		else if (obj.getExpression() instanceof AggregateFunction) {
			AggregateFunction func = (AggregateFunction)obj.getExpression();
			if (func.getName().equalsIgnoreCase("COUNT")) { //$NON-NLS-1$
				this.count = true;
			}
			else {
				this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17007, func.getName())));
			}
		}
		else {
			this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17008)));
		}
	}

	private String getColumnName(Column column) {
		String columnName = column.getName();
		// Check if this is a embedded column, if it is then only 
		// add the parent type
		String entityType = column.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
		if (entityType != null) {
			String parentEntityType = column.getParent().getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
			if (!entityType.equals(parentEntityType)) {
				columnName = entityType;
			}
		}
		return columnName;
	}
	
	@Override
    public void visit(Update obj) {
		
	}
	
    public void append(LanguageObject obj) {
    	visitNode(obj);
    }
    
    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            for (int i = 0; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }
    
    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            for (int i = 0; i < items.length; i++) {
                append(items[i]);
            }
        }
    }	
    
    static class Entity {
    	Table table; 
    	Map<Column, Literal> pkValues = new HashMap<Column, Literal>();
    	boolean hasValidKey = false;
    	List<Object[]> relations = new ArrayList<Object[]>();
    	
    	public Entity(Table t) {
    		this.table = t;
    	}
    	
    	public void addKeyValue(Column column, Literal value) {
    		addKeyValue(column, value, true);
    	}
    	
    	private void addKeyValue(Column column, Literal value, boolean walkRelations) {
    		// add in self key
    		this.pkValues.put(column, value);
    		
    		if (walkRelations) {
	    		// See any other relations exist.
	    		for (Object[] relation:relations) {
	    			if (column.equals(relation[0])){
	    				((Entity)relation[2]).addKeyValue((Column)relation[1], value, false);
	    			}
	    		}
    		}
    		
    		for (Column col:this.table.getPrimaryKey().getColumns()) {
	        	if (this.pkValues.get(col) == null) {
	        		return;
	        	}
	        }  
    		this.hasValidKey = true;
    	}
    	
    	public boolean hasValidKey() {
	        return this.hasValidKey;
    	}

		public void addRelation(Column self, Column other, Entity otherEntity) {
			relations.add(new Object[] {self, other, otherEntity});
		}
    }
    
    class EntitiesInQuery {
    	ArrayList<Entity> entities = new ArrayList<ODataSQLVisitor.Entity>();
    	
    	public void append(StringBuilder url) {
    		if (this.entities.size() == 1) {
    			addEntityToURL(url, this.entities.get(0));
    		}
    		else {
    			for (int i = 0; i < this.entities.size()-1; i++) {
    				addEntityToURL(url, this.entities.get(i));
    				url.append("/"); //$NON-NLS-1$
    			}
    			addEntityToURL(url, this.entities.get(this.entities.size()-1));
    		}
    	}
    	
		public boolean isKeyLookup() {
			return this.entities.get(this.entities.size()-1).hasValidKey();
		}

		public Table getFinalEntity() {
			return this.entities.get(this.entities.size()-1).table;
		}

		private void addEntityToURL(StringBuilder url, Entity entity) {
			url.append(entity.table.getName());
        	if (entity.hasValidKey()) {
        		boolean useNames = entity.pkValues.size() > 1; // multi-key PK, use the name value pairs
        		url.append("("); //$NON-NLS-1$
        		boolean firstKey = true;
        		for (Column c:entity.pkValues.keySet()) {
        			if (firstKey) {
        				firstKey = false;
        			}
        			else {
        				url.append(Tokens.COMMA);
        			}
        			if (useNames) {
        				url.append(c.getName()).append(Tokens.EQ);
        			}
        			appendLiteral(entity.pkValues.get(c), url);
        		}
        		url.append(")"); //$NON-NLS-1$
        	}
		}

		public void addEntity(Table table) {
			Entity entity = new Entity(table);
			this.entities.add(entity);
		}
		
		private Entity getEntity(Table table) {
			for (Entity e:this.entities) {
				if (e.table.equals(table)) {
					return e;
				}
			}
			return null;
		}
		
		private boolean addEnityKey(Comparison obj) {
			if (obj.getOperator().equals(Comparison.Operator.EQ)) {
				if (obj.getLeftExpression() instanceof ColumnReference && obj.getRightExpression() instanceof Literal) {
					ColumnReference columnRef = (ColumnReference)obj.getLeftExpression();
					Table parentTable = columnRef.getTable().getMetadataObject();
					Entity entity = getEntity(parentTable);
					if (entity != null) {
						Column column = columnRef.getMetadataObject();
						if (parentTable.getPrimaryKey().getColumnByName(column.getName())!=null) {
							entity.addKeyValue(column, (Literal)obj.getRightExpression());
							return true;							
						}
					}
				}
				if (obj.getLeftExpression() instanceof ColumnReference && obj.getRightExpression() instanceof ColumnReference) {	
					Column left = ((ColumnReference)obj.getLeftExpression()).getMetadataObject();
					Column right = ((ColumnReference)obj.getRightExpression()).getMetadataObject();
					
					if (isJoinOrPkColumn(left)&& isJoinOrPkColumn(right)) {
						// in odata the navigation from parent to child implicit by their keys
						Entity leftEntity = getEntity((Table)left.getParent());
						Entity rightEntity = getEntity((Table)right.getParent());
						leftEntity.addRelation(left, right, rightEntity);
						rightEntity.addRelation(right,left, leftEntity);
						return true;
					}
				}
			}
			return false;
		}		
		
		private boolean isJoinOrPkColumn(Column column) {
			boolean joinColumn = Boolean.valueOf(column.getProperty(ODataMetadataProcessor.JOIN_COLUMN, false));
			if (!joinColumn) {
				Table table = (Table)column.getParent();
				return (table.getPrimaryKey().getColumnByName(column.getName()) != null);
				
			}
			return false;
		}
		
		private boolean valid() {
			for (Entity e:this.entities) {
				if (e.hasValidKey()) {
					return true;
				}
			}
			return false;
		}
    }
}
