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

import static org.teiid.language.SQLConstants.Reserved.FALSE;
import static org.teiid.language.SQLConstants.Reserved.NOT;
import static org.teiid.language.SQLConstants.Reserved.NULL;
import static org.teiid.language.SQLConstants.Reserved.TRUE;

import java.util.*;

import javax.swing.text.html.parser.Entity;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
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
	protected List<String> select = new ArrayList<String>();
	protected StringBuilder filter = new StringBuilder();
	private Map<Expression, Boolean> ignoreExpression = new HashMap<Expression, Boolean>();
	private EntitiesInQuery entities = new EntitiesInQuery();
	
	
	public static String getODataString(Command obj, ODataExecutionFactory executionFactory, RuntimeMetadata metadata)  throws TranslatorException {
		ODataSQLVisitor visitor = new ODataSQLVisitor(executionFactory, metadata);
        
    	visitor.visitNode(obj);
    	
    	if (!visitor.exceptions.isEmpty()) {
    		throw visitor.exceptions.get(0);
    	}  
    	return visitor.buildURL();

    }
	
	private String buildURL() {
    	boolean first = true;
    	StringBuilder url = new StringBuilder();
    	
    	this.entities.append(url);
    	
    	if (this.filter.length() > 0) {
    		first = addSeparator(first, url);
    		url.append("$filter=").append(this.filter); //$NON-NLS-1$
    	}
    	
    	if (!this.select.isEmpty()) {
    		first = addSeparator(first, url);
    		url.append("$select="); //$NON-NLS-1$ 
    		for (int i = 0; i < this.select.size()-1; i++) {
    			url.append(this.select.get(i)).append(","); //$NON-NLS-1$
    			
    		}
    		url.append(this.select.get(this.select.size()-1));
    	}
        return url.toString();		
	}

	private static boolean addSeparator(boolean first, StringBuilder url) {
		if (first) {
			url.append("?"); //$NON-NLS-1$
		}
		else {
			url.append("&"); //$NON-NLS-1$
		}
		return false;
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
		filter.append(obj.getName());
	}
	
	@Override
    public void visit(Call obj) {
		
	}
	
    protected boolean isInfixFunction(String function) {
    	return infixFunctions.containsKey(function);
    }

	@Override
    public void visit(Function obj) {
        String name = obj.getName();
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
            filter.append(obj.getName())
                  .append(Tokens.LPAREN);
            append(args);
            filter.append(Tokens.RPAREN);
        }		
	}
	
	@Override
    public void visit(NamedTable obj) {
		this.entities.addEntity(obj.getMetadataObject());
	}

	@Override
    public void visit(Insert obj) {
		
	}
	@Override
    public void visit(IsNull obj) {
    	appendNested(obj.getExpression());
        filter.append(Tokens.SPACE);
        if (obj.isNegated()) {
            filter.append(NOT).append(Tokens.LPAREN);
        }
        filter.append(Tokens.EQ).append(Tokens.SPACE);
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
        visitNode(obj.getLeftItem());
        visitNode(obj.getRightItem());
        
        // this should have been covered by the EQUI join support
        //visitNode(obj.getCondition());		
	}
	
	@Override
    public void visit(Limit obj) {
		
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
            	sb.append(obj.getValue().equals(Boolean.TRUE) ? TRUE : FALSE);
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
		
	}

	@Override
    public void visit(SortSpecification obj) {
		
	}
	@Override
    public void visit(Argument obj) {
		
	}
	
	@Override
    public void visit(Select obj) {
    	visitNodes(obj.getDerivedColumns());
    	
        visitNodes(obj.getFrom());
        
        buildEntityKey(obj.getWhere());
        
        visitNode(obj.getWhere());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
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
			ColumnReference column = (ColumnReference)obj.getExpression();
			String joinColumn = column.getMetadataObject().getProperty(ODataMetadataProcessor.JOIN_COLUMN, false);
			if (joinColumn != null && Boolean.valueOf(joinColumn)) {
				this.exceptions.add(new TranslatorException("join_column_not_allowed_in_select"));
			}
			select.add(column.getName());
		}
		else {
			this.exceptions.add(new TranslatorException("expr_not_allowed_in_select"));
		}
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
    	
    	public Entity(Table t) {
    		this.table = t;
    	}
    	
    	public void addKeyValue(Column c, Literal value) {
    		this.pkValues.put(c, value);
    		for (Column column:this.table.getPrimaryKey().getColumns()) {
	        	if (this.pkValues.get(column) == null) {
	        		return;
	        	}
	        }  
    		this.hasValidKey = true;
    	}
    	
    	public boolean hasValidKey() {
	        return this.hasValidKey;
    	}
    }
    
    class EntitiesInQuery {
    	ArrayList<Entity> entities = new ArrayList<ODataSQLVisitor.Entity>();
    	
    	public void append(StringBuilder url) {
    		Entity entity = null;
    		if (this.entities.size() == 1) {
    			entity = this.entities.get(0);
    		}
    		else {
    			
    		}
    		
        	addEntityToURL(url, entity);    		
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
						for (Column c:parentTable.getPrimaryKey().getColumns()) {
							if (c.equals(column)) {
								entity.addKeyValue(c, (Literal)obj.getRightExpression());
								return true;
							}
						}
					}
				}
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
