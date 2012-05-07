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

package org.teiid.translator.object.infinispan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectPlugin;


/**
 * This is an example of extending ObjectVisitor, providing query access to a local Map object cache
 */
public class InfinispanObjectVisitor extends HierarchyVisitor  {
	
    protected TranslatorException exception;
	public String tableName;
	public String columnName;
	public Object value;
	public Class<?> classType;
	public List<Object> parms;
	public Operator op;
	
	public boolean like = false;
	public boolean compare = false;
	public boolean in = false;

    /**
     * 
     */
    public InfinispanObjectVisitor() {
        super();         
    }
    
    

	public List<Object> getKeyCriteria() {
		// TODO Auto-generated method stub
		if (parms != null) return parms;
		
		if (value == null) return Collections.EMPTY_LIST;
		
		List result = new ArrayList(1);
		result.add(value);
		return result;
	}




	public void addCompareCriteria(String tableName,
			String columnName, Object value, Operator op,
			Class<?> type) throws TranslatorException {
		this.tableName = tableName;
		this.columnName = columnName;
		this.op = op;
		this.value = value;
		this.compare = true;
		this.classType = type;
	}
	
	public void addLikeCriteria(String tableName,
			String columnName, Object value)
			throws TranslatorException {
		this.tableName = tableName;
		this.columnName = columnName;
		this.value = value;
		this.like = true;
	}	
	
	public void addInCriteria(String tableName, String columnName,
			List<Object> parms, Class<?> type)
			throws TranslatorException {
		this.tableName = tableName;
		this.columnName = columnName;
		this.parms = parms;
		this.in = true;
		this.classType = type;	
	}	
  
    public TranslatorException getException() {
        return this.exception;
    }
    
	
    public void visit(Comparison obj) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing Comparison criteria."); //$NON-NLS-1$
		try {	
			
			Comparison.Operator op = ((Comparison) obj).getOperator();
	       
			Expression lhs = ((Comparison) obj).getLeftExpression();
			Expression rhs = ((Comparison) obj).getRightExpression();
			
			// comparison between the ojbects is not usable, because the nameInSource and its parent(s) 
			// will be how the child objects are obtained
			if (lhs instanceof ColumnReference && rhs instanceof ColumnReference) {
				return;
			}

	
			String lhsString = getExpressionString(lhs);
			String rhsString = getExpressionString(rhs);
			if(lhsString == null || rhsString == null) {
	            final String msg = ObjectPlugin.Util.getString("ObjectVisitor.missingComparisonExpression"); //$NON-NLS-1$
				exception = new TranslatorException(msg); 
			}

			if (rhs instanceof Literal || lhs instanceof Literal) {
		        if(rhs instanceof Literal) {
		            Literal literal = (Literal) rhs;		            
					String tableName = getTableNameFromColumnObject(lhs);
					addCompareCriteria(tableName, lhsString, literal.getValue(), op, literal.getType());
		            
		        } else {
		            Literal literal = (Literal) lhs;
		            String tableName = getTableNameFromColumnObject(rhs);
		            addCompareCriteria(tableName, rhsString, literal.getValue(), op, literal.getType());

		        	
		        }
			}
		}catch (TranslatorException t) {
			exception = t;
		}
    }

    
    public void visit(Like obj) {
    	
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing LIKE criteria."); //$NON-NLS-1$
//		isNegated = ((Like) criteria).isNegated();
		// Convert LIKE to Equals, where any "%" symbol is replaced with "*".
		try {
			
			Comparison.Operator op = Operator.EQ;
			Expression lhs = ((Like) obj).getLeftExpression();
			Expression rhs = ((Like) obj).getRightExpression();
			
			String tableName = getTableNameFromColumnObject(lhs);
			if (tableName == null) {
				tableName = getTableNameFromColumnObject(rhs);
			}
		
			String lhsString = getExpressionString(lhs);
			String rhsString = getExpressionString(rhs);
			
            addLikeCriteria(tableName, lhsString, rhsString);

		}catch (TranslatorException t) {
			exception = t;
		}
    }

    
    public void visit(In obj) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$
//		isNegated = ((In) criteria).isNegated();
		try {
			
			Expression lhs = ((In)obj).getLeftExpression();
			
			String tableName = getTableNameFromColumnObject(lhs);
			String colName = getExpressionString(lhs);
			
			List<Expression> rhsList = ((In)obj).getRightExpressions();
	
			Class type = lhs.getType();
			List parms = new ArrayList(rhsList.size());
	        Iterator iter = rhsList.iterator();
	        while(iter.hasNext()) {
	  
	            Expression expr = (Expression) iter.next();
	            type = addParmFromExpression(expr, parms);
	            
	        }
	        addInCriteria(tableName, colName, parms, type);
	        
		}catch (TranslatorException t) {
			exception = t;
		}
	        
    }
    
    protected String getTableNameFromColumnObject(Object e)  {
    	Column col = null;
		if(e instanceof ColumnReference) {
			col = ((ColumnReference)e).getMetadataObject();
		} else if (e instanceof Column) {
			col = (Column) e;
		}
			
		Object p = col.getParent();
		if (p instanceof Table) {
			Table t = (Table)p;
			return t.getName();
		}			

		return null;
    	
    }
    
    protected  Class addParmFromExpression(Expression expr, List parms ) {
    	Class type = null;
        if(expr instanceof Literal) {
        	Long longparm = null;
            Literal literal = (Literal) expr;
            
            parms.add(literal);
            
            type = literal.getType();
  
        } else {
            this.exception = new TranslatorException("ObjectVisitor.Unsupported_expression" + expr); //$NON-NLS-1$
        }
        
        return type;
         
    }

	    
	// GHH 20080326 - found that code to fall back on Name if NameInSource
	// was null wasn't working properly, so replaced with tried and true
	// code from another custom connector.
	private  String getExpressionString(Expression e) throws TranslatorException {
		String expressionName = null;
		// GHH 20080326 - changed around the IElement handling here
		// - the rest of this method is unchanged
		if(e instanceof ColumnReference) {
			Column mdIDElement = ((ColumnReference)e).getMetadataObject();
			expressionName = getNameInSourceFromColumn(mdIDElement);
//			expressionName = mdIDElement.getNameInSource();
//			if(expressionName == null || expressionName.equals("")) {  //$NON-NLS-1$
//				expressionName = mdIDElement.getName();
//			}
		} else if(e instanceof Literal) {
//			try {
//				if(((Literal)e).getType().equals(Class.forName(Timestamp.class.getName()))) {
//					LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Found an expression that uses timestamp; converting to LDAP string format."); //$NON-NLS-1$
//					Timestamp ts = (Timestamp)((Literal)e).getValue();
//					Date dt = new Date(ts.getTime());
//					//TODO: Fetch format if provided.
//					SimpleDateFormat sdf = new SimpleDateFormat(LDAPConnectorConstants.ldapTimestampFormat);
//					expressionName = sdf.format(dt);
//					LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Timestamp to stsring is: " + expressionName); //$NON-NLS-1$
//				}
//				else {
//					expressionName = ((Literal)e).getValue().toString();
//				}
				
				expressionName = ((Literal)e).getValue().toString();
//			} catch (ClassNotFoundException cce) {
//	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.timestampClassNotFoundError"); //$NON-NLS-1$
//				throw new TranslatorException(cce, msg); 
//			}
//				
		} else {
			if(e instanceof AggregateFunction) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, "Received IAggregate, but it is not supported. Check capabilities."); //$NON-NLS-1$
			} else if(e instanceof Function) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, "Received IFunction, but it is not supported. Check capabilties."); //$NON-NLS-1$
			} else if(e instanceof ScalarSubquery) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, "Received IScalarSubquery, but it is not supported. Check capabilties."); //$NON-NLS-1$
			} else if (e instanceof SearchedCase) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, "Received ISearchedCaseExpression, but it is not supported. Check capabilties."); //$NON-NLS-1$
			}
            final String msg = ObjectPlugin.Util.getString("ObjectVisitor.unsupportedElementError" , e.toString()); //$NON-NLS-1$
			throw new TranslatorException(msg); 
		}
		expressionName = escapeReservedChars(expressionName);
		return expressionName;
	}
	
	protected  String getNameInSourceFromColumn(Column c) {
		String name = c.getNameInSource();
		if(name == null || name.equals("")) {  //$NON-NLS-1$
			return c.getName();
		}
		return name;
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
	
}
