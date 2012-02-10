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

package org.teiid.translator.coherence.visitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.adapter.coherence.CoherenceFilterUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.coherence.CoherencePlugin;

import com.tangosol.util.Filter;

/**
 */
public class CoherenceVisitor extends HierarchyVisitor {

	private String tableName = null;
	protected String[] attributeNames = null;
	protected Class[] attributeTypes = null;
	private RuntimeMetadata metadata;
	private Filter filter = null;

    private TranslatorException exception;

    /**
     * 
     */
    public CoherenceVisitor(RuntimeMetadata metadata) {
        super();        
        this.metadata = metadata;
    }
    
    public Filter getFilter() {
		return filter;

    }
 
    public String getTableName() {
    	return tableName;
    }
    
    public String[] getAttributeNames() {
    	return attributeNames;
    }
    
    public Class[] getAttributeTypes() {
    	return attributeTypes;
    }    
    
    public TranslatorException getException() {
        return this.exception;
    }
    
    public Filter createFilter(String criteria)  throws TranslatorException {
        return CoherenceFilterUtil.createFilter(criteria);
        
    }

    
	public void visit(Select query) {
		super.visit(query);
		
		Iterator<DerivedColumn> selectSymbolItr = query.getDerivedColumns().iterator();
		attributeNames = new String[query.getDerivedColumns().size()];
		attributeTypes = new Class[query.getDerivedColumns().size()];
		
		int i=0;
		while(selectSymbolItr.hasNext()) {
			Column e = getElementFromSymbol(selectSymbolItr.next());
			String attributeName = this.getNameFromElement(e);
			Class attributeClass = e.getJavaType();
			
			attributeNames[i] = attributeName;
			attributeTypes[i] = attributeClass;

			i++;
		}

		
		List<TableReference> tables = query.getFrom();
		TableReference t = tables.get(0);
		if(t instanceof NamedTable) {
			Table group = ((NamedTable)t).getMetadataObject();
			tableName = group.getName();
		}
		
	}
	
	
    public void visit(Comparison obj) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing Comparison criteria."); //$NON-NLS-1$
		try {
			Comparison.Operator op = ((Comparison) obj).getOperator();
	       
			Expression lhs = ((Comparison) obj).getLeftExpression();
			Expression rhs = ((Comparison) obj).getRightExpression();
	
			String lhsString = getExpressionString(lhs);
			String rhsString = getExpressionString(rhs);
			if(lhsString == null || rhsString == null) {
	            final String msg = CoherencePlugin.Util.getString("CoherenceVisitor.missingComparisonExpression"); //$NON-NLS-1$
				exception = new TranslatorException(msg); 
			}

			if (rhs instanceof Literal || lhs instanceof Literal) {
		        if(rhs instanceof Literal) {
		            Literal literal = (Literal) rhs;
		            addCompareCriteria(lhsString, literal.getValue(), op, literal.getType() );
		            
//		            filter = CoherenceFilterUtil.createCompareFilter(lhsString, literal.getValue(), op, literal.getType() );
		            
		        } else {
		            Literal literal = (Literal) lhs;
		            addCompareCriteria(rhsString, literal.getValue(), op, literal.getType() );
//		            filter = CoherenceFilterUtil.createCompareFilter(rhsString, literal.getValue(), op, literal.getType() );
		        	
		        }
			}
		}catch (TranslatorException t) {
			exception = t;
		}
    }
    
    public void addCompareCriteria(String columnname, Object value, Operator op, Class<?> type ) throws TranslatorException {
        filter = CoherenceFilterUtil.createCompareFilter(columnname, value, op, type);
    }
    
    public void visit(Like obj) {
    	
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing LIKE criteria."); //$NON-NLS-1$
//		isNegated = ((Like) criteria).isNegated();
		// Convert LIKE to Equals, where any "%" symbol is replaced with "*".
		try {
			Comparison.Operator op = Operator.EQ;
			Expression lhs = ((Like) obj).getLeftExpression();
			Expression rhs = ((Like) obj).getRightExpression();
		
			String lhsString = getExpressionString(lhs);
			String rhsString = getExpressionString(rhs);
//			rhsString = rhsString.replace("%", "*"); //$NON-NLS-1$ //$NON-NLS-2$
			filter = CoherenceFilterUtil.createFilter(lhsString + " LIKE \'" + rhsString + "\'");		
		}catch (TranslatorException t) {
			exception = t;
		}
    }

    
    public void visit(In obj) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$
//		isNegated = ((In) criteria).isNegated();
		try {
			Expression lhs = ((In)obj).getLeftExpression();
			String lhsString = getExpressionString(lhs);
			
			List<Expression> rhsList = ((In)obj).getRightExpressions();
	
			Class type = null;
			List parms = new ArrayList(rhsList.size());
	        Iterator iter = rhsList.iterator();
	        while(iter.hasNext()) {
	            Expression expr = (Expression) iter.next();
	            type = addParmFromExpression(expr, parms);
	            
	        }
	        
	        addInCriteria(lhsString, parms, type);
//	        filter = CoherenceFilterUtil.createInFilter(lhsString, parms, type);
		}catch (TranslatorException t) {
			exception = t;
		}
	        
    }
    
    public void addInCriteria(String columnname, List<Object> parms, Class<?> type ) throws TranslatorException {
        filter = CoherenceFilterUtil.createInFilter(columnname, parms, type);
    }
    
    private Class addParmFromExpression(Expression expr, List parms ) {
    	Class type = null;
        if(expr instanceof Literal) {
        	Long longparm = null;
            Literal literal = (Literal) expr;
            
            parms.add(literal);
            
            type = literal.getType();
  
        } else {
            this.exception = new TranslatorException("CoherenceVisitor.Unsupported_expression" + expr); //$NON-NLS-1$
        }
        
        return type;
         
    }
	/** 
	 * Method to get name from the supplied Element
	 * @param e the supplied Element
	 * @return the name
	 */
    // GHH 20080326 - found that code to fall back on Name if NameInSource
	// was null wasn't working properly, so replaced with tried and true
	// code from another custom connector.
	public String getNameFromElement(Column e) {
		String attributeName = e.getNameInSource();
		if (attributeName == null || attributeName.equals("")) { //$NON-NLS-1$
			attributeName = e.getName();
			// If name in source is not set, then fall back to the column name.
		}
		return attributeName;
	}

	public String getNameFromTable(Table e) {
		String tableName = e.getNameInSource();
		if (tableName == null || tableName.equals("")) { //$NON-NLS-1$
			tableName = e.getName();
			// If name in source is not set, then fall back to the column name.
		}
		return tableName;
	}

    /**
     * Helper method for getting runtime {@link org.teiid.connector.metadata.runtime.Element} from a
     * {@link org.teiid.language.DerivedColumn}.
     * @param symbol Input ISelectSymbol
     * @return Element returned metadata runtime Element
     */
    private Column getElementFromSymbol(DerivedColumn symbol) {
        ColumnReference expr = (ColumnReference) symbol.getExpression();
        return expr.getMetadataObject();
    }
	    
	// GHH 20080326 - found that code to fall back on Name if NameInSource
	// was null wasn't working properly, so replaced with tried and true
	// code from another custom connector.
	private String getExpressionString(Expression e) throws TranslatorException {
		String expressionName = null;
		// GHH 20080326 - changed around the IElement handling here
		// - the rest of this method is unchanged
		if(e instanceof ColumnReference) {
			Column mdIDElement = ((ColumnReference)e).getMetadataObject();
			expressionName = mdIDElement.getNameInSource();
			if(expressionName == null || expressionName.equals("")) {  //$NON-NLS-1$
				expressionName = mdIDElement.getName();
			}
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
            final String msg = CoherencePlugin.Util.getString("CoherenceVisitory.unsupportedElementError" , e.toString()); //$NON-NLS-1$
			throw new TranslatorException(msg); 
		}
		expressionName = escapeReservedChars(expressionName);
		return expressionName;
	}
	
	private String escapeReservedChars(String expr) {
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
