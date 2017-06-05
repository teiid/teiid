/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.solr;

import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.visitor.SQLStringVisitor.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.TimeZone;

import org.apache.solr.client.solrj.SolrQuery;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.FunctionModifier;

public class SolrSQLHierarchyVistor extends HierarchyVisitor {
    private static SimpleDateFormat sdf;
    static {
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'"); //$NON-NLS-1$
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); //$NON-NLS-1$
    }
    
	@SuppressWarnings("unused")
	private RuntimeMetadata metadata;
	protected StringBuilder buffer = new StringBuilder();
	private List<String> fieldNameList = new ArrayList<String>();
	protected Stack<String> onGoingExpression  = new Stack<String>();
	private boolean limitInUse;
	private SolrQuery query = new SolrQuery();
	private SolrExecutionFactory ef;
	private HashMap<String, String> columnAliasMap = new HashMap<String, String>();
	private boolean countStarInUse;
	//NOTE: If the entered teiid query doesn't contain a date range that specify the date from-to range, the following error message will appear.
	private static final String DATE_RANGE_MISSING_MSG = "Please provide the query with date range in where clause; For instance => \n 'timestamp' BETWEEN 'yyyy-MM-dd HH:mm:ss' AND 'yyyy-MM-dd HH:mm:ss'";
	private static final String FUNCTION_IS_NOT_ALLOWED_MSG = "Only GAP(timestampColumn, 'TimeGap') is allowed \n TimeGap = ['MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR']";
    private static final String FUNCTION_DOESNT_MATCH = "Function in the group by clause must match that in the select clause";
    private static final String GAP_NOT_ALLOWED_MSG = "GAP is only allowed in SELECT queries";
    	
	private static final String FACET_TAG = "{!tag=r1}";
	private static final String FACET_RANGE_TAG = "{!range=r1}";
	private static final String FACET_RANGE = "facet.range";
	private static final String FACET_RANGE_START = "facet.range.start";
	private static final String FACET_RANGE_END = "facet.range.end";
	private static final String FACET_RANGE_GAP = "facet.range.gap";
	private static final String PLUS_ONE = "+1";
	private static final String MINUTE = "MINUTE";
	private static final String HOUR = "HOUR";
	private static final String DAY = "DAY";
	private static final String MONTH = "MONTH";
	private static final String YEAR = "YEAR";
	
	private String timeGap = null;
	private ColumnReference gapColumn = null;
    private Condition where;

	public SolrSQLHierarchyVistor(RuntimeMetadata metadata, SolrExecutionFactory ef) {
		this.metadata = metadata;
		this.ef = ef;
	}

	@Override
	public void visit(DerivedColumn obj) {		
		visitNode(obj.getExpression());
		
		String expr = this.onGoingExpression.pop();
		if (obj.getAlias() != null) {
			this.columnAliasMap.put(obj.getAlias(), expr);
		}		
		
		query.addField(expr);
		fieldNameList.add(expr);
	}

	public static String getColumnName(ColumnReference obj) {
		String elemShortName = null;
		AbstractMetadataRecord elementID = obj.getMetadataObject();
        if(elementID != null) {
            elemShortName = getRecordName(elementID);
        } else {
            elemShortName = obj.getName();
        }
		return elemShortName;
	}	
	
	@Override
	public void visit(ColumnReference obj) {
		String columnName;
		if (obj.getMetadataObject() != null) {
			columnName = getColumnName(obj);
		} else {
			columnName = this.columnAliasMap.get(getColumnName(obj));
		}
		
		this.onGoingExpression.push(columnName);
	}

	/**
	 * @return the full gapColumn names tableName.columnNames
	 */
	public List<String> getFieldNameList() {
		return fieldNameList;
	}

	/**
	 * Note: Solr does not support <,> exclusively. It is always
	 * <=, >=
	 */
	@Override
	public void visit(Comparison obj) {		
		visitNode(obj.getLeftExpression());
		String lhs = this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		String rhs = this.onGoingExpression.pop();
		
		if (lhs != null) {
			switch (obj.getOperator()) {
			case EQ:
				buffer.append(lhs).append(":").append(rhs); //$NON-NLS-1$
				break;
			case NE:
				buffer.append(Reserved.NOT).append(Tokens.SPACE);
				buffer.append(lhs).append(Tokens.COLON).append(rhs);
				break;
			case LE:
				buffer.append(lhs).append(":[* TO"); //$NON-NLS-1$
				buffer.append(Tokens.SPACE).append(rhs).append(Tokens.RSBRACE);  
				break;
			case LT:
				buffer.append(lhs).append(":[* TO"); //$NON-NLS-1$
				buffer.append(Tokens.SPACE).append(rhs).append(Tokens.RSBRACE);
				buffer.append(Tokens.SPACE).append(Reserved.AND).append(Tokens.SPACE); 
				buffer.append(Reserved.NOT).append(Tokens.SPACE).append(lhs);
				buffer.append(Tokens.COLON).append(rhs);
				break;
			case GE:
				buffer.append(lhs).append(":[").append(rhs).append(" TO *]");//$NON-NLS-1$ //$NON-NLS-2$
				break;
			case GT:
				buffer.append(lhs).append(":[").append(rhs); //$NON-NLS-1$
				buffer.append(" TO *]").append(Tokens.SPACE).append(Reserved.AND).append(Tokens.SPACE); //$NON-NLS-1$
				buffer.append(Reserved.NOT).append(Tokens.SPACE).append(lhs);
				buffer.append(Tokens.COLON).append(rhs);
				break;
			}
		}
	}

	@Override
	public void visit(AndOr obj) {
		
		// prepare statement
		buffer.append(Tokens.LPAREN);
		buffer.append(Tokens.LPAREN);

		// walk left node
		super.visitNode(obj.getLeftCondition());

		buffer.append(Tokens.RPAREN);

		switch (obj.getOperator()) {
		case AND:
			buffer.append(Tokens.SPACE).append(Reserved.AND).append(Tokens.SPACE);
			break;
		case OR:
			buffer.append(Tokens.SPACE).append(Reserved.OR).append(Tokens.SPACE);
			break;
		}
		buffer.append(Tokens.LPAREN);
		
		//walk right node
		super.visitNode(obj.getRightCondition());
		buffer.append(Tokens.RPAREN);
		buffer.append(Tokens.RPAREN);
	}

	@Override
	public void visit(In obj) {
		visitNode(obj.getLeftExpression());
		String lhs = this.onGoingExpression.pop();
		
		visitNodes(obj.getRightExpressions());
		
		if (obj.isNegated()){
			buffer.append(Reserved.NOT).append(Tokens.SPACE);
		}
		
		//start solr expression
		buffer.append(lhs).append(Tokens.COLON).append(Tokens.LPAREN);
		
		int i = obj.getRightExpressions().size();
		while(i-- > 0) {
			//append rhs side as we iterates
			buffer.append(onGoingExpression.pop());
			
			if(i > 0) {				
				buffer.append(Tokens.SPACE).append(Reserved.OR).append(Tokens.SPACE);
			}
		}
		buffer.append(Tokens.RPAREN);
	}

	
	/** 
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Like)
	 * Description: transforms the like statements into solor syntax
	 */
	@Override
	public void visit(Like obj) {
		visitNode(obj.getLeftExpression());
		String lhs = this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		String rhs = this.onGoingExpression.pop();
		
		if (obj.isNegated()){
			buffer.append(Reserved.NOT).append(Tokens.SPACE);
		}
		buffer.append(lhs).append(Tokens.COLON).append(formatSolrQuery(rhs));
	}

	@Override
	public void visit(Literal obj) {
		
    	if (obj.getValue() == null) {
    	    this.onGoingExpression.push(NULL);
        } else {
            Class<?> type = obj.getType();
            Object val = obj.getValue();
            if(Number.class.isAssignableFrom(type)) {
            	this.onGoingExpression.push(escapeString(String.valueOf(val))); 
            } 
            else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            	this.onGoingExpression.push(obj.getValue().equals(Boolean.TRUE) ? TRUE : FALSE);
            } 
            else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP) 
                    || type.equals(DataTypeManager.DefaultDataClasses.TIME) 
                    || type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            	synchronized (sdf) {
                	this.onGoingExpression.push(escapeString(sdf.format(val)));
				}
            } 
            else {
            	this.onGoingExpression.push(escapeString(val.toString()));
            }
        }
	}

    /**
     * Creates a SQL-safe string. Simply replaces all occurrences of ' with ''
     * @param str the input string
     * @return a SQL-safe string
     */
    protected String escapeString(String str) {
    	// needs escaping + - && || ! ( ) { } [ ] ^ " ~ * ? :
    	// source: http://khaidoan.wikidot.com/solr
    	String[] array = {"+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~",  "*", "?", ":"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ //$NON-NLS-11$ //$NON-NLS-12$ //$NON-NLS-13$ //$NON-NLS-14$ //$NON-NLS-15$ //$NON-NLS-16$ //$NON-NLS-17$

    	for (int i = 0; i < array.length; i++) {
    		str = StringUtil.replaceAll(str, array[i],  "\\" + array[i]); //$NON-NLS-1$
    	}
    	return str;
    }	
    
	@Override
	public void visit(Limit obj) {
		this.limitInUse = true;
		this.query.setRows(obj.getRowLimit());
		this.query.setStart(obj.getRowOffset());
	}
	
	@Override
	public void visit(OrderBy obj) {
		visitNodes(obj.getSortSpecifications());
	}
	
	@Override
	public void visit(SortSpecification obj) {
		visitNode(obj.getExpression());
		String expr = this.onGoingExpression.pop();
		this.query.addSort(expr, obj.getOrdering() == SortSpecification.Ordering.ASC?SolrQuery.ORDER.asc:SolrQuery.ORDER.desc);
	}
	
	@Override
	public void visit(Function obj) {
	    FunctionModifier funcModifier = this.ef.getFunctionModifiers().get(obj.getName());
        if (funcModifier != null) {
            funcModifier.translate(obj);
        }
        
		StringBuilder sb = new StringBuilder();
		if(obj.getName().equalsIgnoreCase("gap")) {
		    Expression gapExpr = obj.getParameters().get(1);
		    if (!(gapExpr instanceof Literal)) {
		        throw new TeiidRuntimeException(new TranslatorException(FUNCTION_IS_NOT_ALLOWED_MSG));
		    }
			validateTimeGap(((Literal)gapExpr).getValue().toString());
			Expression expression = obj.getParameters().get(0);
			if (!(expression instanceof ColumnReference)) {
			    throw new TeiidRuntimeException(new TranslatorException(FUNCTION_IS_NOT_ALLOWED_MSG));
			}
			if (this.gapColumn != null && !this.gapColumn.getMetadataObject().equals(((ColumnReference) expression).getMetadataObject())) {
			    throw new TeiidRuntimeException(new TranslatorException(FUNCTION_DOESNT_MATCH));
			}
            this.gapColumn = (ColumnReference) expression;
            visit(gapColumn);
			sb.insert(0,this.onGoingExpression.pop());
		} else {
			visitNodes(obj.getParameters());
			for (int i = 0; i < obj.getParameters().size(); i++) {
				sb.insert(0,this.onGoingExpression.pop());
				if (i < obj.getParameters().size()-1) {
					sb.insert(0,Tokens.COMMA);
				}
			}
			sb.insert(0,Tokens.LPAREN);
			sb.insert(0,obj.getName());
			sb.append(Tokens.RPAREN);
		}
		
		this.onGoingExpression.push(sb.toString());
	}
	
	
	private void validateTimeGap(String val) {
	    if (val == null) {
	        throw new TeiidRuntimeException(new TranslatorException(FUNCTION_IS_NOT_ALLOWED_MSG));
	    }
		switch(val){
			case MINUTE:
			case HOUR:
			case DAY:
			case MONTH:
			case YEAR:
			    if (this.timeGap != null && !this.timeGap.equals(val)) {
			        throw new TeiidRuntimeException(new TranslatorException(FUNCTION_DOESNT_MATCH));
			    }
			    this.timeGap = val;
			    break;
			default:
			    throw new TeiidRuntimeException(new TranslatorException(FUNCTION_IS_NOT_ALLOWED_MSG));
		}
	}
	
	@Override
	public void visit(AggregateFunction obj) {
		if (obj.getName().equals(AggregateFunction.COUNT)) {
	        // this is only true for count(*) case, so we need implicit group id clause
			this.query.setRows(0);
			this.countStarInUse = true;
			this.onGoingExpression.push("1"); //$NON-NLS-1$
		}
		else if (obj.getName().equals(AggregateFunction.AVG)) {
		}
		else if (obj.getName().equals(AggregateFunction.SUM)) {
		}
		else if (obj.getName().equals(AggregateFunction.MIN)) {
		}
		else if (obj.getName().equals(AggregateFunction.MAX)) {
		}
		else {
		}
    }	
	
	@Override
	public void visit(GroupBy obj) {
	    
	    String gap = this.timeGap;
        this.timeGap = null;
	    
		StringBuilder facetFields = new StringBuilder();

		visitNodes(obj.getElements());
		
		if (gap != null) {
		    if (timeGap == null) {
		        throw new TeiidRuntimeException(new TranslatorException(FUNCTION_DOESNT_MATCH));
		    }
		    validateTimeGap(gap);
		}
		
		Stack<String> reversedOnGoingExpression = reverseOnGoingExpression(this.onGoingExpression);

		this.query.setFacet(true);

		if (this.timeGap == null) {
		    String facetField = reversedOnGoingExpression.pop();
			facetFields.append(facetField);
		} else {
		    String facetField = getColumnName(this.gapColumn);
		    for (Iterator<String> iter = reversedOnGoingExpression.iterator(); iter.hasNext();) {
		        String field = iter.next();
		        if (field.equals(facetField)) {
		            iter.remove();
		        }
		    }
            setFacetDateRange(facetField);
		}

		while (!reversedOnGoingExpression.isEmpty()) {
			facetFields.append(Tokens.COMMA);
			facetFields.append(reversedOnGoingExpression.pop());
		}

		if (this.timeGap == null) {
			this.query.addFacetPivotField(facetFields.toString());
		} else if (facetFields.length() > 1) {
			facetFields.deleteCharAt(0); // remove the first comma
			facetFields.insert(0, FACET_RANGE_TAG);
			this.query.addFacetPivotField(facetFields.toString());
		}
		this.query.setFacetMissing(true);
	}

	/**
	 * Examine the where clause for predicates that match an inclusive range over the
	 * facet field
	 */
    private void setFacetDateRange(String facetField) {
        this.query.add(FACET_RANGE, FACET_TAG + facetField);
        boolean lower = false;
        boolean upper = false;
        for (Condition cond : LanguageUtil.separateCriteriaByAnd(where)) {
            if (!(cond instanceof Comparison)) {
                continue;
            }
            Comparison comp = (Comparison)cond;
            if (!(comp.getLeftExpression() instanceof ColumnReference)) {
                continue;
            }
            ColumnReference cr = (ColumnReference)comp.getLeftExpression();
            if (!cr.getMetadataObject().equals(this.gapColumn.getMetadataObject())) {
                continue;
            }
            if (comp.getOperator() == Comparison.Operator.LE) {
                String val = null;
                synchronized (sdf) {
                    val = sdf.format(((Literal)comp.getRightExpression()).getValue());
                }
                this.query.add(FACET_RANGE_END, val);
                upper = true;
            } else if (comp.getOperator() == Comparison.Operator.GE) {
                String val = null;
                synchronized (sdf) {
                    val = sdf.format(((Literal)comp.getRightExpression()).getValue());
                }
                this.query.add(FACET_RANGE_START, val);
                lower = true;
            }
        }
        this.query.add(FACET_RANGE_GAP, PLUS_ONE + this.timeGap);
        if (!lower || !upper) {
            throw new TeiidRuntimeException(new TranslatorException(DATE_RANGE_MISSING_MSG));
        }
    }
	
	/**
     * Reverse the "onGoingExpression" stack object
     * @param Stack<String> expression
     * @return Stack<String> reversed expression
     */
	private Stack<String> reverseOnGoingExpression(Stack<String> expression) {
		Stack<String> result = new Stack<String>();
		while (!expression.isEmpty()) {
			result.push(expression.pop());
		}
		return result;
	}
	
	private String formatSolrQuery(String solrQuery) {
		solrQuery = solrQuery.replace("%", "*"); //$NON-NLS-1$ //$NON-NLS-2$
		solrQuery = solrQuery.replace("'",""); //$NON-NLS-1$ //$NON-NLS-2$
		// solrQuery = solrQuery.replace("_", "?");
		return solrQuery;
	}

	public SolrQuery getSolrQuery() {
		if (buffer == null || buffer.length() == 0) {
			buffer = new StringBuilder("*:*"); //$NON-NLS-1$
		}
		return query.setQuery(buffer.toString());
	}
	
	public boolean isLimitInUse() {
		return this.limitInUse;
	}
	
	public boolean isCountStarInUse() {
		return countStarInUse;
	}
	
	@Override
	public void visit(Select obj) {
        this.where = obj.getWhere();
        super.visit(obj);
        if (timeGap != null && obj.getGroupBy() == null) {
            throw new TeiidRuntimeException(new TranslatorException(FUNCTION_DOESNT_MATCH));
        }
	}

    public void visitCommand(Command command) throws TranslatorException {
        try {
            visitNode(command);
            if (timeGap != null && !(command instanceof Select)) {
                throw new TranslatorException(GAP_NOT_ALLOWED_MSG);
            }
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof TranslatorException) {
                throw (TranslatorException)e.getCause();
            }
            throw e;
        }
    }
	
}
