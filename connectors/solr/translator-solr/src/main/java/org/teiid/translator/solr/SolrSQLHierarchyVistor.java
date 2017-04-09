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
package org.teiid.translator.solr;

import static org.teiid.language.SQLConstants.Reserved.FALSE;
import static org.teiid.language.SQLConstants.Reserved.NULL;
import static org.teiid.language.SQLConstants.Reserved.TRUE;
import static org.teiid.language.visitor.SQLStringVisitor.getRecordName;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.TimeZone;

import org.apache.solr.client.solrj.SolrQuery;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.In;
import org.teiid.language.Like;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.OrderBy;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.SortSpecification;
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
	private List<String> fieldNameListWithNoCount = new ArrayList<String>();
	private int timestampCount = 0;
	
	private boolean inComparison = false;
	
	protected Stack<String> onGoingExpression  = new Stack<String>();
	private boolean limitInUse;
	private SolrQuery query = new SolrQuery();
	private SolrExecutionFactory ef;
	private HashMap<String, String> columnAliasMap = new HashMap<String, String>();
	private boolean countStarInUse;
	private LinkedList<String> dateRange = new LinkedList<String>();
	private boolean dateRangMissingException = false;
	//NOTE: If the entered teiid query doesn't contain a date range that specify the date from-to range, the following error message will appear.
	private static final String DATE_RANGE_MISSING_MSG = "\n Please provide the query with date range in where clause; For instance => \n 'timestamp' BETWEEN 'yyyy-MM-dd HH:mm:ss' AND 'yyyy-MM-dd HH:mm:ss'";
	private static final String FACET_TAG = "{!tag=r1}";
	private static final String FACET_RANGE_TAG = "{!range=r1}";
	private static final String FACET_RANGE = "facet.range";
	private static final String FACET_RANGE_START = "facet.range.start";
	private static final String FACET_RANGE_END = "facet.range.end";
	private static final String FACET_RANGE_GAP = "facet.range.gap";
	private static final String PLUS_ONE = "+1";
	private static final String MINUTE_FORMAT = "mm";
	private static final String MINUTE = "MINUTE";
	private static final String HOUR_FORMAT = "HH";
	private static final String HOUR = "HOUR";
	private static final String DAY_FORMAT = "dd";
	private static final String DAY = "DAY";
	private static final String MONTH_FORMAT = "MM";
	private static final String MONTH = "MONTH";
	private static final String YEAR = "YEAR";
	
	private boolean moreThanOneTimestamp = false;
	private static final String ONLY_ONE_TIMESTAMP_IS_ALLOWED = "\n Only one timestamp is allowed in both select and groupby clauses ";
	
	private boolean parametersDontMatch = false;
	private static final String PARAMETERS_DONT_MATCH = "\n Parameters must be the same in both the select and group by clauses  ";
	
	//private boolean invalidLiteralPositionException = false;
	//private static final String INVALID_LITERAL_POSITION_MSG = "\n No literals are allowed except in the where clause as in form of => \n BETWEEN 'yyyy-MM-dd HH:mm:ss' AND 'yyyy-MM-dd HH:mm:ss' \n  -or- \n columnName = 'value'";
	
	private boolean functionIsNotAllowed = false;
	private static final String FUNCTION_IS_NOT_ALLOWED_MSG = " is not allowed. Only \n GAP(timestampColumn, 'TimeGap') is allowed \n TimeGap = ['MINUTE', 'DAY', 'MONTH', 'YEAR']";
	private String functionIsNotAllowedMsg = "";

	private String timeGap = null;
	private ColumnReference column = null;

	private boolean functionsMatch = true;
	private static final String FUNCTION_DOESNT_MATCH = "\n Function in the group by clause must match that in the select clause";
	
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
		if(!this.countStarInUse) {
			this.fieldNameListWithNoCount.add(expr);
		}
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
	/*
	 * Check if this column belongs to the timestamp class, then add this column at the first position of the "onGoingExpression" object.
	 * To be able to construct the right solr pivot query that takes the {!range} function at its first position to be applied on the following parameters
	 * For example: facet.range={!tag=r1}date&facet.pivot={!range=r1}field 
	 * The range r1 will be applied on the "field" field
	 */
	@Override
	public void visit(ColumnReference obj) {
		String columnName;
		if (obj.getMetadataObject() != null) {
			columnName = getColumnName(obj);
		} else {
			columnName = this.columnAliasMap.get(getColumnName(obj));
		}
		
		if(obj.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
			if(!this.inComparison) {
				this.timestampCount++;
			}
			this.onGoingExpression.add(0, columnName);
		} else {
			this.onGoingExpression.push(columnName);
		}
	}

	/**
	 * @return the full column names tableName.columnNames
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
		this.inComparison = true;
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
            buffer.append(NULL);
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
                	// Push date start and end to "dateRange" object 
                	this.dateRange.add(sdf.format(val));
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
		if (!countStarInUse) {
			this.query.setRows(obj.getRowLimit());
			this.query.setStart(obj.getRowOffset());
		}
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
		if ( 	( !obj.getName().equals("gap") )|| 
				( obj.getName().equals("gap") && obj.getParameters().get(0).getClass() == Function.class) ) 
		{
			this.functionIsNotAllowed = true;
			this.functionIsNotAllowedMsg = obj.getName() + FUNCTION_IS_NOT_ALLOWED_MSG;
		}
			
		StringBuilder sb = new StringBuilder();
		if(obj.getName() == "gap" && !this.functionIsNotAllowed) {
			visitNodes(obj.getParameters());
			this.timeGap = validateTimeGap(this.onGoingExpression.pop());
			this.column = (ColumnReference) obj.getParameters().get(0);
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
	
	
	private String validateTimeGap(String timeGap) {
		switch(timeGap){
			case MINUTE:
				return MINUTE;
			case HOUR:
				return HOUR;
			case DAY:
				return DAY;
			case MONTH:
				return MONTH;
			case YEAR:
				return YEAR;
			default:
				this.functionIsNotAllowedMsg = "gap" + FUNCTION_IS_NOT_ALLOWED_MSG;
				this.functionIsNotAllowed = true;
				return null;
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
		
		this.inComparison = false;
		
		String timeGap = this.timeGap;
		ColumnReference column = this.column;
		StringBuilder facetFields = new StringBuilder();
		
		if(this.timestampCount > 1) {
			this.moreThanOneTimestamp = true;
		}
		this.timestampCount = 0;
		
		visitNodes(obj.getElements());
		
		if(this.timestampCount > 1) {
			this.moreThanOneTimestamp = true;
		}
		this.timestampCount = 0;
		
		//method in group by doesn't match that in the select clause
		if( (timeGap != null && timeGap != this.timeGap) || (column != null && column.getName() != this.column.getName()) ) {
			this.functionsMatch = false;
		}
		
		Stack<String> reversedOnGoingExpression = reverseOnGoingExpression(this.onGoingExpression);
		
		this.compareSelectAndGroupByParameters(this.fieldNameListWithNoCount, reversedOnGoingExpression);
		
		String facetField = reversedOnGoingExpression.pop();
		this.query.setFacet(true);

		if (this.timeGap == null) {
			facetFields.append(facetField);
		} else {
			setFacetDateRange(this.timeGap, facetField);
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
	
	private void compareSelectAndGroupByParameters(List<String> selectFieldNames, Stack<String> groupByFieldNames) {
		try{
			List<String> tempSelectFields = new ArrayList<String>(selectFieldNames); 
			Stack<String> tempGroupByFields = (Stack<String>) groupByFieldNames.clone();
			Collections.sort(tempSelectFields);
			Collections.sort(tempGroupByFields);
			
			if(tempSelectFields.size() != tempGroupByFields.size()) {
				this.parametersDontMatch = true;
			} else {
				for(int i=0; i<tempSelectFields.size(); i++) {
					if(!tempSelectFields.get(i).equals(tempGroupByFields.get(i))) {
						this.parametersDontMatch = true;
						break;
					}
				}
			}
		} catch(Exception e) {
			this.parametersDontMatch = true;
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
	/**
     * Add facet range to the solr query (range field, start date, end date and the gap between dates)
     * @param String gap, String facetField
     * @return Stack<String>
     */
	private void setFacetDateRange(String dateRangeGap, String facetField) {
		if (!dateRange.isEmpty() && dateRange.size() == 2) {
			this.query.add(FACET_RANGE, FACET_TAG + facetField);
			this.query.add(FACET_RANGE_START, dateRange.poll());
			this.query.add(FACET_RANGE_END, dateRange.poll());
			this.query.add(FACET_RANGE_GAP, PLUS_ONE + dateRangeGap);
		} else {
			this.dateRangMissingException = true;
		}
	}
	
	private String formatSolrQuery(String solrQuery) {
		solrQuery = solrQuery.replace("%", "*"); //$NON-NLS-1$ //$NON-NLS-2$
		solrQuery = solrQuery.replace("'",""); //$NON-NLS-1$ //$NON-NLS-2$
		// solrQuery = solrQuery.replace("_", "?");
		return solrQuery;
	}

	public SolrQuery getSolrQuery() throws TranslatorException {
		
		String errorMsg = "";
		boolean errorFlag = false;
		if(!this.functionsMatch) {
			errorMsg += FUNCTION_DOESNT_MATCH;
			errorFlag = true;
		}
		if(this.functionIsNotAllowed) {
			errorMsg += this.functionIsNotAllowedMsg;
			errorFlag = true;
		}
		if(this.dateRangMissingException) {
			errorMsg += DATE_RANGE_MISSING_MSG;
			errorFlag = true;
		}
		if(this.moreThanOneTimestamp) {
			errorMsg += ONLY_ONE_TIMESTAMP_IS_ALLOWED;
			errorFlag = true;
		}
		if(this.parametersDontMatch) {
			errorMsg += PARAMETERS_DONT_MATCH;
			errorFlag = true;
		}
		if(errorFlag) {
			throw new TranslatorException(errorMsg);
		}
		
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
	
}
