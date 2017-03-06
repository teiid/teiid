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
	protected Stack<String> onGoingExpression  = new Stack<String>();
	private boolean limitInUse;
	private SolrQuery query = new SolrQuery();
	private SolrExecutionFactory ef;
	private HashMap<String, String> columnAliasMap = new HashMap<String, String>();
	private boolean countStarInUse;
	private LinkedList<String> dateRange = new LinkedList<String>();
	private boolean dateRangMissingException = false;
	//NOTE: If the entered teiid query doesn't contain a date range that specify the date from-to range, the following error message will appear.
	private static final String DATE_RANGE_MISSING_MSG = "Please provide the query with date range in where clause; For instance 'date_field' between date_1 and date_2";
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
		FunctionModifier funcModifier = this.ef.getFunctionModifiers().get(obj.getName());
		if (funcModifier != null) {
			funcModifier.translate(obj);
		}
			
		StringBuilder sb = new StringBuilder();
		visitNodes(obj.getParameters());
		for (int i = 0; i < obj.getParameters().size(); i++) {
			sb.insert(0,this.onGoingExpression.pop());
			if (i < obj.getParameters().size()-1) {
				sb.insert(0,Tokens.COMMA);
			}
		}
		// Remove method name and its brackets if function's type is timestamp or string
		
		if(!obj.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP) && 
				!obj.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
			sb.insert(0,Tokens.LPAREN);
			sb.insert(0,obj.getName());
			sb.append(Tokens.RPAREN);
		}
		
		this.onGoingExpression.push(sb.toString());
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
		String dateRangeGap = extractDateRangeGap(obj);
		StringBuilder facetFields = new StringBuilder();

		visitNodes(obj.getElements());
		Stack<String> reversedOnGoingExpression = reverseOnGoingExpression(this.onGoingExpression);

		String facetField = reversedOnGoingExpression.pop();
		this.query.setFacet(true);

		if (dateRangeGap == null) {
			facetFields.append(facetField);
		} else {
			setFacetDateRange(dateRangeGap, facetField);
		}

		while (!reversedOnGoingExpression.isEmpty()) {
			facetFields.append(Tokens.COMMA);
			facetFields.append(reversedOnGoingExpression.pop());
		}

		if (dateRangeGap == null) {
			this.query.addFacetPivotField(facetFields.toString());
		} else if (facetFields.length() > 1) {
			facetFields.deleteCharAt(0); // remove the first comma
			facetFields.insert(0, FACET_RANGE_TAG);
			this.query.addFacetPivotField(facetFields.toString());
		}
	}
	/**
     * Extract the date range gap from the parameters of the FORMATTIMESTAMP function in teiid query
     * @param GroupBy object
     * @return String gap
     */
	private String extractDateRangeGap(GroupBy obj) {
		try {
			Function function = (Function) (getTimestampElement(obj));
			Literal literalDateFormat = (Literal) function.getParameters().get(1);
			String dateFormat = literalDateFormat.toString();
			new SimpleDateFormat(dateFormat);
			return getDateRangeGap(dateFormat);
		} catch (Exception e) {
			return null;
		}
	}
	/**
     * Search through the list of the grouby elements to find the timestamp element.
     * @param GroupBy object
     * @return element
     */
	private Expression getTimestampElement(GroupBy obj) {
		List<Expression> elements = obj.getElements();
		for (int i = 0; i < elements.size(); i++) {
			if (elements.get(i).getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
				return elements.get(i);
			}
		}
		return null;
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
	/**
     * Get the date range gap
     * @param String dateFormat
     * @return String gap
     */
	private String getDateRangeGap(String dateFormat) {
		// yyyy-MM-dd'T'HH:mm:ss:SSS'Z'
		if (dateFormat.contains(MINUTE_FORMAT)) {
			return MINUTE;
		} else if (dateFormat.contains(HOUR_FORMAT)) {
			return HOUR;
		} else if (dateFormat.contains(DAY_FORMAT)) {
			return DAY;
		} else if (dateFormat.contains(MONTH_FORMAT)) {
			return MONTH;
		} else {
			return YEAR;
		}
	}
	
	private String formatSolrQuery(String solrQuery) {
		solrQuery = solrQuery.replace("%", "*"); //$NON-NLS-1$ //$NON-NLS-2$
		solrQuery = solrQuery.replace("'",""); //$NON-NLS-1$ //$NON-NLS-2$
		// solrQuery = solrQuery.replace("_", "?");
		return solrQuery;
	}

	public SolrQuery getSolrQuery() throws TranslatorException {
		
		if(this.dateRangMissingException) {
			throw new TranslatorException(DATE_RANGE_MISSING_MSG);
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
