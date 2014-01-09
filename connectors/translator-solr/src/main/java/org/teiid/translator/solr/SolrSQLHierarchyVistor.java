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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.solr.client.solrj.SolrQuery;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;

public class SolrSQLHierarchyVistor extends HierarchyVisitor {

	@SuppressWarnings("unused")
	private RuntimeMetadata metadata;
	protected StringBuilder buffer = new StringBuilder();
	private List<String> fieldNameList = new ArrayList<String>();
	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	private boolean limitInUse;
	private SolrQuery query = new SolrQuery();

	public SolrSQLHierarchyVistor(RuntimeMetadata metadata) {
		this.metadata = metadata;
	}

	@Override
	public void visit(DerivedColumn obj) {
		visitNode(obj.getExpression());
		if (obj.getExpression() instanceof ColumnReference) {
			Column c = (Column)this.onGoingExpression.pop();
			query.addField(c.getName());
			fieldNameList.add(c.getName());
		}
	}

	
	@Override
	public void visit(ColumnReference obj) {
		this.onGoingExpression.push(obj.getMetadataObject());
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
		Column lhs = (Column)this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		Object rhs = this.onGoingExpression.pop();
		
		if (lhs != null) {
			switch (obj.getOperator()) {
			case EQ:
				buffer.append(lhs.getName()).append(":").append(rhs.toString()); //$NON-NLS-1$
				break;
			case NE:
				buffer.append(Reserved.NOT).append(Tokens.SPACE);
				buffer.append(lhs.getName()).append(Tokens.COLON).append(rhs.toString());
				break;
			case LE:
				buffer.append(lhs.getName()).append(":[* TO"); //$NON-NLS-1$
				buffer.append(Tokens.SPACE).append(rhs.toString()).append(Tokens.RSBRACE);  
				break;
			case LT:
				buffer.append(lhs.getName()).append(":[* TO"); //$NON-NLS-1$
				buffer.append(Tokens.SPACE).append(rhs.toString()).append(Tokens.RSBRACE);
				buffer.append(Tokens.SPACE).append(Reserved.AND).append(Tokens.SPACE); 
				buffer.append(Reserved.NOT).append(Tokens.SPACE).append(lhs.getName());
				buffer.append(Tokens.COLON).append(rhs.toString());
				break;
			case GE:
				buffer.append(lhs.getName()).append(":[").append(rhs.toString()).append(" TO *]");//$NON-NLS-1$ //$NON-NLS-2$
				break;
			case GT:
				buffer.append(lhs.getName()).append(":[").append(rhs.toString()); //$NON-NLS-1$
				buffer.append(" TO *]").append(Tokens.SPACE).append(Reserved.AND).append(Tokens.SPACE); //$NON-NLS-1$
				buffer.append(Reserved.NOT).append(Tokens.SPACE).append(lhs.getName());
				buffer.append(Tokens.COLON).append(rhs.toString());
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
		Column lhs = (Column)this.onGoingExpression.pop();
		
		visitNodes(obj.getRightExpressions());
		
		if (obj.isNegated()){
			buffer.append(Reserved.NOT).append(Tokens.SPACE);
		}
		
		//start solr expression
		buffer.append(lhs.getName()).append(Tokens.COLON).append(Tokens.LPAREN);
		
		int i = obj.getRightExpressions().size();
		while(i-- > 0) {
			//append rhs side as we iterates
			buffer.append(onGoingExpression.pop().toString());
			
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
		Column lhs = (Column)this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		Object rhs = this.onGoingExpression.pop();
		
		if (obj.isNegated()){
			buffer.append(Reserved.NOT).append(Tokens.SPACE);
		}
		buffer.append(lhs.getName()).append(Tokens.COLON).append(formatSolrQuery(rhs.toString()));
	}

	@Override
	public void visit(Literal obj) {
    	if (obj.getValue() == null) {
            buffer.append(NULL);
        } else {
            Class<?> type = obj.getType();
            Object val = obj.getValue();
            if(Number.class.isAssignableFrom(type)) {
            	this.onGoingExpression.push(val);
            } 
            else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            	this.onGoingExpression.push(obj.getValue().equals(Boolean.TRUE) ? TRUE : FALSE);
            } 
            else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            	this.onGoingExpression.push(new SimpleDateFormat("yyyy-MM-DD'T'HH-mm-ss:SSSZ").format(val)); //$NON-NLS-1$
            } 
            else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
            	this.onGoingExpression.push(new SimpleDateFormat("HH-mm-ss:SSSZ").format(val)); //$NON-NLS-1$
            } 
            else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            	this.onGoingExpression.push(new SimpleDateFormat("yyyy-MM-DD").format(val)); //$NON-NLS-1$            	
            }  
            else {
            	this.onGoingExpression.push(escapeString(val.toString(), "\""));//$NON-NLS-1$ 
            }
        }
	}

    /**
     * Creates a SQL-safe string. Simply replaces all occurrences of ' with ''
     * @param str the input string
     * @return a SQL-safe string
     */
    protected String escapeString(String str, String quote) {
        return StringUtil.replaceAll(str, quote, quote + quote);
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
		Column c = (Column)this.onGoingExpression.pop();
		this.query.addSort(c.getName(), obj.getOrdering() == SortSpecification.Ordering.ASC?SolrQuery.ORDER.asc:SolrQuery.ORDER.desc);
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
}
