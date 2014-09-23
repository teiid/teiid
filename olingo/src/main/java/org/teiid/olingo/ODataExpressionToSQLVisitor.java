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
package org.teiid.olingo;

import static org.teiid.language.SQLConstants.Reserved.CONVERT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.Alias;
import org.apache.olingo.server.api.uri.queryoption.expression.Binary;
import org.apache.olingo.server.api.uri.queryoption.expression.Enumeration;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.LambdaRef;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.Method;
import org.apache.olingo.server.api.uri.queryoption.expression.TypeLiteral;
import org.apache.olingo.server.api.uri.queryoption.expression.Unary;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.translator.SourceSystemFunctions;

public class ODataExpressionToSQLVisitor extends ODataExpressionVisitor {
	private Stack<org.teiid.query.sql.symbol.Expression> stack = new Stack<org.teiid.query.sql.symbol.Expression>();
	private ODataQueryContext context;
	private ArrayList<SQLParam> params = new ArrayList<SQLParam>();
	private boolean prepared = false;
	private List<Exception> exceptions = new ArrayList<Exception>(); 
	private UriInfo uriInfo;
	
	public ODataExpressionToSQLVisitor(ODataQueryContext context, boolean prepared, UriInfo info){
		this.context = context;
		this.prepared = prepared;
		this.uriInfo = info;
	}
	
	public org.teiid.query.sql.symbol.Expression getExpression(Expression expr) {
		accept(expr);
		return this.stack.pop();
	}
	
	@Override
	public void visit(Alias expr) {
		Object value = LiteralParser.parseLiteral(this.uriInfo.getValueForAlias(expr.getParameterName()));
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(value, JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
		}
		else {
			this.stack.add(new Constant(value));
		}
	}

	@Override
	public void visit(Binary expr) {
		accept(expr.getLeftOperand());
		org.teiid.query.sql.symbol.Expression lhs = this.stack.pop();
		
		accept(expr.getRightOperand());
		org.teiid.query.sql.symbol.Expression rhs = this.stack.pop();
		
		switch(expr.getOperator()) {
		case HAS:
			// TODO: not supported. What would be SQL equivalent?
			break;
		case MUL:
			stack.push(new Function("*", new org.teiid.query.sql.symbol.Expression[] {lhs, rhs})); //$NON-NLS-1$
			break;
		case DIV:
			stack.push(new Function("/", new org.teiid.query.sql.symbol.Expression[] {lhs, rhs})); //$NON-NLS-1$
			break;
		case MOD:
			stack.push(new Function("MOD", new org.teiid.query.sql.symbol.Expression[] {lhs, rhs})); //$NON-NLS-1$
			break;
		case ADD:
			stack.push(new Function("+", new org.teiid.query.sql.symbol.Expression[] {lhs, rhs})); //$NON-NLS-1$
			break;
		case SUB:
			stack.push(new Function("-", new org.teiid.query.sql.symbol.Expression[] {lhs, rhs})); //$NON-NLS-1$
			break;
		case GT:
			this.stack.push(new CompareCriteria(lhs, CompareCriteria.GT, rhs));
			break;
		case GE:
			this.stack.push(new CompareCriteria(lhs, CompareCriteria.GE, rhs));
			break;
		case LT:
			this.stack.push(new CompareCriteria(lhs, CompareCriteria.LT, rhs));
			break;
		case LE:
			this.stack.push(new CompareCriteria(lhs, CompareCriteria.LE, rhs));
			break;
		case EQ:
			if (rhs instanceof Constant && ((Constant)rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
				this.stack.push(new IsNullCriteria(lhs));
			}
			else {
				this.stack.push(new CompareCriteria(lhs, CompareCriteria.EQ, rhs));
			}			
			break;
		case NE:
			if (rhs instanceof Constant && ((Constant)rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
				IsNullCriteria crit = new IsNullCriteria(lhs);
				crit.setNegated(true);
				this.stack.push(crit);
			}
			else {
				this.stack.push(new CompareCriteria(lhs, CompareCriteria.NE, rhs));
			}
			break;
		case AND:
			this.stack.push(new CompoundCriteria(CompoundCriteria.AND, (Criteria)lhs, (Criteria)rhs));
			break;
		case OR:	
			this.stack.push(new CompoundCriteria(CompoundCriteria.OR, (Criteria)lhs, (Criteria)rhs));
			break;
		}
	}

	@Override
	public void visit(Enumeration expr) {
		this.exceptions.add(new TeiidException("un supported option"));//$NON-NLS-1$
	}

	@Override
	public void visit(LambdaRef expr) {
		//TODO: any and All implementations
	}

	@Override
	public void visit(Literal expr) {
		Object value = LiteralParser.parseLiteral(expr.getText());
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(value, JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
		}
		else {
			this.stack.add(new Constant(value));
		}
	}

	@Override
	public void visit(Member expr) {
		ResourcePropertyCollector visitor = new ResourcePropertyCollector();
		visitor.visit(expr.getResourcePath());
		UriResource resource = visitor.getResource();
		
		if (resource.getKind() == UriResourceKind.primitiveProperty) {
			this.stack.add(new ElementSymbol(((UriResourceProperty)resource).getProperty().getName(), context.getEdmEntityTableGroup()));
		}
		else if (resource.getKind() == UriResourceKind.navigationProperty) {
			EdmNavigationProperty navigation = ((UriResourceNavigation)resource).getProperty();
			EdmEntityType type = navigation.getType();
			if (!visitor.isCount()) {
				this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16028, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16028)));
			}
			
			GroupSymbol navGroup = new GroupSymbol(context.getNextAliasGroup(), type.getNamespace()+"."+type.getName());//$NON-NLS-1$
			Query query = new Query();
			query.setSelect(new Select(Arrays.asList(new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null))));
			query.setFrom(new From(Arrays.asList(new UnaryFromClause(navGroup))));
			
			Criteria criteria = null;
			for (ForeignKey fk:context.getEdmEntityTable().getForeignKeys()) {
				if (fk.getName().equals(navigation.getName())) {
					List<String> lhsColumns = ODataSQLBuilder.getColumnNames(fk.getColumns());
					List<String> rhsColumns = fk.getReferenceColumns();
					for (int i = 0; i < lhsColumns.size(); i++) {
						if (criteria == null) {
							criteria = new CompareCriteria(new ElementSymbol(lhsColumns.get(i), 
									context.getEdmEntityTableGroup()), 
									CompareCriteria.EQ, 
									new ElementSymbol(rhsColumns.get(i), navGroup));
						}
						else {
							Criteria subcriteria = new CompareCriteria(new ElementSymbol(lhsColumns.get(i), 
									context.getEdmEntityTableGroup()), 
									CompareCriteria.EQ, 
									new ElementSymbol(rhsColumns.get(i), navGroup));
							
							criteria = new CompoundCriteria(CompoundCriteria.AND, criteria, subcriteria);
						}
					}
					break;
				}
			}
			query.setCriteria(criteria);
			this.stack.add(new ScalarSubquery(query));
		}
	}

	@Override
	public void visit(Method expr) {
		List<org.teiid.query.sql.symbol.Expression> teiidExprs = new ArrayList<org.teiid.query.sql.symbol.Expression>();
		for (Expression exp:expr.getParameters()) {
			accept(exp);
			teiidExprs.add(this.stack.pop());
		}
		
		switch(expr.getMethod()) {
		case CONTAINS:
			CompareCriteria criteria = new CompareCriteria(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(1),teiidExprs.get(0), new Constant(1)}), CompareCriteria.GE, new Constant(1)); //$NON-NLS-1$
			this.stack.push(criteria);						
			break;
		case STARTSWITH:
			criteria = new CompareCriteria(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0), teiidExprs.get(1), new Constant(1)}), CompareCriteria.EQ, new Constant(1)); //$NON-NLS-1$
			this.stack.push(criteria);			
			break;
		case ENDSWITH:
			criteria = new CompareCriteria(new Function("ENDSWITH", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0), teiidExprs.get(1)}), CompareCriteria.EQ, new Constant(Boolean.TRUE));//$NON-NLS-1$
			this.stack.push(criteria);
			break;
		case LENGTH:
			this.stack.push(new Function("LENGTH", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case INDEXOF:
			stack.push(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(1), teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case SUBSTRING:
			this.stack.push(new Function("SUBSTRING", teiidExprs.toArray(new org.teiid.query.sql.symbol.Expression[teiidExprs.size()]))); //$NON-NLS-1$			
			break;
		case TOLOWER:
			this.stack.push(new Function("LCASE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case TOUPPER:
			this.stack.push(new Function("UCASE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case TRIM:
			this.stack.push(new Function("TRIM", new org.teiid.query.sql.symbol.Expression[] {new Constant("BOTH"), new Constant(' '), teiidExprs.get(0)})); //$NON-NLS-1$ //$NON-NLS-2$
			break;
		case CONCAT:
			this.stack.push(new Function("CONCAT2", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0), teiidExprs.get(1)})); //$NON-NLS-1$
			break;
		case YEAR:
			this.stack.push(new Function("YEAR", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case MONTH:
			this.stack.push(new Function("MONTH", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case DAY:
			this.stack.push(new Function("DAYOFMONTH", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case HOUR:
			this.stack.push(new Function("HOUR", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case MINUTE:
			this.stack.push(new Function("MINUTE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case SECOND:
			this.stack.push(new Function("SECOND", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case NOW:
			this.stack.push(new Function("NOW", new org.teiid.query.sql.symbol.Expression[] {})); //$NON-NLS-1$
			break;
		case ROUND:
			stack.push(new Function("ROUND", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0), new Constant(0)})); //$NON-NLS-1$
			break;
		case FLOOR:
			this.stack.push(new Function("FLOOR", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case CEILING:
			this.stack.push(new Function("CEILING", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0)})); //$NON-NLS-1$
			break;
		case CAST:
			this.stack.push(new Function(CONVERT, new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0),teiidExprs.get(1)}));
			break;
		case FRACTIONALSECONDS:
		case TOTALSECONDS: 
		case DATE: 
		case TIME:
		case TOTALOFFSETMINUTES:
		case MINDATETIME:
		case MAXDATETIME:
		case GEODISTANCE:
		case GEOLENGTH:
		case GEOINTERSECTS:
		case ISOF:
		default:
			this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16027, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16027, expr.getMethod())));
		}
	}

	@Override
	public void visit(TypeLiteral expr) {
		this.stack.push(new Constant(ODataTypeManager.teiidType((SingletonPrimitiveType)expr.getType())));
	}

	@Override
	public void visit(Unary expr) {
		accept(expr.getOperand());
		org.teiid.query.sql.symbol.Expression teiidExpr = this.stack.pop();
		switch(expr.getOperator()) {
		case MINUS:
			this.stack.push(new Function(SourceSystemFunctions.MULTIPLY_OP, new org.teiid.query.sql.symbol.Expression[] {new Constant(-1), teiidExpr}));
			break;
		case NOT:
			this.stack.push(new NotCriteria(new ExpressionCriteria(teiidExpr)));			
			break;
		}
	}
}
