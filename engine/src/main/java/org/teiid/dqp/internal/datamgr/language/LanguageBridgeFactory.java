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

package org.teiid.dqp.internal.datamgr.language;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.client.metadata.ParameterInfo;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.AggregateFunction;
import org.teiid.connector.language.AndOr;
import org.teiid.connector.language.Argument;
import org.teiid.connector.language.BatchedUpdates;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.ColumnReference;
import org.teiid.connector.language.Condition;
import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.DerivedTable;
import org.teiid.connector.language.Exists;
import org.teiid.connector.language.ExpressionValueSource;
import org.teiid.connector.language.In;
import org.teiid.connector.language.InsertValueSource;
import org.teiid.connector.language.IsNull;
import org.teiid.connector.language.Join;
import org.teiid.connector.language.Like;
import org.teiid.connector.language.Literal;
import org.teiid.connector.language.NamedTable;
import org.teiid.connector.language.Not;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.language.SearchedCase;
import org.teiid.connector.language.SearchedWhenClause;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.SortSpecification;
import org.teiid.connector.language.SubqueryComparison;
import org.teiid.connector.language.SubqueryIn;
import org.teiid.connector.language.TableReference;
import org.teiid.connector.language.Argument.Direction;
import org.teiid.connector.language.Comparison.Operator;
import org.teiid.connector.language.SortSpecification.Ordering;
import org.teiid.connector.language.SubqueryComparison.Quantifier;
import org.teiid.connector.metadata.runtime.Procedure;
import org.teiid.connector.metadata.runtime.ProcedureParameter;
import org.teiid.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.OrderByItem;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

public class LanguageBridgeFactory {
    private RuntimeMetadataImpl metadataFactory = null;

    public LanguageBridgeFactory(QueryMetadataInterface metadata) {
        if (metadata != null) {
            metadataFactory = new RuntimeMetadataImpl(metadata);
        }
    }

    public org.teiid.connector.language.Command translate(Command command) {
        if (command == null) return null;
        if (command instanceof Query) {
            return translate((Query)command);
        } else if (command instanceof SetQuery) {
            return translate((SetQuery)command);
        } else if (command instanceof Insert) {
            return translate((Insert)command);
        } else if (command instanceof Update) {
            return translate((Update)command);
        } else if (command instanceof Delete) {
            return translate((Delete)command);
        } else if (command instanceof StoredProcedure) {
            return translate((StoredProcedure)command);
        } else if (command instanceof BatchedUpdateCommand) {
            return translate((BatchedUpdateCommand)command);
        }
        throw new AssertionError();
    }
    
    QueryExpression translate(QueryCommand command) {
    	if (command instanceof Query) {
            return translate((Query)command);
        } 
    	return translate((SetQuery)command);
    }

    org.teiid.connector.language.SetQuery translate(SetQuery union) {
        org.teiid.connector.language.SetQuery result = new org.teiid.connector.language.SetQuery();
        result.setAll(union.isAll());
        switch (union.getOperation()) {
            case UNION:
                result.setOperation(org.teiid.connector.language.SetQuery.Operation.UNION);
                break;
            case INTERSECT:
                result.setOperation(org.teiid.connector.language.SetQuery.Operation.INTERSECT);
                break;
            case EXCEPT:
                result.setOperation(org.teiid.connector.language.SetQuery.Operation.EXCEPT);
                break;
        }
        result.setLeftQuery(translate(union.getLeftQuery()));
        result.setRightQuery(translate(union.getRightQuery()));
        result.setOrderBy(translate(union.getOrderBy()));
        result.setLimit(translate(union.getLimit()));
        return result;
    }

    /* Query */
    Select translate(Query query) {
        List symbols = query.getSelect().getSymbols();
        List<DerivedColumn> translatedSymbols = new ArrayList<DerivedColumn>(symbols.size());
        for (Iterator i = symbols.iterator(); i.hasNext();) {
            SingleElementSymbol symbol = (SingleElementSymbol)i.next();
            String alias = null;
            if(symbol instanceof AliasSymbol) {
                alias = symbol.getOutputName();
                symbol = ((AliasSymbol)symbol).getSymbol();
            }

            org.teiid.connector.language.Expression iExp = null;
            if(symbol instanceof ElementSymbol) {
                iExp = translate((ElementSymbol)symbol);
            } else if(symbol instanceof AggregateSymbol) {
                iExp = translate((AggregateSymbol)symbol);
            } else if(symbol instanceof ExpressionSymbol) {
                iExp = translate(((ExpressionSymbol)symbol).getExpression());
            }

            DerivedColumn selectSymbol = new DerivedColumn(alias, iExp);
            translatedSymbols.add(selectSymbol);
        }
    	List<TableReference> items = null;
    	if (query.getFrom() != null) {
	    	List clauses = query.getFrom().getClauses();
	        items = new ArrayList<TableReference>(clauses.size());
	        for (Iterator i = clauses.iterator(); i.hasNext();) {
	            items.add(translate((FromClause)i.next()));
	        }
    	}
		Select q = new Select(translatedSymbols, query
				.getSelect().isDistinct(), items,
				translate(query.getCriteria()), translate(query.getGroupBy()),
				translate(query.getHaving()), translate(query.getOrderBy()));
        q.setLimit(translate(query.getLimit()));
        return q;
    }

    public TableReference translate(FromClause clause) {
        if (clause == null) return null;
        if (clause instanceof JoinPredicate) {
            return translate((JoinPredicate)clause);
        } else if (clause instanceof SubqueryFromClause) {
            return translate((SubqueryFromClause)clause);
        } else if (clause instanceof UnaryFromClause) {
            return translate((UnaryFromClause)clause);
        }
        throw new AssertionError();
    }

    Join translate(JoinPredicate join) {
        List crits = join.getJoinCriteria();
        Criteria crit = null;
        if (crits.size() == 1) {
        	crit = (Criteria)crits.get(0);
        } else if (crits.size() > 1) {
        	crit = new CompoundCriteria(crits);        	
        }
        
        Join.JoinType joinType = Join.JoinType.INNER_JOIN;
        if(join.getJoinType().equals(JoinType.JOIN_INNER)) {
            joinType = Join.JoinType.INNER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_LEFT_OUTER)) {
            joinType = Join.JoinType.LEFT_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_RIGHT_OUTER)) {
            joinType = Join.JoinType.RIGHT_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_FULL_OUTER)) {
            joinType = Join.JoinType.FULL_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_CROSS)) {
            joinType = Join.JoinType.CROSS_JOIN;
        }
        
        return new Join(translate(join.getLeftClause()),
                            translate(join.getRightClause()),
                            joinType,
                            translate(crit));
    }

    TableReference translate(SubqueryFromClause clause) {        
        return new DerivedTable(translate((QueryCommand)clause.getCommand()), clause.getOutputName());
    }

    NamedTable translate(UnaryFromClause clause) {
        return translate(clause.getGroup());
    }

    public Condition translate(Criteria criteria) {
        if (criteria == null) return null;
        if (criteria instanceof CompareCriteria) {
            return translate((CompareCriteria)criteria);
        } else if (criteria instanceof CompoundCriteria) {
            return translate((CompoundCriteria)criteria);
        } else if (criteria instanceof ExistsCriteria) {
            return translate((ExistsCriteria)criteria);
        } else if (criteria instanceof IsNullCriteria) {
            return translate((IsNullCriteria)criteria);
        }else if (criteria instanceof MatchCriteria) {
            return translate((MatchCriteria)criteria);
        } else if (criteria instanceof NotCriteria) {
            return translate((NotCriteria)criteria);
        } else if (criteria instanceof SetCriteria) {
            return translate((SetCriteria)criteria);
        } else if (criteria instanceof SubqueryCompareCriteria) {
            return translate((SubqueryCompareCriteria)criteria);
        } else if (criteria instanceof SubquerySetCriteria) {
            return translate((SubquerySetCriteria)criteria);
        }
        throw new AssertionError();
    }

    org.teiid.connector.language.Comparison translate(CompareCriteria criteria) {
        Operator operator = Operator.EQ;
        switch(criteria.getOperator()) {
            case CompareCriteria.EQ:    
                operator = Operator.EQ;
                break;
            case CompareCriteria.NE:    
                operator = Operator.NE;
                break;
            case CompareCriteria.LT:    
                operator = Operator.LT;
                break;
            case CompareCriteria.LE:    
                operator = Operator.LE;
                break;
            case CompareCriteria.GT:    
                operator = Operator.GT;
                break;
            case CompareCriteria.GE:    
                operator = Operator.GE;
                break;
            
        }
        
        return new org.teiid.connector.language.Comparison(translate(criteria.getLeftExpression()),
                                        translate(criteria.getRightExpression()), operator);
    }

    AndOr translate(CompoundCriteria criteria) {
        List nestedCriteria = criteria.getCriteria();
        int size = nestedCriteria.size();
        AndOr.Operator op = criteria.getOperator() == CompoundCriteria.AND?AndOr.Operator.AND:AndOr.Operator.OR;
        AndOr result = new AndOr(translate((Criteria)nestedCriteria.get(size - 2)), translate((Criteria)nestedCriteria.get(size - 1)), op);
        for (int i = nestedCriteria.size() - 3; i >= 0; i--) {
        	result = new AndOr(translate((Criteria)nestedCriteria.get(i)), result, op);
        }
        return result;
    }

    Exists translate(ExistsCriteria criteria) {
        return new Exists(translate((QueryCommand)criteria.getCommand()));
    }

    IsNull translate(IsNullCriteria criteria) {
        return new IsNull(translate(criteria.getExpression()), criteria.isNegated());
    }

    Like translate(MatchCriteria criteria) {
        Character escapeChar = null;
        if(criteria.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            escapeChar = new Character(criteria.getEscapeChar());
        }
        return new Like(translate(criteria.getLeftExpression()),
                                    translate(criteria.getRightExpression()), 
                                    escapeChar, 
                                    criteria.isNegated());
    }

    In translate(SetCriteria criteria) {
        List expressions = criteria.getValues();
        List translatedExpressions = new ArrayList();
        for (Iterator i = expressions.iterator(); i.hasNext();) {
            translatedExpressions.add(translate((Expression)i.next()));
        }
        return new In(translate(criteria.getExpression()),
                                  translatedExpressions, 
                                  criteria.isNegated());
    }

    SubqueryComparison translate(SubqueryCompareCriteria criteria) {
        Quantifier quantifier = Quantifier.ALL;
        switch(criteria.getPredicateQuantifier()) {
            case SubqueryCompareCriteria.ALL:   
                quantifier = Quantifier.ALL;
                break;
            case SubqueryCompareCriteria.ANY:
                quantifier = Quantifier.SOME;
                break;
            case SubqueryCompareCriteria.SOME:
                quantifier = Quantifier.SOME;
                break;
        }
        
        Operator operator = Operator.EQ;
        switch(criteria.getOperator()) {
            case SubqueryCompareCriteria.EQ:
                operator = Operator.EQ;
                break;
            case SubqueryCompareCriteria.NE:
                operator = Operator.NE;
                break;
            case SubqueryCompareCriteria.LT:
                operator = Operator.LT;
                break;
            case SubqueryCompareCriteria.LE:
                operator = Operator.LE;
                break;
            case SubqueryCompareCriteria.GT:
                operator = Operator.GT;
                break;
            case SubqueryCompareCriteria.GE:
                operator = Operator.GE;
                break;                    
        }
                
        return new SubqueryComparison(translate(criteria.getLeftExpression()),
                                  operator,
                                  quantifier,
                                  translate((QueryCommand)criteria.getCommand()));
    }

    SubqueryIn translate(SubquerySetCriteria criteria) {
        return new SubqueryIn(translate(criteria.getExpression()),
                                  criteria.isNegated(),
                                  translate((QueryCommand)criteria.getCommand()));
    }

    Not translate(NotCriteria criteria) {
        return new Not(translate(criteria.getCriteria()));
    }

    public org.teiid.connector.language.GroupBy translate(GroupBy groupBy) {
        if(groupBy == null){
            return null;
        }
        List items = groupBy.getSymbols();
        List<org.teiid.connector.language.Expression> translatedItems = new ArrayList<org.teiid.connector.language.Expression>();
        for (Iterator i = items.iterator(); i.hasNext();) {
            translatedItems.add(translate((Expression)i.next()));
        }
        return new org.teiid.connector.language.GroupBy(translatedItems);
    }

    public org.teiid.connector.language.OrderBy translate(OrderBy orderBy) {
        if(orderBy == null){
            return null;
        }
        List<OrderByItem> items = orderBy.getOrderByItems();
        List<SortSpecification> translatedItems = new ArrayList<SortSpecification>();
        for (int i = 0; i < items.size(); i++) {
            SingleElementSymbol symbol = items.get(i).getSymbol();
            Ordering direction = items.get(i).isAscending() ? Ordering.ASC: Ordering.DESC;
            
            SortSpecification orderByItem = null;                                
            if(symbol instanceof AliasSymbol || !items.get(i).isUnrelated()){
            	orderByItem = new SortSpecification(direction, new ColumnReference(null, symbol.getOutputName(), null, symbol.getType()));
            } else {
            	orderByItem = new SortSpecification(direction, translate(symbol));                                
            }
            translatedItems.add(orderByItem);
        }
        return new org.teiid.connector.language.OrderBy(translatedItems);
    }


    /* Expressions */
    public org.teiid.connector.language.Expression translate(Expression expr) {
        if (expr == null) return null;
        if (expr instanceof Constant) {
            return translate((Constant)expr);
        } else if (expr instanceof Function) {
            return translate((Function)expr);
        } else if (expr instanceof ScalarSubquery) {
            return translate((ScalarSubquery)expr);
        } else if (expr instanceof SearchedCaseExpression) {
            return translate((SearchedCaseExpression)expr);
        } else if (expr instanceof SingleElementSymbol) {
            return translate((SingleElementSymbol)expr);
        }
        throw new AssertionError();
    }

    Literal translate(Constant constant) {
        Literal result = new Literal(constant.getValue(), constant.getType());
        result.setBindValue(constant.isMultiValued());
        result.setMultiValued(constant.isMultiValued());
        return result;
    }

    org.teiid.connector.language.Function translate(Function function) {
        Expression [] args = function.getArgs();
        List<org.teiid.connector.language.Expression> params = new ArrayList<org.teiid.connector.language.Expression>(args.length);
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                params.add(translate(args[i]));
            }
        }
        return new org.teiid.connector.language.Function(function.getName(), params, function.getType());
    }

    SearchedCase translate(SearchedCaseExpression expr) {
        ArrayList<SearchedWhenClause> whens = new ArrayList<SearchedWhenClause>();
        for (int i = 0; i < expr.getWhenCount(); i++) {
        	whens.add(new SearchedWhenClause(translate(expr.getWhenCriteria(i)), translate(expr.getThenExpression(i))));
        }
        return new SearchedCase(whens,
                                      translate(expr.getElseExpression()),
                                      expr.getType());
    }


    org.teiid.connector.language.Expression translate(ScalarSubquery ss) {
        return new org.teiid.connector.language.ScalarSubquery(translate((QueryCommand)ss.getCommand()));
    }

    org.teiid.connector.language.Expression translate(SingleElementSymbol symbol) {
        if (symbol == null) return null;
        if (symbol instanceof ElementSymbol) {
            return translate((ElementSymbol)symbol);
        } else if (symbol instanceof AggregateSymbol) {
            return translate((AggregateSymbol)symbol);
        } else if (symbol instanceof ExpressionSymbol) {
            return translate((ExpressionSymbol)symbol);
        }
        throw new AssertionError();
    }

    org.teiid.connector.language.Expression translate(AliasSymbol symbol) {
        return translate(symbol.getSymbol());
    }

    ColumnReference translate(ElementSymbol symbol) {
        ColumnReference element = new ColumnReference(translate(symbol.getGroupSymbol()), symbol.getOutputName(), null, symbol.getType());
        if (element.getTable().getMetadataObject() == null) {
            return element;
        }

        Object mid = symbol.getMetadataID();
        
        if(! (mid instanceof TempMetadataID)) { 
            element.setMetadataObject(metadataFactory.getElement(mid));
        }
        return element;
    }

    AggregateFunction translate(AggregateSymbol symbol) {
        return new AggregateFunction(symbol.getAggregateFunction(), 
                                symbol.isDistinct(), 
                                translate(symbol.getExpression()),
                                symbol.getType());
    }

    org.teiid.connector.language.Expression translate(ExpressionSymbol symbol) {
        return translate(symbol.getExpression());
    }


    /* Insert */
    org.teiid.connector.language.Insert translate(Insert insert) {
        List elements = insert.getVariables();
        List<ColumnReference> translatedElements = new ArrayList<ColumnReference>();
        for (Iterator i = elements.iterator(); i.hasNext();) {
            translatedElements.add(translate((ElementSymbol)i.next()));
        }
        
        InsertValueSource valueSource = null;
        if (insert.getQueryExpression() != null) {
        	valueSource = translate(insert.getQueryExpression());
        } else {
            // This is for the simple one row insert.
            List values = insert.getValues();
            List<org.teiid.connector.language.Expression> translatedValues = new ArrayList<org.teiid.connector.language.Expression>();
            for (Iterator i = values.iterator(); i.hasNext();) {
                translatedValues.add(translate((Expression)i.next()));
            }
            valueSource = new ExpressionValueSource(translatedValues);
        }
        
        return new org.teiid.connector.language.Insert(translate(insert.getGroup()),
                              translatedElements,
                              valueSource);
    }

    /* Update */
    org.teiid.connector.language.Update translate(Update update) {
        return new org.teiid.connector.language.Update(translate(update.getGroup()),
                              translate(update.getChangeList()),
                              translate(update.getCriteria()));
    }
    
    List<org.teiid.connector.language.SetClause> translate(SetClauseList setClauseList) {
    	List<org.teiid.connector.language.SetClause> clauses = new ArrayList<org.teiid.connector.language.SetClause>(setClauseList.getClauses().size());
    	for (SetClause setClause : setClauseList.getClauses()) {
    		clauses.add(translate(setClause));
    	}
    	return clauses;
    }
    
    org.teiid.connector.language.SetClause translate(SetClause setClause) {
    	return new org.teiid.connector.language.SetClause(translate(setClause.getSymbol()), translate(setClause.getValue()));
    }

    /* Delete */
    org.teiid.connector.language.Delete translate(Delete delete) {
        org.teiid.connector.language.Delete deleteImpl = new org.teiid.connector.language.Delete(translate(delete.getGroup()),
                              translate(delete.getCriteria()));
        return deleteImpl;
    }

    /* Execute */
    Call translate(StoredProcedure sp) {
        Procedure proc = null;
        if(sp.getProcedureID() != null) {
            try {
				proc = this.metadataFactory.getProcedure(sp.getGroup().getName());
			} catch (ConnectorException e) {
				throw new MetaMatrixRuntimeException(e);
			}
        }
        Class<?> returnType = null;
        List parameters = sp.getParameters();
        List<Argument> translatedParameters = new ArrayList<Argument>();
        for (Iterator i = parameters.iterator(); i.hasNext();) {
        	SPParameter param = (SPParameter)i.next();
        	Direction direction = Direction.IN;
            switch(param.getParameterType()) {
                case ParameterInfo.IN:    
                    direction = Direction.IN;
                    break;
                case ParameterInfo.INOUT: 
                    direction = Direction.INOUT;
                    break;
                case ParameterInfo.OUT: 
                    direction = Direction.OUT;
                    break;
                case ParameterInfo.RESULT_SET: 
                    continue; //already part of the metadata
                case ParameterInfo.RETURN_VALUE: 
                	returnType = param.getClassType();
                	break;
                    
            }
            
            ProcedureParameter metadataParam = metadataFactory.getParameter(param);
            //we can assume for now that all arguments will be literals, which may be multivalued
            Literal value = (Literal)translate(param.getExpression());
            Argument arg = new Argument(direction, value, param.getClassType(), metadataParam);
            translatedParameters.add(arg);
        }
                        
        Call call = new Call(removeSchemaName(sp.getProcedureName()), translatedParameters, proc);
        call.setReturnType(returnType);
        return call;
    }

    public NamedTable translate(GroupSymbol symbol) {
    	String alias = null;
        String fullGroup = symbol.getOutputName();
        if(symbol.getOutputDefinition() != null) {
            alias = symbol.getOutputName();
            fullGroup = symbol.getOutputDefinition();
        }
        fullGroup = removeSchemaName(fullGroup);
        NamedTable group = new NamedTable(fullGroup, alias, null);
		if (symbol.getMetadataID() instanceof TempMetadataID) {
			return group;
		}
        try {
			group.setMetadataObject(metadataFactory.getGroup(symbol.getMetadataID()));
		} catch (QueryMetadataException e) {
			throw new MetaMatrixRuntimeException(e);
		} catch (MetaMatrixComponentException e) {
			throw new MetaMatrixRuntimeException(e);
		}
        return group;
    }

	private String removeSchemaName(String fullGroup) {
		//remove the model name
        int index = fullGroup.indexOf(ElementSymbol.SEPARATOR);
        if (index > 0) {
        	fullGroup = fullGroup.substring(index + 1);
        }
		return fullGroup;
	}
    
    /* Batched Updates */
    BatchedUpdates translate(BatchedUpdateCommand command) {
        List updates = command.getUpdateCommands();
        List<org.teiid.connector.language.Command> translatedUpdates = new ArrayList<org.teiid.connector.language.Command>(updates.size());
        for (Iterator i = updates.iterator(); i.hasNext();) {
            translatedUpdates.add(translate((Command)i.next()));
        }
        return new BatchedUpdates(translatedUpdates);
    }

    org.teiid.connector.language.Limit translate(Limit limit) {
        if (limit == null) {
            return null;
        }
        int rowOffset = 0;
        if (limit.getOffset() != null) {
            Literal c1 = (Literal)translate(limit.getOffset());
            rowOffset = ((Integer)c1.getValue()).intValue();
        }
        Literal c2 = (Literal)translate(limit.getRowLimit());
        int rowLimit = ((Integer)c2.getValue()).intValue();
        return new org.teiid.connector.language.Limit(rowOffset, rowLimit);
    }
}
