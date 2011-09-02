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

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.Argument;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.DerivedTable;
import org.teiid.language.Exists;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.In;
import org.teiid.language.InsertValueSource;
import org.teiid.language.IsNull;
import org.teiid.language.IteratorValueSource;
import org.teiid.language.Join;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Not;
import org.teiid.language.QueryExpression;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SortSpecification;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryIn;
import org.teiid.language.TableReference;
import org.teiid.language.WindowSpecification;
import org.teiid.language.With;
import org.teiid.language.WithItem;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.SubqueryComparison.Quantifier;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.SelectSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.translator.TranslatorException;


public class LanguageBridgeFactory {
    private RuntimeMetadataImpl metadataFactory = null;

    public LanguageBridgeFactory(QueryMetadataInterface metadata) {
        if (metadata != null) {
            metadataFactory = new RuntimeMetadataImpl(metadata);
        }
    }

    public org.teiid.language.Command translate(Command command) {
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

    org.teiid.language.SetQuery translate(SetQuery union) {
        org.teiid.language.SetQuery result = new org.teiid.language.SetQuery();
        result.setAll(union.isAll());
        switch (union.getOperation()) {
            case UNION:
                result.setOperation(org.teiid.language.SetQuery.Operation.UNION);
                break;
            case INTERSECT:
                result.setOperation(org.teiid.language.SetQuery.Operation.INTERSECT);
                break;
            case EXCEPT:
                result.setOperation(org.teiid.language.SetQuery.Operation.EXCEPT);
                break;
        }
        result.setLeftQuery(translate(union.getLeftQuery()));
        result.setRightQuery(translate(union.getRightQuery()));
        result.setOrderBy(translate(union.getOrderBy(), true));
        result.setLimit(translate(union.getLimit()));
        result.setWith(translate(union.getWith()));
        return result;
    }

    /* Query */
    Select translate(Query query) {
        List<SelectSymbol> symbols = query.getSelect().getSymbols();
        List<DerivedColumn> translatedSymbols = new ArrayList<DerivedColumn>(symbols.size());
        for (Iterator<SelectSymbol> i = symbols.iterator(); i.hasNext();) {
            SingleElementSymbol symbol = (SingleElementSymbol)i.next();
            String alias = null;
            if(symbol instanceof AliasSymbol) {
                alias = symbol.getOutputName();
                symbol = ((AliasSymbol)symbol).getSymbol();
            }

            org.teiid.language.Expression iExp = translate(symbol);

            DerivedColumn selectSymbol = new DerivedColumn(alias, iExp);
            translatedSymbols.add(selectSymbol);
        }
    	List<TableReference> items = null;
    	if (query.getFrom() != null) {
	    	List<FromClause> clauses = query.getFrom().getClauses();
	        items = new ArrayList<TableReference>(clauses.size());
	        for (Iterator<FromClause> i = clauses.iterator(); i.hasNext();) {
	            items.add(translate(i.next()));
	        }
    	}
		Select q = new Select(translatedSymbols, query
				.getSelect().isDistinct(), items,
				translate(query.getCriteria()), translate(query.getGroupBy()),
				translate(query.getHaving()), translate(query.getOrderBy(), false));
        q.setLimit(translate(query.getLimit()));
        q.setWith(translate(query.getWith()));
        return q;
    }
    
    public With translate(List<WithQueryCommand> with) {
    	if (with == null || with.isEmpty()) {
    		return null;
    	}
    	With result = new With();
    	ArrayList<WithItem> items = new ArrayList<WithItem>(with.size());
    	for (WithQueryCommand withQueryCommand : with) {
			WithItem item = new WithItem();
			item.setTable(translate(withQueryCommand.getGroupSymbol()));
			if (withQueryCommand.getColumns() != null) {
				List<ColumnReference> translatedElements = new ArrayList<ColumnReference>(withQueryCommand.getColumns().size());
		        for (ElementSymbol es: withQueryCommand.getColumns()) {
		            translatedElements.add(translate(es));
		        }
			}
			item.setSubquery(translate(withQueryCommand.getCommand()));
			items.add(item);
		}
    	result.setItems(items);
    	return result;
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

    org.teiid.language.Comparison translate(CompareCriteria criteria) {
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
        
        return new org.teiid.language.Comparison(translate(criteria.getLeftExpression()),
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

    Condition translate(ExistsCriteria criteria) {
        Exists exists = new Exists(translate(criteria.getCommand()));
        if (criteria.isNegated()) {
        	return new Not(exists);
        }
        return exists;
    }

    IsNull translate(IsNullCriteria criteria) {
        return new IsNull(translate(criteria.getExpression()), criteria.isNegated());
    }

    Like translate(MatchCriteria criteria) {
        Character escapeChar = null;
        if(criteria.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            escapeChar = new Character(criteria.getEscapeChar());
        }
        Like like = new Like(translate(criteria.getLeftExpression()),
                                    translate(criteria.getRightExpression()), 
                                    escapeChar, 
                                    criteria.isNegated());
        like.setMode(criteria.getMode());
        return like;
    }

    In translate(SetCriteria criteria) {
        Collection expressions = criteria.getValues();
        List<org.teiid.language.Expression> translatedExpressions = new ArrayList<org.teiid.language.Expression>();
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
                                  translate(criteria.getCommand()));
    }

    SubqueryIn translate(SubquerySetCriteria criteria) {
        return new SubqueryIn(translate(criteria.getExpression()),
                                  criteria.isNegated(),
                                  translate(criteria.getCommand()));
    }

    Not translate(NotCriteria criteria) {
        return new Not(translate(criteria.getCriteria()));
    }

    public org.teiid.language.GroupBy translate(GroupBy groupBy) {
        if(groupBy == null){
            return null;
        }
        List items = groupBy.getSymbols();
        List<org.teiid.language.Expression> translatedItems = new ArrayList<org.teiid.language.Expression>();
        for (Iterator i = items.iterator(); i.hasNext();) {
            translatedItems.add(translate((Expression)i.next()));
        }
        return new org.teiid.language.GroupBy(translatedItems);
    }

    public org.teiid.language.OrderBy translate(OrderBy orderBy, boolean set) {
        if(orderBy == null){
            return null;
        }
        List<OrderByItem> items = orderBy.getOrderByItems();
        List<SortSpecification> translatedItems = new ArrayList<SortSpecification>();
        for (int i = 0; i < items.size(); i++) {
            SingleElementSymbol symbol = items.get(i).getSymbol();
            Ordering direction = items.get(i).isAscending() ? Ordering.ASC: Ordering.DESC;
            
            SortSpecification orderByItem = null;                                
            if(!set && (items.get(i).isUnrelated() || symbol instanceof ElementSymbol)){
            	orderByItem = new SortSpecification(direction, translate(symbol));                                
            } else {
            	orderByItem = new SortSpecification(direction, new ColumnReference(null, SingleElementSymbol.getShortName(symbol.getOutputName()), null, symbol.getType()));
            }
            orderByItem.setNullOrdering(items.get(i).getNullOrdering());
            translatedItems.add(orderByItem);
        }
        return new org.teiid.language.OrderBy(translatedItems);
    }


    /* Expressions */
    public org.teiid.language.Expression translate(Expression expr) {
        if (expr == null) return null;
        if (expr instanceof Constant) {
            return translate((Constant)expr);
        } else if (expr instanceof Function) {
            return translate((Function)expr);
        } else if (expr instanceof ScalarSubquery) {
            return translate((ScalarSubquery)expr);
        } else if (expr instanceof SearchedCaseExpression) {
            return translate((SearchedCaseExpression)expr);
        } else if (expr instanceof ElementSymbol) {
            return translate((ElementSymbol)expr);
        } else if (expr instanceof AggregateSymbol) {
            return translate((AggregateSymbol)expr);
        } else if (expr instanceof ExpressionSymbol) {
            return translate((ExpressionSymbol)expr);
        } else if (expr instanceof Criteria) {
        	return translate((Criteria)expr);
        } else if (expr instanceof WindowFunction) {
        	return translate((WindowFunction)expr);
        }
        throw new AssertionError();
    }
    
    org.teiid.language.WindowFunction translate(WindowFunction windowFunction) {
    	org.teiid.language.WindowFunction result = new org.teiid.language.WindowFunction();
    	result.setFunction(translate(windowFunction.getFunction()));
    	WindowSpecification ws = new WindowSpecification();
    	ws.setOrderBy(translate(windowFunction.getWindowSpecification().getOrderBy(), false));
    	List<Expression> partition = windowFunction.getWindowSpecification().getPartition();
    	if (partition != null) {
	    	ArrayList<org.teiid.language.Expression> partitionList = new ArrayList<org.teiid.language.Expression>(partition.size());
	    	for (Expression ex : partition) {
	    		partitionList.add(translate(ex));
	    	}
	    	ws.setPartition(partitionList);
    	}
    	result.setWindowSpecification(ws);
    	return result;
    }

    Literal translate(Constant constant) {
        Literal result = new Literal(constant.getValue(), constant.getType());
        result.setBindValue(constant.isMultiValued());
        result.setMultiValued(constant.isMultiValued());
        result.setBindEligible(constant.isBindEligible());
        return result;
    }

    org.teiid.language.Function translate(Function function) {
        Expression [] args = function.getArgs();
        List<org.teiid.language.Expression> params = new ArrayList<org.teiid.language.Expression>(args.length);
        for (int i = 0; i < args.length; i++) {
            params.add(translate(args[i]));
        }
        String name = function.getName();
        if (function.getFunctionDescriptor() != null) {
        	name = function.getFunctionDescriptor().getName();
        	//check for translator pushdown functions, and use the name in source if possible
        	if (function.getFunctionDescriptor().getPushdown() == PushDown.MUST_PUSHDOWN 
        			&& function.getFunctionDescriptor().getSchema().equalsIgnoreCase(CoreConstants.SYSTEM_MODEL)
        			&& function.getFunctionDescriptor().getMethod().getNameInSource() != null) {
        		name = function.getFunctionDescriptor().getMethod().getNameInSource();
        	}
        } else {
        	name = SingleElementSymbol.getShortName(name);
        }

        //if there is any ambiguity in the function name it will be up to the translator logic to check the 
        //metadata
        org.teiid.language.Function result = new org.teiid.language.Function(name, params, function.getType());
        if (function.getFunctionDescriptor() != null) {
        	result.setMetadataObject(function.getFunctionDescriptor().getMethod());
        }
        return result;
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


    org.teiid.language.Expression translate(ScalarSubquery ss) {
        return new org.teiid.language.ScalarSubquery(translate(ss.getCommand()));
    }

    org.teiid.language.Expression translate(AliasSymbol symbol) {
        return translate(symbol.getSymbol());
    }

    ColumnReference translate(ElementSymbol symbol) {
        ColumnReference element = new ColumnReference(translate(symbol.getGroupSymbol()), SingleElementSymbol.getShortName(symbol.getOutputName()), null, symbol.getType());
        if (element.getTable().getMetadataObject() == null) {
            return element;
        }

        Object mid = symbol.getMetadataID();
        
        element.setMetadataObject(metadataFactory.getElement(mid));
        return element;
    }

    AggregateFunction translate(AggregateSymbol symbol) {
    	AggregateFunction af = new AggregateFunction(symbol.getAggregateFunction().name(), 
                                symbol.isDistinct(), 
                                translate(symbol.getExpression()),
                                symbol.getType());
    	af.setCondition(translate(symbol.getCondition()));
    	return af;
    }

    org.teiid.language.Expression translate(ExpressionSymbol symbol) {
        return translate(symbol.getExpression());
    }

    /* Insert */
    org.teiid.language.Insert translate(Insert insert) {
        List<ElementSymbol> elements = insert.getVariables();
        List<ColumnReference> translatedElements = new ArrayList<ColumnReference>();

        for (ElementSymbol elementSymbol : elements) {
            translatedElements.add(translate(elementSymbol));
		}
        
        InsertValueSource valueSource = null;
        if (insert.getQueryExpression() != null) {
        	valueSource = translate(insert.getQueryExpression());
        } else if (insert.getTupleSource() != null) {
        	final TupleSource ts = insert.getTupleSource();
        	valueSource = new IteratorValueSource<List<?>>(new Iterator<List<?>>() {
        		
        		List<?> next;
				
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
				@Override
				public List<?> next() {
					if (hasNext()) {
						List<?> result = next;
						next = null;
						return result;
					}
					throw new NoSuchElementException();
				}
				
				@Override
				public boolean hasNext() {
					if (next != null) {
						return true;
					}
					try {
						next = ts.nextTuple();
					} catch (TeiidException e) {
						throw new TeiidRuntimeException(e);
					}
					return next != null;
				}
			}, elements.size());
        } else {
            // This is for the simple one row insert.
            List values = insert.getValues();
            List<org.teiid.language.Expression> translatedValues = new ArrayList<org.teiid.language.Expression>();
            for (Iterator i = values.iterator(); i.hasNext();) {
                translatedValues.add(translate((Expression)i.next()));
            }
            valueSource = new ExpressionValueSource(translatedValues);
        }
        
        return new org.teiid.language.Insert(translate(insert.getGroup()),
                              translatedElements,
                              valueSource);
    }

    /* Update */
    org.teiid.language.Update translate(Update update) {
        return new org.teiid.language.Update(translate(update.getGroup()),
                              translate(update.getChangeList()),
                              translate(update.getCriteria()));
    }
    
    List<org.teiid.language.SetClause> translate(SetClauseList setClauseList) {
    	List<org.teiid.language.SetClause> clauses = new ArrayList<org.teiid.language.SetClause>(setClauseList.getClauses().size());
    	for (SetClause setClause : setClauseList.getClauses()) {
    		clauses.add(translate(setClause));
    	}
    	return clauses;
    }
    
    org.teiid.language.SetClause translate(SetClause setClause) {
    	return new org.teiid.language.SetClause(translate(setClause.getSymbol()), translate(setClause.getValue()));
    }

    /* Delete */
    org.teiid.language.Delete translate(Delete delete) {
        org.teiid.language.Delete deleteImpl = new org.teiid.language.Delete(translate(delete.getGroup()),
                              translate(delete.getCriteria()));
        return deleteImpl;
    }

    /* Execute */
    Call translate(StoredProcedure sp) {
        Procedure proc = null;
        if(sp.getProcedureID() != null) {
            try {
				proc = this.metadataFactory.getProcedure(sp.getGroup().getName());
			} catch (TranslatorException e) {
				throw new TeiidRuntimeException(e);
			}
        }
        Class<?> returnType = null;
        List<Argument> translatedParameters = new ArrayList<Argument>();
        for (SPParameter param : sp.getParameters()) {
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
                	continue;
            }
            
            ProcedureParameter metadataParam = metadataFactory.getParameter(param);
            //we can assume for now that all arguments will be literals, which may be multivalued
            Literal value = null;
            if (direction != Direction.OUT) {
            	value = (Literal)translate(param.getExpression());
            }
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
        try {
			group.setMetadataObject(metadataFactory.getGroup(symbol.getMetadataID()));
		} catch (QueryMetadataException e) {
			throw new TeiidRuntimeException(e);
		} catch (TeiidComponentException e) {
			throw new TeiidRuntimeException(e);
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
        List<Command> updates = command.getUpdateCommands();
        List<org.teiid.language.Command> translatedUpdates = new ArrayList<org.teiid.language.Command>(updates.size());
        for (Iterator<Command> i = updates.iterator(); i.hasNext();) {
            translatedUpdates.add(translate(i.next()));
        }
        return new BatchedUpdates(translatedUpdates);
    }

    org.teiid.language.Limit translate(Limit limit) {
        if (limit == null) {
            return null;
        }
        int rowOffset = 0;
        if (limit.getOffset() != null) {
            Literal c1 = (Literal)translate(limit.getOffset());
            rowOffset = ((Integer)c1.getValue()).intValue();
        }
        Literal c2 = (Literal)translate(limit.getRowLimit());
        int rowLimit = Integer.MAX_VALUE;
        if (c2 != null) {
        	rowLimit = ((Integer)c2.getValue()).intValue();
        }
        return new org.teiid.language.Limit(rowOffset, rowLimit);
    }
}
