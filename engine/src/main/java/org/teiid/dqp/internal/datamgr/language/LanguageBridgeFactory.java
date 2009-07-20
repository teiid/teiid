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

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICompoundCriteria;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IFromItem;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IGroupBy;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertValueSource;
import org.teiid.connector.language.IIsNullCriteria;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.INotCriteria;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISetClause;
import org.teiid.connector.language.ISetClauseList;
import org.teiid.connector.language.ISetQuery;
import org.teiid.connector.language.ISubqueryCompareCriteria;
import org.teiid.connector.language.ISubqueryInCriteria;
import org.teiid.connector.language.IUpdate;
import org.teiid.connector.language.ICompareCriteria.Operator;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.language.ISubqueryCompareCriteria.Quantifier;
import org.teiid.connector.metadata.runtime.Parameter;
import org.teiid.connector.metadata.runtime.Procedure;
import org.teiid.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.From;
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
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
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

    public ICommand translate(Command command) throws MetaMatrixComponentException {
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
    
    IQueryCommand translate(QueryCommand command) throws MetaMatrixComponentException {
    	if (command instanceof Query) {
            return translate((Query)command);
        } 
    	return translate((SetQuery)command);
    }

    ISetQuery translate(SetQuery union) throws MetaMatrixComponentException {
        SetQueryImpl result = new SetQueryImpl();
        result.setAll(union.isAll());
        switch (union.getOperation()) {
            case UNION:
                result.setOperation(ISetQuery.Operation.UNION);
                break;
            case INTERSECT:
                result.setOperation(ISetQuery.Operation.INTERSECT);
                break;
            case EXCEPT:
                result.setOperation(ISetQuery.Operation.EXCEPT);
                break;
        }
        result.setLeftQuery(translate(union.getLeftQuery()));
        result.setRightQuery(translate(union.getRightQuery()));
        result.setOrderBy(translate(union.getOrderBy()));
        result.setLimit(translate(union.getLimit()));
        return result;
    }

    /* Query */
    IQuery translate(Query query) throws MetaMatrixComponentException {
        QueryImpl q = new QueryImpl(translate(query.getSelect()),
                             translate(query.getFrom()),
                             translate(query.getCriteria()),
                             translate(query.getGroupBy()),
                             translate(query.getHaving()),
                             translate(query.getOrderBy()));
        q.setLimit(translate(query.getLimit()));
        return q;
    }

    public ISelect translate(Select select) throws MetaMatrixComponentException {
        List symbols = select.getSymbols();
        List translatedSymbols = new ArrayList(symbols.size());
        for (Iterator i = symbols.iterator(); i.hasNext();) {
            SingleElementSymbol symbol = (SingleElementSymbol)i.next();
            boolean isAlias = false;
            String alias = symbol.getOutputName();
            if(symbol instanceof AliasSymbol) {
                symbol = ((AliasSymbol)symbol).getSymbol();
                isAlias = true;
            }

            IExpression iExp = null;
            if(symbol instanceof ElementSymbol) {
                iExp = translate((ElementSymbol)symbol);
            } else if(symbol instanceof AggregateSymbol) {
                iExp = translate((AggregateSymbol)symbol);
            } else if(symbol instanceof ExpressionSymbol) {
                iExp = translate(((ExpressionSymbol)symbol).getExpression());
            }

            SelectSymbolImpl selectSymbol = new SelectSymbolImpl(alias, iExp);
            if(isAlias){
                selectSymbol.setOutputName(alias);
                selectSymbol.setAlias(true);
            }
            translatedSymbols.add(selectSymbol);
        }
        return new SelectImpl(translatedSymbols, select.isDistinct());
    }
    
    public IFrom translate(From from) throws MetaMatrixComponentException {
        List clauses = from.getClauses();
        List items = new ArrayList();
        for (Iterator i = clauses.iterator(); i.hasNext();) {
            items.add(translate((FromClause)i.next()));
        }
        return new FromImpl(items);
    }

    public IFromItem translate(FromClause clause) throws MetaMatrixComponentException {
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

    IJoin translate(JoinPredicate join) throws MetaMatrixComponentException {
        List crits = join.getJoinCriteria();
        List criteria = new ArrayList();
        for (Iterator i = crits.iterator(); i.hasNext();) {
            criteria.add(translate((Criteria)i.next()));
        }
        
        IJoin.JoinType joinType = IJoin.JoinType.INNER_JOIN;
        if(join.getJoinType().equals(JoinType.JOIN_INNER)) {
            joinType = IJoin.JoinType.INNER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_LEFT_OUTER)) {
            joinType = IJoin.JoinType.LEFT_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_RIGHT_OUTER)) {
            joinType = IJoin.JoinType.RIGHT_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_FULL_OUTER)) {
            joinType = IJoin.JoinType.FULL_OUTER_JOIN;
        } else if(join.getJoinType().equals(JoinType.JOIN_CROSS)) {
            joinType = IJoin.JoinType.CROSS_JOIN;
        }
        
        return new JoinImpl(translate(join.getLeftClause()),
                            translate(join.getRightClause()),
                            joinType,
                            criteria);
    }

    IFromItem translate(SubqueryFromClause clause) throws MetaMatrixComponentException {        
        return new InlineViewImpl(translate((QueryCommand)clause.getCommand()), clause.getOutputName());
    }

    IGroup translate(UnaryFromClause clause) throws MetaMatrixComponentException {
        return translate(clause.getGroup());
    }

    public ICriteria translate(Criteria criteria) throws MetaMatrixComponentException {
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

    ICompareCriteria translate(CompareCriteria criteria) throws MetaMatrixComponentException {
        ICompareCriteria.Operator operator = Operator.EQ;
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
        
        return new CompareCriteriaImpl(translate(criteria.getLeftExpression()),
                                        translate(criteria.getRightExpression()), operator);
    }

    ICompoundCriteria translate(CompoundCriteria criteria) throws MetaMatrixComponentException {
        List nestedCriteria = criteria.getCriteria();
        List translatedCriteria = new ArrayList();
        for (Iterator i = nestedCriteria.iterator(); i.hasNext();) {
            translatedCriteria.add(translate((Criteria)i.next()));
        }
        
        return new CompoundCriteriaImpl(translatedCriteria, criteria.getOperator() == CompoundCriteria.AND?ICompoundCriteria.Operator.AND:ICompoundCriteria.Operator.OR);
    }

    IExistsCriteria translate(ExistsCriteria criteria) throws MetaMatrixComponentException {
        return new ExistsCriteriaImpl(translate((Query)criteria.getCommand()));
    }

    IIsNullCriteria translate(IsNullCriteria criteria) throws MetaMatrixComponentException {
        return new IsNullCriteriaImpl(translate(criteria.getExpression()), criteria.isNegated());
    }

    ILikeCriteria translate(MatchCriteria criteria) throws MetaMatrixComponentException {
        Character escapeChar = null;
        if(criteria.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            escapeChar = new Character(criteria.getEscapeChar());
        }
        return new LikeCriteriaImpl(translate(criteria.getLeftExpression()),
                                    translate(criteria.getRightExpression()), 
                                    escapeChar, 
                                    criteria.isNegated());
    }

    IInCriteria translate(SetCriteria criteria) throws MetaMatrixComponentException {
        List expressions = criteria.getValues();
        List translatedExpressions = new ArrayList();
        for (Iterator i = expressions.iterator(); i.hasNext();) {
            translatedExpressions.add(translate((Expression)i.next()));
        }
        return new InCriteriaImpl(translate(criteria.getExpression()),
                                  translatedExpressions, 
                                  criteria.isNegated());
    }

    ISubqueryCompareCriteria translate(SubqueryCompareCriteria criteria) throws MetaMatrixComponentException {
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
        
        ICompareCriteria.Operator operator = ICompareCriteria.Operator.EQ;
        switch(criteria.getOperator()) {
            case SubqueryCompareCriteria.EQ:
                operator = ICompareCriteria.Operator.EQ;
                break;
            case SubqueryCompareCriteria.NE:
                operator = ICompareCriteria.Operator.NE;
                break;
            case SubqueryCompareCriteria.LT:
                operator = ICompareCriteria.Operator.LT;
                break;
            case SubqueryCompareCriteria.LE:
                operator = ICompareCriteria.Operator.LE;
                break;
            case SubqueryCompareCriteria.GT:
                operator = ICompareCriteria.Operator.GT;
                break;
            case SubqueryCompareCriteria.GE:
                operator = ICompareCriteria.Operator.GE;
                break;                    
        }
                
        return new SubqueryCompareCriteriaImpl(translate(criteria.getLeftExpression()),
                                  operator,
                                  quantifier,
                                  translate((Query)criteria.getCommand()));
    }

    ISubqueryInCriteria translate(SubquerySetCriteria criteria) throws MetaMatrixComponentException {
        return new SubqueryInCriteriaImpl(translate(criteria.getExpression()),
                                  criteria.isNegated(),
                                  translate((Query)criteria.getCommand()));
    }

    INotCriteria translate(NotCriteria criteria) throws MetaMatrixComponentException {
        return new NotCriteriaImpl(translate(criteria.getCriteria()));
    }

    public IGroupBy translate(GroupBy groupBy) throws MetaMatrixComponentException {
        if(groupBy == null){
            return null;
        }
        List items = groupBy.getSymbols();
        List translatedItems = new ArrayList();
        for (Iterator i = items.iterator(); i.hasNext();) {
            translatedItems.add(translate((Expression)i.next()));
        }
        return new GroupByImpl(translatedItems);
    }

    public IOrderBy translate(OrderBy orderBy) throws MetaMatrixComponentException {
        if(orderBy == null){
            return null;
        }
        List items = orderBy.getVariables();
        List types = orderBy.getTypes();
        List translatedItems = new ArrayList();
        for (int i = 0; i < items.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol)items.get(i);
            boolean direction = (((Boolean)types.get(i)).booleanValue() == OrderBy.DESC)
                                ? IOrderByItem.DESC
                                : IOrderByItem.ASC;
                                
            OrderByItemImpl orderByItem = null;                                
            if(symbol instanceof ElementSymbol){
                IElement innerElement = translate((ElementSymbol)symbol);
                if (symbol.getOutputName() != null && symbol.getOutputName().indexOf(ElementSymbol.SEPARATOR) != -1) {
                	orderByItem = new OrderByItemImpl(null, direction, innerElement);
                } else {
                	orderByItem = new OrderByItemImpl(symbol.getOutputName(), direction, innerElement);
                }
            } else {
                orderByItem = new OrderByItemImpl(symbol.getOutputName(), direction, null);                
            }
            translatedItems.add(orderByItem);
        }
        return new OrderByImpl(translatedItems);
    }


    /* Expressions */
    public IExpression translate(Expression expr) throws MetaMatrixComponentException {
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

    ILiteral translate(Constant constant) {
        LiteralImpl result = new LiteralImpl(constant.getValue(), constant.getType());
        result.setBindValue(constant.isMultiValued());
        result.setMultiValued(constant.isMultiValued());
        return result;
    }

    IFunction translate(Function function) throws MetaMatrixComponentException {
        Expression [] args = function.getArgs();
        List<IExpression> params = new ArrayList<IExpression>(args.length);
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                params.add(translate(args[i]));
            }
        }
        return new FunctionImpl(function.getName(), params, function.getType());
    }

    ISearchedCaseExpression translate(SearchedCaseExpression expr) throws MetaMatrixComponentException {
        ArrayList whens = new ArrayList(), thens = new ArrayList();
        for (int i = 0; i < expr.getWhenCount(); i++) {
            whens.add(translate(expr.getWhenCriteria(i)));
            thens.add(translate(expr.getThenExpression(i)));
        }
        return new SearchedCaseExpressionImpl(whens,
                                      thens,
                                      translate(expr.getElseExpression()),
                                      expr.getType());
    }


    IExpression translate(ScalarSubquery ss) throws MetaMatrixComponentException {
        return new ScalarSubqueryImpl(translate((Query)ss.getCommand()));
    }

    IExpression translate(SingleElementSymbol symbol) throws MetaMatrixComponentException {
        if (symbol == null) return null;
        if (symbol instanceof AliasSymbol) {
            return translate((AliasSymbol)symbol);
        } else if (symbol instanceof ElementSymbol) {
            return translate((ElementSymbol)symbol);
        } else if (symbol instanceof AggregateSymbol) {
            return translate((AggregateSymbol)symbol);
        } else if (symbol instanceof ExpressionSymbol) {
            return translate((ExpressionSymbol)symbol);
        }
        throw new AssertionError();
    }

    IExpression translate(AliasSymbol symbol) throws MetaMatrixComponentException {
        return translate(symbol.getSymbol());
    }

    IElement translate(ElementSymbol symbol) throws MetaMatrixComponentException {
        ElementImpl element = null;
        element = new ElementImpl(translate(symbol.getGroupSymbol()), symbol.getOutputName(), null, symbol.getType());
        
        if (element.getGroup().getMetadataObject() == null) {
            return element;
        }

        Object mid = symbol.getMetadataID();
        
        if(! (mid instanceof TempMetadataID)) { 
            element.setMetadataObject(metadataFactory.getElement(mid));
        }
        return element;
    }

    IAggregate translate(AggregateSymbol symbol) throws MetaMatrixComponentException {
        return new AggregateImpl(symbol.getAggregateFunction(), 
                                symbol.isDistinct(), 
                                translate(symbol.getExpression()),
                                symbol.getType());
    }

    IExpression translate(ExpressionSymbol symbol) throws MetaMatrixComponentException {
        return translate(symbol.getExpression());
    }


    /* Insert */
    IInsert translate(Insert insert) throws MetaMatrixComponentException {
        List elements = insert.getVariables();
        List translatedElements = new ArrayList();
        for (Iterator i = elements.iterator(); i.hasNext();) {
            translatedElements.add(translate((ElementSymbol)i.next()));
        }
        
        IInsertValueSource valueSource = null;
        if (insert.getQueryExpression() != null) {
        	valueSource = translate(insert.getQueryExpression());
        } else {
            // This is for the simple one row insert.
            List values = insert.getValues();
            List translatedValues = new ArrayList();
            for (Iterator i = values.iterator(); i.hasNext();) {
                translatedValues.add(translate((Expression)i.next()));
            }
            valueSource = new InsertValueExpressionsImpl(translatedValues);
        }
        
        InsertImpl result = new InsertImpl(translate(insert.getGroup()),
                              translatedElements,
                              valueSource);
        return result;
    }

    /* Update */
    IUpdate translate(Update update) throws MetaMatrixComponentException {
        UpdateImpl updateImpl =  new UpdateImpl(translate(update.getGroup()),
                              translate(update.getChangeList()),
                              translate(update.getCriteria()));
        return updateImpl;
    }
    
    ISetClauseList translate(SetClauseList setClauseList) throws MetaMatrixComponentException {
    	List<ISetClause> clauses = new ArrayList<ISetClause>(setClauseList.getClauses().size());
    	for (SetClause setClause : setClauseList.getClauses()) {
    		clauses.add(translate(setClause));
    	}
    	return new SetClauseListImpl(clauses);
    }
    
    ISetClause translate(SetClause setClause) throws MetaMatrixComponentException {
    	return new SetClauseImpl(translate(setClause.getSymbol()), translate(setClause.getValue()));
    }

    /* Delete */
    IDelete translate(Delete delete) throws MetaMatrixComponentException {
        DeleteImpl deleteImpl = new DeleteImpl(translate(delete.getGroup()),
                              translate(delete.getCriteria()));
        return deleteImpl;
    }

    /* Execute */
    IProcedure translate(StoredProcedure sp) throws MetaMatrixComponentException {
        Procedure proc = null;
        if(sp.getProcedureID() != null) {
            try {
                proc = this.metadataFactory.getProcedure(sp.getGroup().getName());
            } catch(ConnectorException e) {
                throw new MetaMatrixComponentException(e);
            }
        }

        List parameters = sp.getParameters();
        List<IParameter> translatedParameters = new ArrayList<IParameter>();
        for (Iterator i = parameters.iterator(); i.hasNext();) {
            translatedParameters.add(translate((SPParameter)i.next(), proc));
        }
                        
        return new ProcedureImpl(sp.getProcedureName(), translatedParameters, proc);
    }

    IParameter translate(SPParameter param, Procedure parent) {
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
                direction = Direction.RESULT_SET;
                break;
            case ParameterInfo.RETURN_VALUE: 
                direction = Direction.RETURN;
                break;
        }
        
        Parameter metadataParam = metadataFactory.getParameter(param, parent);
        return new ParameterImpl(param.getIndex(), direction, param.getValue(), param.getClassType(), metadataParam);                
    }

    public IGroup translate(GroupSymbol symbol) throws MetaMatrixComponentException {
        GroupImpl group = new GroupImpl(symbol.getOutputName(), symbol.getOutputDefinition(), null);
		if (symbol.getMetadataID() instanceof TempMetadataID) {
			return group;
		}
        try {
            group.setMetadataObject(metadataFactory.getGroup(symbol.getMetadataID()));
        } catch(Exception e) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, DQPPlugin.Util.getString("LanguageBridgeFactory.Unable_to_set_the_metadata_ID_for_group_{0}._11", symbol.getName())); //$NON-NLS-1$
            throw new MetaMatrixComponentException(e);
        }
        return group;
    }
    
    /* Batched Updates */
    IBatchedUpdates translate(BatchedUpdateCommand command) throws MetaMatrixComponentException {
        List updates = command.getUpdateCommands();
        List translatedUpdates = new ArrayList(updates.size());
        for (Iterator i = updates.iterator(); i.hasNext();) {
            translatedUpdates.add(translate((Command)i.next()));
        }
        return new BatchedUpdatesImpl(translatedUpdates);
    }

    ILimit translate(Limit limit) throws MetaMatrixComponentException {
        if (limit == null) {
            return null;
        }
        int rowOffset = 0;
        if (limit.getOffset() != null) {
            ILiteral c1 = (ILiteral)translate(limit.getOffset());
            rowOffset = ((Integer)c1.getValue()).intValue();
        }
        ILiteral c2 = (ILiteral)translate(limit.getRowLimit());
        int rowLimit = ((Integer)c2.getValue()).intValue();
        return new LimitImpl(rowOffset, rowLimit);
    }
}
