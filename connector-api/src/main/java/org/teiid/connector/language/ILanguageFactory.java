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

package org.teiid.connector.language;

import java.util.List;

import org.teiid.connector.language.ICompoundCriteria.Operator;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;
import org.teiid.connector.metadata.runtime.Parameter;
import org.teiid.connector.metadata.runtime.Procedure;


/**
 * Factory for the construction of language objects that implement the language interfaces.
 * This factory is provided by the connector environment and can be used in modifying the language
 * interfaces if needed.  
 */
public interface ILanguageFactory {

    /**
     * Create aggregate function.
     * @param name Aggregate function name, as defined by constants in {@link IAggregate}
     * @param isDistinct True if aggregate function is DISTINCT, false otherwise
     * @param expression Inner expression of the aggregate function
     * @param type Data type
     * @return New IAggregate
     */
    IAggregate createAggregate(String name, boolean isDistinct, IExpression expression, Class type);

    /**
     * Create compare criteria.
     * @param operator Operator, as defined in constants in {@link ICompareCriteria}
     * @param leftExpression Left expression
     * @param rightExpression Right expression
     * @return New ICompareCriteria
     */
    ICompareCriteria createCompareCriteria(ICompareCriteria.Operator operator, IExpression leftExpression, IExpression rightExpression);
    
    /**
     * Create a new ICompoundCriteria
     * @param operator Operator, as defined by {@link Operator#AND} or 
     * {@link Operator#OR}
     * @param innerCriteria List of ICriteria, typically containing two criteria
     * @return New ICompoundCriteria
     */
    ICompoundCriteria createCompoundCriteria(ICompoundCriteria.Operator operator, List<? extends ICriteria> innerCriteria);

    /**
     * Create a new IDelete.
     * @param group The group to delete from
     * @param criteria The criteria (can be null)
     * @return New IDelete
     */
    IDelete createDelete(IGroup group, ICriteria criteria);

    /**
     * Create new element.
     * @param name Name of the element
     * @param group Group this element is in
     * @param metadataReference Metadata reference describing this element
     * @param type Data type
     * @return New IElement
     */
    IElement createElement(String name, IGroup group, Element metadataReference, Class type);
    
    /**
     * Create new exists criteria.
     * @param query Inner query
     * @return New IExists
     */
    IExistsCriteria createExistsCriteria(IQuery query);
    
    /**
     * Create new from clause
     * @param items List of IFromItem
     * @return New IFrom
     */
    IFrom createFrom(List<? extends IFromItem> items);

    /**
     * Create new function
     * @param functionName Name of the function
     * @param args Arguments, should never be null
     * @param type Data type returned
     * @return New IFunction
     */
    IFunction createFunction(String functionName, IExpression[] args, Class<?> type);
    
    /**
     * Create new function
     * @param functionName Name of the function
     * @param args Arguments, should never be null
     * @param type Data type returned
     * @return New IFunction
     */
    IFunction createFunction(String functionName, List<? extends IExpression> args, Class<?> type);

    /**
     * Create new group.
     * @param context Alias if it exists, or group name if no alias exists
     * @param definition Group name
     * @param metadataReference Reference to metadata identifier
     * @return New IGroup
     */    
    IGroup createGroup(String context, String definition, Group metadataReference);
    
    /**
     * Create new group by.
     * @param items List of IGroupByItem
     * @return New IGroupBy
     */
    IGroupBy createGroupBy(List<? extends IExpression> items);
    
    /**
     * Create new IN criteria
     * @param leftExpression Left expression
     * @param rightExpressions List of right expressions
     * @param isNegated True if NOT IN, false for IN
     * @return New IInCriteria
     */
    IInCriteria createInCriteria(IExpression leftExpression, List<? extends IExpression> rightExpressions, boolean isNegated);

    /**
     * Create new inline view
     * @param query The query defining the inline view
     * @param name The name of the inline view
     * @return New IInLineView
     */
    IInlineView createInlineView(IQueryCommand query, String name);
    
    /**
     * Create new insert command
     * @param group Insert group
     * @param columns List of IElement being inserted into
     * @param values List of IExpression (usually ILiteral)
     * @return New IInsert
     */
    IInsert createInsert(IGroup group, List<IElement> columns, IInsertValueSource valueSource);
 
    /**
     * Create a new value source for an insert command
     * @param values
     * @return
     */
    IInsertExpressionValueSource createInsertExpressionValueSource(List<IExpression> values);
    
    /**
     * Create new IS NULL criteria
     * @param expression Expression
     * @param isNegated True if IS NOT NULL, false if IS NULL
     * @return New IIsNullCriteria
     */
    IIsNullCriteria createIsNullCriteria(IExpression expression, boolean isNegated);
    
    /**
     * Create new join predicate 
     * @param joinType Join type as defined by constants in {@link IJoin}
     * @param leftItem Left from clause item
     * @param rightItem Right from clause item
     * @param criteria List of ICriteria (considered to be AND'ed together)
     * @return New IJoin
     */
    IJoin createJoin(IJoin.JoinType joinType, IFromItem leftItem, IFromItem rightItem, List<? extends ICriteria> criteria);
    
    /**
     * Create new LIKE criteria
     * @param leftExpression Left expression
     * @param rightExpression Right expression
     * @param escapeCharacter Escape character or null if none 
     * @param isNegated True if NOT LIKE, false if LIKE
     * @return New ILikeCriteria
     */
    ILikeCriteria createLikeCriteria(IExpression leftExpression, IExpression rightExpression, Character escapeCharacter, boolean isNegated);
    
    /**
     * Create new literal value.
     * @param value The value, may be null
     * @param type The data type
     * @return New ILiteral
     */
    ILiteral createLiteral(Object value, Class type);
    
    /**
     * Create new NOT criteria
     * @param criteria Inner criteria
     * @return New INotCriteria
     */
    INotCriteria createNotCriteria(ICriteria criteria);
    
    /**
     * Create new ORDER BY clause
     * @param items List of IOrderByItem
     * @return New IOrderBy
     */
    IOrderBy createOrderBy(List<? extends IOrderByItem> items);
    
    /**
     * Create new ORDER BY item
     * @param name Name of item
     * @param element Associated element, if applicable
     * @param direction Direction, defined by constants in {@link IOrderByItem}
     * @return New IOrderByItem
     */
    IOrderByItem createOrderByItem(String name, IElement element, boolean direction);
    
    /**
     * Create new procedure parameter
     * @param index Index in the procedure call
     * @param direction Kind of parameter - IN, OUT, .... as defined in {@link IParameter}
     * @param value Value, may be null if not applicable
     * @param type Data type
     * @param metadataReference Metadata identifier reference
     * @return New IParameter
     */
    IParameter createParameter(int index, Direction direction, Object value, Class type, Parameter metadataReference);
    
    /**
     * Create new procedure
     * @param name Name of procedure
     * @param parameters List of IParameter 
     * @param metadataReference Metadata identifier reference
     * @return New IProcedure
     */
    IProcedure createProcedure(String name, List<? extends IParameter> parameters, Procedure metadataReference);
    
    /**
     * Create new query
     * @param select SELECT clause
     * @param from FROM clause
     * @param where WHERE clause
     * @param groupBy GROUP BY clause
     * @param having HAVING clause
     * @param orderBy ORDER BY clause
     * @return New IQuery
     */
    IQuery createQuery(ISelect select, IFrom from, ICriteria where, IGroupBy groupBy, ICriteria having, IOrderBy orderBy);
    
    ISetQuery createSetOp(ISetQuery.Operation operation, boolean all, IQueryCommand leftQuery, IQueryCommand rightQuery, IOrderBy orderBy, ILimit limit);
    
    /**
     * Create new scalar subquery which can be used as an expression
     * @param query Subquery
     * @return New scalar subquery
     */
    IScalarSubquery createScalarSubquery(IQuery query);
    
    /**
     * Create searched case expression.
     * @param whenExpressions List of when expressions, should match thenExpressions
     * @param thenExpressions List of then expressions, should match whenExpressions
     * @param elseExpression Else expression, may be null
     * @param type Data type
     * @return New ICaseExpression
     */    
    ISearchedCaseExpression createSearchedCaseExpression(List<? extends ICriteria> whenExpressions, List<? extends IExpression> thenExpressions, IExpression elseExpression, Class type);

    /**
     * Create new SELECT clause
     * @param isDistinct True if DISTINCT, false for ALL
     * @param selectSymbols List of ISelectSymbol
     * @return New ISelect
     */
    ISelect createSelect(boolean isDistinct, List<? extends ISelectSymbol> selectSymbols);
    
    /**
     * Create new select symbol
     * @param name Name of the symbol (which may be an alias)
     * @param expression Expression 
     * @return New ISelectSymbol
     */
    ISelectSymbol createSelectSymbol(String name, IExpression expression);
    
    /**
     * Create new subquery compare criteria
     * @param leftExpression Left expression
     * @param operator Comparison operator, as defined in {@link ISubqueryCompareCriteria}
     * @param quantifier Quantification operator, as defined in {@link ISubqueryCompareCriteria}
     * @param subquery Right subquery
     * @return New ISubqueryCompareCriteria
     */
    ISubqueryCompareCriteria createSubqueryCompareCriteria(IExpression leftExpression, ICompareCriteria.Operator operator, ISubqueryCompareCriteria.Quantifier quantifier, IQuery subquery);
    
    /**
     * Create new subquery IN criteria
     * @param expression Left expression
     * @param subquery Right subquery
     * @param isNegated True if NOT IN, false if IN
     * @return New ISubqueryInCriteria
     */
    ISubqueryInCriteria createSubqueryInCriteria(IExpression expression, IQuery subquery, boolean isNegated);
    
    /**
     * Create new UPDATE command
     * @param group Group being updated
     * @param updates
     * @param criteria Criteria to use, may be null
     * @return New IUpdate
     */
    IUpdate createUpdate(IGroup group, ISetClauseList updates, ICriteria criteria);
    
    /**
     * Creates a new SetClauseList
     * @param clauses
     * @return
     */
    ISetClauseList createSetClauseList(List<ISetClause> clauses);
    
    /**
     * Creates a new SetClause
     * @param symbol
     * @param value
     * @return
     */
    ISetClause createSetClause(IElement symbol, IExpression value);
}
