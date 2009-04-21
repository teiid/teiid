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

package com.metamatrix.query.resolver.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.saxon.trans.XPathException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.DataTypeManager.DefaultDataClasses;
import com.metamatrix.internal.core.xml.XPathHelper;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionForm;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.util.ErrorMessageKeys;

public class ResolverVisitorUtil {
	
	public static class ResolvedLookup {
		private GroupSymbol group;
		private ElementSymbol keyElement;
		private ElementSymbol returnElement;
		
		void setGroup(GroupSymbol group) {
			this.group = group;
		}
		public GroupSymbol getGroup() {
			return group;
		}
		void setKeyElement(ElementSymbol keyElement) {
			this.keyElement = keyElement;
		}
		public ElementSymbol getKeyElement() {
			return keyElement;
		}
		void setReturnElement(ElementSymbol returnElement) {
			this.returnElement = returnElement;
		}
		public ElementSymbol getReturnElement() {
			return returnElement;
		}
	}
	
    /**
     * Resolves criteria "a BETWEEN b AND c". If type conversions are necessary,
     * this method attempts the following implicit conversions:
     * <br/>
     * <ol type="1" start="1">
     *   <li>convert the lower and upper expressions to the criteria expression's type, or</li>
     *   <li>convert the criteria and upper expressions to the lower expression's type, or</li>
     *   <li>convert the criteria and lower expressions to the upper expression's type, or</li>
     *   <li>convert all expressions to a common type to which all three expressions' types can be implicitly converted.</li>
     * </ol>
     * @param criteria
     * @throws QueryResolverException
     * @throws MetaMatrixComponentException
     */
    static void resolveBetweenCriteria(BetweenCriteria criteria)
        throws QueryResolverException {
    
        Expression exp = criteria.getExpression();
        Expression lower = criteria.getLowerExpression();
        Expression upper = criteria.getUpperExpression();
    
        // invariants: none of the expressions is an aggregate symbol
        ResolverUtil.setTypeIfReference(exp,
                                       (lower.getType() == null)
                                            ? upper.getType()
                                            : lower.getType(), criteria);
        // invariants: exp.getType() != null
        ResolverUtil.setTypeIfReference(lower, exp.getType(), criteria);
        ResolverUtil.setTypeIfReference(upper, exp.getType(), criteria);
        // invariants: none of the types is null
    
        String expTypeName = DataTypeManager.getDataTypeName(exp.getType());
        String lowerTypeName = DataTypeManager.getDataTypeName(lower.getType());
        String upperTypeName = DataTypeManager.getDataTypeName(upper.getType());
        if (exp.getType().equals(lower.getType()) && exp.getType().equals(upper.getType())) {
            return;
        }
    
        String commonType = ResolverUtil.getCommonType(new String[] {expTypeName, lowerTypeName, upperTypeName});
        if (commonType != null) {
            criteria.setExpression(ResolverUtil.convertExpression(exp, expTypeName, commonType));
            criteria.setLowerExpression(ResolverUtil.convertExpression(lower, lowerTypeName, commonType));
            criteria.setUpperExpression(ResolverUtil.convertExpression(upper, upperTypeName, commonType));
        } else {
            // Couldn't find a common type to implicitly convert to
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0027, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0027, expTypeName, lowerTypeName, criteria));
        }
        // invariants: exp.getType() == lower.getType() == upper.getType()
    }

    static void resolveCompareCriteria(CompareCriteria ccrit)
    	throws QueryResolverException {
    
    	Expression leftExpression = ccrit.getLeftExpression();
    	Expression rightExpression = ccrit.getRightExpression();
    
    	// Check typing between expressions
        ResolverUtil.setTypeIfReference(leftExpression, rightExpression.getType(), ccrit);
        ResolverUtil.setTypeIfReference(rightExpression, leftExpression.getType(), ccrit);
    
    	if(leftExpression.getType().equals(rightExpression.getType()) ) {
    		return;
    	}
    
		// Try to apply an implicit conversion from one side to the other
		String leftTypeName = DataTypeManager.getDataTypeName(leftExpression.getType());
		String rightTypeName = DataTypeManager.getDataTypeName(rightExpression.getType());

        // Special cases when right expression is a constant
        if(rightExpression instanceof Constant) {
            // Auto-convert constant string on right to expected type on left
            try {
                ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, leftTypeName));
                return;
            } catch (QueryResolverException qre) {
                //ignore
            }
        } 
        
        // Special cases when left expression is a constant
        if(leftExpression instanceof Constant) {
            // Auto-convert constant string on left to expected type on right
            try {
                ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName));
                return;                                           
            } catch (QueryResolverException qre) {
                //ignore
            }
        }

        // Try to apply a conversion generically
		
        if(ResolverUtil.canImplicitlyConvert(leftTypeName, rightTypeName)) {
			ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName) );
			return;
		}

		if(ResolverUtil.canImplicitlyConvert(rightTypeName, leftTypeName)) {
			ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, leftTypeName) );
			return;
	    }

    	String commonType = ResolverUtil.getCommonType(new String[] {leftTypeName, rightTypeName});
    	
    	if (commonType == null) {
            // Neither are aggs, but types can't be reconciled
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0027, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0027, new Object[] { leftTypeName, rightTypeName, ccrit }));
    	}
    	ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, commonType) );
		ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, commonType) );
    }

    static void resolveMatchCriteria(MatchCriteria mcrit)
        throws QueryResolverException {
    
        ResolverUtil.setTypeIfReference(mcrit.getLeftExpression(), mcrit.getRightExpression().getType(), mcrit);
        mcrit.setLeftExpression(resolveMatchCriteriaExpression(mcrit, mcrit.getLeftExpression()));
    
        ResolverUtil.setTypeIfReference(mcrit.getRightExpression(), mcrit.getLeftExpression().getType(), mcrit);
        mcrit.setRightExpression(resolveMatchCriteriaExpression(mcrit, mcrit.getRightExpression()));
    }

    /**
     * Checks one side of a LIKE Criteria; implicitly converts to a String or CLOB if necessary.
     * @param mcrit the Match Criteria
     * @param expr either left or right expression
     * @return either 'expr' itself, or a new implicit type conversion wrapping expr
     * @throws QueryResolverException if no implicit type conversion is available
     */
    static Expression resolveMatchCriteriaExpression(MatchCriteria mcrit, Expression expr)
    throws QueryResolverException {
        // Check left expression == string or CLOB
        String type = DataTypeManager.getDataTypeName(expr.getType());
        Expression result = expr;
        if(type != null) {
            if (! type.equals(DataTypeManager.DefaultDataTypes.STRING) &&
                ! type.equals(DataTypeManager.DefaultDataTypes.CLOB)) {
                    
                if(!(expr instanceof AggregateSymbol) &&
                    ResolverUtil.canImplicitlyConvert(type, DataTypeManager.DefaultDataTypes.STRING)) {
    
                    result = ResolverUtil.convertExpression(expr, type, DataTypeManager.DefaultDataTypes.STRING);
                    
                } else if (!(expr instanceof AggregateSymbol) &&
                    ResolverUtil.canImplicitlyConvert(type, DataTypeManager.DefaultDataTypes.CLOB)){
                        
                    result = ResolverUtil.convertExpression(expr, type, DataTypeManager.DefaultDataTypes.CLOB);
    
                } else {
                    throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0029, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0029, mcrit));
                }
            }
        }
        return result;
    }

    static void resolveSetCriteria(SetCriteria scrit)
        throws QueryResolverException {
    
        // Check that each of the values are the same type as expression
        Class exprType = scrit.getExpression().getType();
        if(exprType == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0030, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0030, scrit.getExpression()));
        }
    
        String exprTypeName = DataTypeManager.getDataTypeName(exprType);
        boolean changed = false;
        List newVals = new ArrayList();
    
        boolean convertLeft = false;
        Class setType = null;
    
        Iterator valIter = scrit.getValues().iterator();
        while(valIter.hasNext()) {
            Expression value = (Expression) valIter.next();
            ResolverUtil.setTypeIfReference(value, exprType, scrit);
            if(! value.getType().equals(exprType)) {
                if(value instanceof AggregateSymbol) {
                    throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0031, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0031, scrit));
                }
                // try to apply cast
                String valTypeName = DataTypeManager.getDataTypeName(value.getType());
                if(ResolverUtil.canImplicitlyConvert(valTypeName, exprTypeName)) {
                    // Apply cast and replace current value
                    newVals.add(ResolverUtil.convertExpression(value, valTypeName, exprTypeName) );
                    changed = true;
                } else {
                    convertLeft = true;
                    setType = value.getType();
                    break;
                }
            } else {
                newVals.add(value);
            }
        }
    
        // If no convert found for first element, check whether everything in the
        // set is the same and the convert can be placed on the left side
        if(convertLeft) {
            // Is there a possible conversion from left to right?
            String setTypeName = DataTypeManager.getDataTypeName(setType);
            if(ResolverUtil.canImplicitlyConvert(exprTypeName, setTypeName)) {
                valIter = scrit.getValues().iterator();
                while(valIter.hasNext()) {
                    Expression value = (Expression) valIter.next();
                    if(value.getType() == null) {
                        throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0030, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0030, value));
                    } else if(! value.getType().equals(setType)) {
                        throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0031, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0031, scrit));
                    }
                }
    
                // Convert left expression to type of values in the set
                scrit.setExpression(ResolverUtil.convertExpression(scrit.getExpression(), exprTypeName, setTypeName ));
    
            } else {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0031, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0031, scrit));
            }
        }
    
        if(changed) {
            scrit.setValues(newVals);
        }
    }

    /**
     * Check the type of the (left) expression and the type of the single
     * projected symbol of the subquery.  If they are not the same, try to find
     * an implicit conversion from the former type to the latter type, and wrap
     * the left expression in that conversion function; otherwise throw an
     * Exception.
     * @param expression the Expression on one side of the predicate criteria
     * @param crit the SubqueryContainer containing the subquery Command of the other
     * side of the predicate criteria
     * @return implicit conversion Function, or null if none is necessary
     * @throws QueryResolverException if a conversion is necessary but none can
     * be found
     */
    static Expression resolveSubqueryPredicateCriteria(Expression expression, SubqueryContainer crit)
    	throws QueryResolverException {
    
    	// Check that type of the expression is same as the type of the
    	// single projected symbol of the subquery
    	Class exprType = expression.getType();
    	if(exprType == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0030, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0030, expression));
    	}
    	String exprTypeName = DataTypeManager.getDataTypeName(exprType);
    
    	Collection projectedSymbols = crit.getCommand().getProjectedSymbols();
    	if (projectedSymbols.size() != 1){
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0032, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0032, crit.getCommand()));
    	}
    	Class subqueryType = ((Expression)projectedSymbols.iterator().next()).getType();
    	String subqueryTypeName = DataTypeManager.getDataTypeName(subqueryType);
    	Expression result = null;
        try {
            result = ResolverUtil.convertExpression(expression, exprTypeName, subqueryTypeName);
        } catch (QueryResolverException qre) {
            throw new QueryResolverException(qre, ErrorMessageKeys.RESOLVER_0033, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0033, crit));
        }
        return result;
    }

    static void resolveIsNullCriteria(IsNullCriteria crit)
        throws QueryResolverException {
    
        ResolverUtil.setTypeIfReference(crit.getExpression(), DefaultDataClasses.OBJECT, crit);
    }

    static void resolveCaseExpression(CaseExpression obj) throws QueryResolverException {
        // If already resolved, do nothing
        if (obj.getType() != null) {
            return;
        }
        final int whenCount = obj.getWhenCount();
        Expression expr = obj.getExpression();
    
        Class whenType = null;
        Class thenType = null;
        // Get the WHEN and THEN types, and get a candidate type for each (for the next step)
        for (int i = 0; i < whenCount; i++) {
            if (whenType == null) {
                whenType = obj.getWhenExpression(i).getType();
            }
            if (thenType == null) {
                thenType = obj.getThenExpression(i).getType();
            }
        }
    
        Expression elseExpr = obj.getElseExpression();
        if (elseExpr != null) {
            if (thenType == null) {
                thenType = elseExpr.getType();
            }
        }
        // Invariant: All the expressions contained in the obj are resolved (except References)
    
        // 2. Attempt to set the target types of all contained expressions,
        //    and collect their type names for the next step
        ArrayList whenTypeNames = new ArrayList(whenCount + 1);
        ArrayList thenTypeNames = new ArrayList(whenCount + 1);
        // Set the type of the expression
        ResolverUtil.setTypeIfReference(expr, whenType, obj);
        // Add the expression's type to the WHEN types
        whenTypeNames.add(DataTypeManager.getDataTypeName(expr.getType()));
        Expression when = null;
        Expression then = null;
        // Set the types of the WHEN and THEN parts
        for (int i = 0; i < whenCount; i++) {
            when = obj.getWhenExpression(i);
            then = obj.getThenExpression(i);
    
            ResolverUtil.setTypeIfReference(when, expr.getType(), obj);
            ResolverUtil.setTypeIfReference(then, thenType, obj);
    
            if (!whenTypeNames.contains(DataTypeManager.getDataTypeName(when.getType()))) {
                whenTypeNames.add(DataTypeManager.getDataTypeName(when.getType()));
            }
            if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(then.getType()))) {
                thenTypeNames.add(DataTypeManager.getDataTypeName(then.getType()));
            }
        }
        // Set the type of the else expression
        if (elseExpr != null) {
            ResolverUtil.setTypeIfReference(elseExpr, thenType, obj);
            if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(elseExpr.getType()))) {
                thenTypeNames.add(DataTypeManager.getDataTypeName(elseExpr.getType()));
            }
        }
    
        // Invariants: all the expressions' types are non-null
    
        // 3. Perform implicit type conversions
        String whenTypeName = ResolverUtil.getCommonType((String[])whenTypeNames.toArray(new String[whenTypeNames.size()]));
        if (whenTypeName == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0068, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0068, "WHEN", obj)); //$NON-NLS-1$
        }
        String thenTypeName = ResolverUtil.getCommonType((String[])thenTypeNames.toArray(new String[thenTypeNames.size()]));
        if (thenTypeName == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0068, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0068, "THEN/ELSE", obj)); //$NON-NLS-1$
        }
        obj.setExpression(ResolverUtil.convertExpression(obj.getExpression(), whenTypeName));
        ArrayList whens = new ArrayList(whenCount);
        ArrayList thens = new ArrayList(whenCount);
        for (int i = 0; i < whenCount; i++) {
            whens.add(ResolverUtil.convertExpression(obj.getWhenExpression(i), whenTypeName));
            thens.add(ResolverUtil.convertExpression(obj.getThenExpression(i), thenTypeName));
        }
        obj.setWhen(whens, thens);
        if (elseExpr != null) {
            obj.setElseExpression(ResolverUtil.convertExpression(elseExpr, thenTypeName));
        }
        // Set this CASE expression's type to the common THEN type, and we're done.
        obj.setType(DataTypeManager.getDataTypeClass(thenTypeName));
    }

    static void resolveSearchedCaseExpression(SearchedCaseExpression obj) throws QueryResolverException {
        // If already resolved, do nothing
        if (obj.getType() != null) {
            return;
        }
        final int whenCount = obj.getWhenCount();
        // 1. Call recursively to resolve any contained CASE expressions
    
        Class thenType = null;
        // Get the WHEN and THEN types, and get a candidate type for each (for the next step)
        for (int i = 0; i < whenCount; i++) {
            if (thenType == null) {
                thenType = obj.getThenExpression(i).getType();
            }
        }
    
        Expression elseExpr = obj.getElseExpression();
        if (elseExpr != null) {
            if (thenType == null) {
                thenType = elseExpr.getType();
            }
        }
        // Invariant: All the expressions contained in the obj are resolved (except References)
    
        // 2. Attempt to set the target types of all contained expressions,
        //    and collect their type names for the next step
        ArrayList thenTypeNames = new ArrayList(whenCount + 1);
        Expression then = null;
        // Set the types of the WHEN and THEN parts
        for (int i = 0; i < whenCount; i++) {
            then = obj.getThenExpression(i);
            ResolverUtil.setTypeIfReference(then, thenType, obj);
            if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(then.getType()))) {
                thenTypeNames.add(DataTypeManager.getDataTypeName(then.getType()));
            }
        }
        // Set the type of the else expression
        if (elseExpr != null) {
            ResolverUtil.setTypeIfReference(elseExpr, thenType, obj);
            if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(elseExpr.getType()))) {
                thenTypeNames.add(DataTypeManager.getDataTypeName(elseExpr.getType()));
            }
        }
    
        // Invariants: all the expressions' types are non-null
    
        // 3. Perform implicit type conversions
        String thenTypeName = ResolverUtil.getCommonType((String[])thenTypeNames.toArray(new String[thenTypeNames.size()]));
        if (thenTypeName == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0068, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0068, "THEN/ELSE", obj)); //$NON-NLS-1$
        }
        ArrayList thens = new ArrayList(whenCount);
        for (int i = 0; i < whenCount; i++) {
            thens.add(ResolverUtil.convertExpression(obj.getThenExpression(i), thenTypeName));
        }
        obj.setWhen(obj.getWhen(), thens);
        if (elseExpr != null) {
            obj.setElseExpression(ResolverUtil.convertExpression(elseExpr, thenTypeName));
        }
        // Set this CASE expression's type to the common THEN type, and we're done.
        obj.setType(DataTypeManager.getDataTypeClass(thenTypeName));
    }

    /**
     * Resolve function such that all functions are resolved and type-safe.
     */
    public static void resolveFunction(Function function, QueryMetadataInterface metadata)
        throws QueryResolverException, MetaMatrixComponentException {
    
        // Check whether this function is already resolved
        if(function.getFunctionDescriptor() != null) {
            return;
        }
    
        // Look up types for all args
        boolean hasArgWithoutType = false;
        Expression[] args = function.getArgs();
        Class[] types = new Class[args.length];
        for(int i=0; i<args.length; i++) {
            types[i] = args[i].getType();
            if(types[i] == null) {
            	if(!(args[i] instanceof Reference)){
                    throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0035, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0035, new Object[] {args[i], function}));
            	}
                hasArgWithoutType = true;
            }
        }
            
        FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();

        //special case handling for convert of an untyped reference
        if (FunctionLibrary.isConvert(function) && hasArgWithoutType) {
            Constant constant = (Constant)function.getArg(1);
            Class type = DataTypeManager.getDataTypeClass((String)constant.getValue());

            ResolverUtil.setTypeIfReference(function.getArg(0), type, function);
            types[0] = type;
            hasArgWithoutType = false;
        }
    
        // Attempt to get exact match of function for this signature
        FunctionDescriptor fd = findWithImplicitConversions(library, function, args, types, hasArgWithoutType);
        
        // Function did not resolve - determine reason and throw exception
        if(fd == null) {
            FunctionForm form = library.findFunctionForm(function.getName(), args.length);
            if(form == null) {
                // Unknown function form
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0039, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0039, function));
            }
            // Known function form - but without type information
            if (hasArgWithoutType) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0036, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0036, function));
            }
            // Known function form - unable to find implicit conversions
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0040, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0040, function));
        }
        
        if(fd.getName().equalsIgnoreCase(FunctionLibrary.CONVERT) || fd.getName().equalsIgnoreCase(FunctionLibrary.CAST)) {
            String dataType = (String) ((Constant)args[1]).getValue();
            Class dataTypeClass = DataTypeManager.getDataTypeClass(dataType);
            fd = library.findTypedConversionFunction(args[0].getType(), dataTypeClass);

            // Verify that the type conversion from src to type is even valid
            Class srcTypeClass = args[0].getType();
            if(srcTypeClass != null && dataTypeClass != null &&
               !srcTypeClass.equals(dataTypeClass) &&
               !DataTypeManager.isTransformable(srcTypeClass, dataTypeClass)) {

                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0037, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0037, new Object[] {DataTypeManager.getDataTypeName(srcTypeClass), dataType}));
            }
        } else if(fd.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)) {
			ResolvedLookup lookup = resolveLookup(function, metadata);
			fd = library.copyFunctionChangeReturnType(fd, lookup.getReturnElement().getType());
        } else if(fd.getName().equalsIgnoreCase(FunctionLibrary.XPATHVALUE)) {
            // Validate the xpath value is valid
            if(args[1] != null && args[1] instanceof Constant) {
                Constant xpathConst = (Constant) args[1];
                if(xpathConst.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
                    String value = (String) xpathConst.getValue();
                    if(value == null) {
                        throw new QueryResolverException(QueryPlugin.Util.getString("QueryResolver.invalid_xpath", QueryPlugin.Util.getString("ResolveFunctionsVisitor.xpath_cant_be_null"))); //$NON-NLS-1$ //$NON-NLS-2$                        
                    } 
    
                    try {
                        XPathHelper.validateXpath(value);
                    } catch(XPathException e) {
                        throw new QueryResolverException(QueryPlugin.Util.getString("QueryResolver.invalid_xpath", e.getMessage())); //$NON-NLS-1$
                    }
                }                
            }
        }

        // Resolve the function
        function.setFunctionDescriptor(fd);
        function.setType(fd.getReturnType());
    }
    
    public static ResolvedLookup resolveLookup(Function lookup, QueryMetadataInterface metadata) throws QueryResolverException, MetaMatrixComponentException {
    	Expression[] args = lookup.getArgs();
    	ResolvedLookup result = new ResolvedLookup();
        // Special code to handle setting return type of the lookup function to match the type of the return element
        if( args[0] instanceof Constant && args[1] instanceof Constant && args[2] instanceof Constant) {
            // If code table name in lookup function refers to virtual group, throws exception
			GroupSymbol groupSym = new GroupSymbol((String) ((Constant)args[0]).getValue());
			try {
				groupSym.setMetadataID(metadata.getGroupID((String) ((Constant)args[0]).getValue()));
				if (groupSym.getMetadataID() instanceof TempMetadataID) {
					throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0065, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0065, ((Constant)args[0]).getValue()));
				}
			} catch(QueryMetadataException e) {
				throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0062, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0062, ((Constant)args[0]).getValue()));
			}
			result.setGroup(groupSym);
			
			List<GroupSymbol> groups = Arrays.asList(groupSym);
			
			String returnElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[1]).getValue(); //$NON-NLS-1$
			ElementSymbol returnElement = new ElementSymbol(returnElementName);
            try {
                ResolverVisitor.resolveLanguageObject(returnElement, groups, metadata);
            } catch(QueryMetadataException e) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0062, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0062, returnElementName));
            }
			result.setReturnElement(returnElement);
            
            String keyElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[2]).getValue(); //$NON-NLS-1$
            ElementSymbol keyElement = new ElementSymbol(keyElementName);
            try {
                ResolverVisitor.resolveLanguageObject(keyElement, groups, metadata);
            } catch(QueryMetadataException e) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0062, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0062, keyElementName));
            }
			result.setKeyElement(keyElement);
			return result;
        } 
        throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0063, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0063));
    }

    /**
     * Find possible matches based on implicit conversions of the arguments.
     * NOTE: This method has the side-effect of explicitly inserting conversions into the function arguments,
     * and thereby changing the structure of the function call.
     * @param library
     * @param function
     * @param types
     * @return
     * @since 4.3
     */
    static FunctionDescriptor findWithImplicitConversions(FunctionLibrary library, Function function, Expression[] args, Class[] types, boolean hasArgWithoutType) throws QueryResolverException {
        
        // Try to find implicit conversion path to still perform this function
        FunctionDescriptor[] conversions = library.determineNecessaryConversions(function.getName(), types, hasArgWithoutType);
        
        if(conversions == null) {
            return null;
        }
        // Insert new conversion functions as necessary, while building new signature
        Class[] newSignature = new Class[conversions.length];
        for(int i=0; i<conversions.length; i++) {
            
            Class newType = types[i];
            
            if(conversions[i] != null) {
                newType = conversions[i].getReturnType();
                
                ResolverUtil.setTypeIfReference(args[i], newType, function);
                                    
                //only currently typed expressions need conversions
                if (types[i] != null) {
                    function.insertConversion(i, conversions[i]);
                }
            } 
                        
            newSignature[i] = newType;
        }

        // Now resolve using the new signature to get the function's descriptor
        return library.findFunction(function.getName(), newSignature);
    }

	private static QueryResolverException handleUnresolvedGroup(GroupSymbol symbol, String description) {
		UnresolvedSymbolDescription usd = new UnresolvedSymbolDescription(symbol.toString(), description);
	    QueryResolverException e = new QueryResolverException(usd.getDescription()+": "+usd.getSymbol()); //$NON-NLS-1$
	    e.setUnresolvedSymbols(Arrays.asList(usd));
	    return e;
	}

	public static void resolveGroup(GroupSymbol symbol, QueryMetadataInterface metadata)
	    throws MetaMatrixComponentException, QueryResolverException {
	
	    if (symbol.getMetadataID() != null){
	        return;
	    }
	
	    // determine the "metadataID" part of the symbol to look up
	    String potentialID = symbol.getNonCorrelationName();
	    
	    String name = symbol.getName();
	    String definition = symbol.getDefinition();
	
	    Object groupID = null;
	    try {
	        // get valid GroupID for possibleID - this may throw exceptions if group is invalid
	        groupID = metadata.getGroupID(potentialID);
	    } catch(QueryMetadataException e) {
	        // didn't find this group ID
	    } 
	
	    // If that didn't work, try to strip a vdb name from potentialID
	    String vdbName = null;
	    if(groupID == null) {
			String newPotentialID = potentialID;
	        int vdbIndex = potentialID.indexOf(ElementSymbol.SEPARATOR);
	        if(vdbIndex >= 0) {
	            String potentialVdbName = potentialID.substring(0, vdbIndex);
	            newPotentialID = potentialID.substring(vdbIndex+1);
	
	            try {
	                groupID = metadata.getGroupID(newPotentialID);
	                vdbName = potentialVdbName;
	            } catch(QueryMetadataException e) {
	                // ignore - just didn't find it
	            } 
	            if(groupID != null) {
	            	potentialID = newPotentialID;
	            }
	        }
	    }
	
	    // the group could be partially qualified,  verify that this group exists
	    // and there is only one group that matches the given partial name
	    if(groupID == null) {
	    	Collection groupNames = null;
	    	try {
	        	groupNames = metadata.getGroupsForPartialName(potentialID);
	        } catch(QueryMetadataException e) {
	            // ignore - just didn't find it
	        } 
	
	        if(groupNames != null) {
	            int matches = groupNames.size();
	            if(matches == 1) {
	            	potentialID = (String) groupNames.iterator().next();
			        try {
			            // get valid GroupID for possibleID - this may throw exceptions if group is invalid
			            groupID = metadata.getGroupID(potentialID);
			            //set group full name
			            if(symbol.getDefinition() != null){
			            	symbol.setDefinition(potentialID);
			            }else{
			            	symbol.setName(potentialID);
			            }
			        } catch(QueryMetadataException e) {
			            // didn't find this group ID
			        } 
	            } else if(matches > 1) {
	                throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0055));
	            }
	        }
	    }
	    
	    if (groupID == null || metadata.isProcedure(groupID)) {
		    //try procedure relational resolving
	        try {
	            StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(potentialID);
	            symbol.setProcedure(true);
	            groupID = storedProcedureInfo.getProcedureID();
	        } catch(QueryMetadataException e) {
	            // just ignore
	        } 
	    }
	    
	    if(groupID == null) {
	        throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0056));
	    }
	    // set real metadata ID in the symbol
	    symbol.setMetadataID(groupID);
	    if(vdbName != null) {
	        // reset name or definition to strip vdb name
	        if(symbol.getDefinition() == null) {
	            symbol.setName(potentialID);
	        } else {
	            symbol.setDefinition(potentialID);
	        }
	    }
	    try {
	        if (!symbol.isProcedure()) {
	            symbol.setIsTempTable(metadata.isTemporaryTable(groupID));
	        }
	    } catch(QueryMetadataException e) {
	        // should not come here
	    } 
	    
	    symbol.setOutputDefinition(definition);
	    symbol.setOutputName(name);
	}

}
