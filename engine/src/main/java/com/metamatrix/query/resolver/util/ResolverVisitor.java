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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.saxon.trans.XPathException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.DataTypeManager.DefaultDataClasses;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.internal.core.xml.XPathHelper;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionForm;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.metadata.GroupInfo;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.navigator.PostOrderNavigator;
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

public class ResolverVisitor extends LanguageVisitor {
    
    private static class ElementMatch {
    	ElementSymbol element;
    	GroupSymbol group;
    	
		public ElementMatch(ElementSymbol element, GroupSymbol group) {
			this.element = element;
			this.group = group;
		}
    }
    
    private Collection<GroupSymbol> groups;
    private GroupContext externalContext;
    protected QueryMetadataInterface metadata;
    private MetaMatrixComponentException componentException;
    private QueryResolverException resolverException;
    private Map<Function, QueryResolverException> unresolvedFunctions;
    
    /**
     * Constructor for ResolveElementsVisitor.
     * 
     * External groups are ordered from inner to outer most
     */
    public ResolverVisitor(QueryMetadataInterface metadata, Collection<GroupSymbol> internalGroups, GroupContext externalContext) {
        this.groups = internalGroups;
        this.externalContext = externalContext;
        this.metadata = metadata;
    }

	public void setGroups(Collection<GroupSymbol> groups) {
		this.groups = groups;
	}

    public void visit(ElementSymbol obj) {
        try {
            resolveElementSymbol(obj);
        } catch(QueryMetadataException e) {
            handleUnresolvedElement(obj, e.getMessage());
        } catch(MetaMatrixComponentException e) {
            handleException(e);
        } 
    }

    private void handleUnresolvedElement(ElementSymbol symbol, String description) {
    	UnresolvedSymbolDescription usd = new UnresolvedSymbolDescription(symbol.toString(), description);
        QueryResolverException e = new QueryResolverException(usd.getDescription());
        e.setUnresolvedSymbols(Arrays.asList(usd));
        handleException(e);
    }

    private void resolveElementSymbol(ElementSymbol elementSymbol)
        throws QueryMetadataException, MetaMatrixComponentException {

        // already resolved
        if(elementSymbol.getMetadataID() != null) {
        	return;
        }
        
        // determine the "metadataID" part of the symbol to look up
        String potentialID = elementSymbol.getName();
        
        // look up group and element parts of the potentialID
        String groupContext = metadata.getGroupName(potentialID);
        String elementShortName = metadata.getShortElementName(potentialID);
        boolean isUUID = false;
        if (groupContext != null) {
            groupContext = groupContext.toUpperCase();
            if (metadata.getGroupName(elementShortName) != null) {
                isUUID = true;
            }
        }
        
        boolean isExternal = false;
        boolean groupMatched = false;
        
        GroupContext root = null;
        
        if (groups != null || externalContext != null) {
            if (groups != null) {
                root = new GroupContext(externalContext, groups);
            }
            if (root == null) {
                isExternal = true;
                root = externalContext;
            }
        } else {
            try {
                LinkedList<GroupSymbol> matchedGroups = new LinkedList<GroupSymbol>();
                
                if (groupContext != null) {
                    //assume that this is fully qualified
                    Object groupID = this.metadata.getGroupID(groupContext);
                    // No groups specified, so any group is valid
                    GroupSymbol groupSymbol = new GroupSymbol(groupContext);
                    groupSymbol.setMetadataID(groupID);
                    matchedGroups.add(groupSymbol);
                }
                
                root = new GroupContext(null, matchedGroups);
            } catch(QueryMetadataException e) {
                // ignore 
            }
        }
        
        LinkedList<ElementMatch> matches = new LinkedList<ElementMatch>();
        String shortCanonicalName = elementShortName.toUpperCase();
        while (root != null) {
            Collection<GroupSymbol> matchedGroups = ResolverUtil.findMatchingGroups(groupContext, root.getGroups(), metadata);
            if (matchedGroups != null && !matchedGroups.isEmpty()) {
                groupMatched = true;
                    
                if (isUUID) {
                    resolveUsingUUID(potentialID, elementShortName, matches, matchedGroups);
                    break;
                }
                resolveAgainstGroups(shortCanonicalName, matchedGroups, matches);
                
                if (matches.size() > 1) {
                    handleUnresolvedElement(elementSymbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0053, elementSymbol));
                    return;
                }
                
                if (matches.size() == 1) {
                    break;
                }
            }
            
            root = root.getParent();
            isExternal = true;
        }
        
        if (matches.isEmpty()) {
            if (groupMatched) {
                handleUnresolvedElement(elementSymbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0054, elementSymbol));
            } else {
                handleUnresolvedElement(elementSymbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0051, elementSymbol));
            }
            return;
        }
        ElementMatch match = matches.getFirst();
        
        //copy the match information
        ElementSymbol resolvedSymbol = match.element;
        elementSymbol.setIsExternalReference(isExternal);
        elementSymbol.setType(resolvedSymbol.getType());
        elementSymbol.setMetadataID(resolvedSymbol.getMetadataID());
        elementSymbol.setGroupSymbol(match.group);
        String oldName = elementSymbol.getOutputName();
        elementSymbol.setName(resolvedSymbol.getName());
        elementSymbol.setOutputName(oldName);
   }

    private void resolveUsingUUID(String potentialID,
                                  String elementShortName,
                                  LinkedList<ElementMatch> matches,
                                  Collection<GroupSymbol> matchedGroups) throws MetaMatrixComponentException,
                                                           QueryMetadataException {
        Assertion.assertTrue(matchedGroups.size() == 1);
        GroupSymbol group = matchedGroups.iterator().next();
        ElementSymbol resolvedSymbol = new ElementSymbol(potentialID);
        resolvedSymbol.setGroupSymbol(group);
        String elementID = metadata.getFullElementName( metadata.getFullName(group.getMetadataID()), elementShortName );
        resolvedSymbol.setMetadataID( metadata.getElementID(elementID) );
        resolvedSymbol.setType( DataTypeManager.getDataTypeClass(metadata.getElementType(resolvedSymbol.getMetadataID())) );
        matches.add(new ElementMatch(resolvedSymbol, group));
    }

    private void resolveAgainstGroups(String elementShortName,
                                      Collection<GroupSymbol> matchedGroups, LinkedList<ElementMatch> matches) throws QueryMetadataException,
                                                         MetaMatrixComponentException {
    	for (GroupSymbol group : matchedGroups) {
            GroupInfo groupInfo = ResolverUtil.getGroupInfo(group, metadata);
            
            ElementSymbol result = groupInfo.getSymbol(elementShortName);
            if (result != null) {
            	matches.add(new ElementMatch(result, group));
            }
        }
    }
        
    public void visit(BetweenCriteria obj) {
        try {
            resolveBetweenCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        } catch(MetaMatrixComponentException e) {
            handleException(e);
        }
    }

    public void visit(CompareCriteria obj) {
        try {
            resolveCompareCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(MatchCriteria obj) {
        try {
            resolveMatchCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SetCriteria obj) {
        try {
            resolveSetCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SubqueryCompareCriteria obj) {
        try {
            obj.setLeftExpression(ResolverUtil.resolveSubqueryPredicateCriteria(obj.getLeftExpression(), obj));
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }


    public void visit(SubquerySetCriteria obj) {
        try {
            obj.setExpression(ResolverUtil.resolveSubqueryPredicateCriteria(obj.getExpression(), obj));
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(IsNullCriteria obj) {
        try {
        	setDesiredType(obj.getExpression(), DefaultDataClasses.OBJECT, obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }
    
    public void visit(Function obj) {
        try {
            resolveFunction(obj);
        } catch(QueryResolverException e) {
        	if (unresolvedFunctions == null) {
        		unresolvedFunctions = new LinkedHashMap<Function, QueryResolverException>();
        	}
        	unresolvedFunctions.put(obj, e);
        } catch(MetaMatrixComponentException e) {
            handleException(e);
        }
    }

    public void visit(CaseExpression obj) {
        try {
            resolveCaseExpression(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }
    
    public void visit(SearchedCaseExpression obj) {
        try {
            resolveSearchedCaseExpression(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }
    
    public void visit(SetClause obj) {
    	String type = DataTypeManager.getDataTypeName(obj.getSymbol().getType());
    	try {
    		setDesiredType(obj.getValue(), obj.getSymbol().getType(), obj);
            obj.setValue(ResolverUtil.convertExpression(obj.getValue(), type));                    
        } catch(QueryResolverException e) {
            handleException(new QueryResolverException(e, QueryPlugin.Util.getString("SetClause.resolvingError", new Object[] {obj.getValue(), obj.getSymbol(), type}))); //$NON-NLS-1$
        } 
    }

    public MetaMatrixComponentException getComponentException() {
        return this.componentException;
    }

    public QueryResolverException getResolverException() {
        return this.resolverException;
    }

    void handleException(MetaMatrixComponentException e) {
        this.componentException = e;

        // Abort the validation process
        setAbort(true);
    }

    void handleException(QueryResolverException e) {
        this.resolverException = e;

        // Abort the validation process
        setAbort(true);
    }

	public void throwException(boolean includeUnresolvedFunctions)
			throws MetaMatrixComponentException, QueryResolverException {
		if(getComponentException() != null) {
            throw getComponentException();
        }

        if(getResolverException() != null) {
            throw getResolverException();
        }
        
        if (includeUnresolvedFunctions 
        		&& unresolvedFunctions != null && !unresolvedFunctions.isEmpty()) {
        	throw unresolvedFunctions.values().iterator().next();
        }
	}

	/**
	 * Resolve function such that all functions are resolved and type-safe.
	 */
	void resolveFunction(Function function)
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
	        Class<?> type = DataTypeManager.getDataTypeClass((String)constant.getValue());
	
	        setDesiredType(function.getArg(0), type, function);
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
			ResolverUtil.ResolvedLookup lookup = ResolverUtil.resolveLookup(function, metadata);
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

	/**
	 * Find possible matches based on implicit conversions of the arguments.
	 * NOTE: This method has the side-effect of explicitly inserting conversions into the function arguments,
	 * and thereby changing the structure of the function call.
	 * @param library
	 * @param function
	 * @param types
	 * @return
	 * @throws MetaMatrixComponentException 
	 * @since 4.3
	 */
	private FunctionDescriptor findWithImplicitConversions(FunctionLibrary library, Function function, Expression[] args, Class<?>[] types, boolean hasArgWithoutType) throws QueryResolverException, MetaMatrixComponentException {
	    
	    // Try to find implicit conversion path to still perform this function
	    FunctionDescriptor[] conversions = library.determineNecessaryConversions(function.getName(), function.getType(), types, hasArgWithoutType);
	    
	    if(conversions == null) {
	        return null;
	    }
	    // Insert new conversion functions as necessary, while building new signature
	    Class<?>[] newSignature = new Class[conversions.length];
	    for(int i=0; i<conversions.length; i++) {
	        
	        Class<?> newType = types[i];
	        
	        if(conversions[i] != null) {
	            newType = conversions[i].getReturnType();
	            
	            setDesiredType(args[i], newType, function);
	                                
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
	 * @throws MetaMatrixComponentException
	 */
	void resolveBetweenCriteria(BetweenCriteria criteria)
	    throws QueryResolverException, MetaMatrixComponentException {
	
	    Expression exp = criteria.getExpression();
	    Expression lower = criteria.getLowerExpression();
	    Expression upper = criteria.getUpperExpression();
	
	    // invariants: none of the expressions is an aggregate symbol
	    setDesiredType(exp,
	                                   (lower.getType() == null)
	                                        ? upper.getType()
	                                        : lower.getType(), criteria);
	    // invariants: exp.getType() != null
	    setDesiredType(lower, exp.getType(), criteria);
	    setDesiredType(upper, exp.getType(), criteria);
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

	void resolveCompareCriteria(CompareCriteria ccrit)
		throws QueryResolverException {
	
		Expression leftExpression = ccrit.getLeftExpression();
		Expression rightExpression = ccrit.getRightExpression();
	
		// Check typing between expressions
	    setDesiredType(leftExpression, rightExpression.getType(), ccrit);
	    setDesiredType(rightExpression, leftExpression.getType(), ccrit);
	
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

	void resolveMatchCriteria(MatchCriteria mcrit)
	    throws QueryResolverException {
	
	    setDesiredType(mcrit.getLeftExpression(), mcrit.getRightExpression().getType(), mcrit);
	    mcrit.setLeftExpression(resolveMatchCriteriaExpression(mcrit, mcrit.getLeftExpression()));
	
	    setDesiredType(mcrit.getRightExpression(), mcrit.getLeftExpression().getType(), mcrit);
	    mcrit.setRightExpression(resolveMatchCriteriaExpression(mcrit, mcrit.getRightExpression()));
	}

	/**
	 * Checks one side of a LIKE Criteria; implicitly converts to a String or CLOB if necessary.
	 * @param mcrit the Match Criteria
	 * @param expr either left or right expression
	 * @return either 'expr' itself, or a new implicit type conversion wrapping expr
	 * @throws QueryResolverException if no implicit type conversion is available
	 */
	Expression resolveMatchCriteriaExpression(MatchCriteria mcrit, Expression expr)
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

	void resolveSetCriteria(SetCriteria scrit)
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
	        setDesiredType(value, exprType, scrit);
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

	void resolveCaseExpression(CaseExpression obj) throws QueryResolverException {
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
	    setDesiredType(expr, whenType, obj);
	    // Add the expression's type to the WHEN types
	    whenTypeNames.add(DataTypeManager.getDataTypeName(expr.getType()));
	    Expression when = null;
	    Expression then = null;
	    // Set the types of the WHEN and THEN parts
	    for (int i = 0; i < whenCount; i++) {
	        when = obj.getWhenExpression(i);
	        then = obj.getThenExpression(i);
	
	        setDesiredType(when, expr.getType(), obj);
	        setDesiredType(then, thenType, obj);
	
	        if (!whenTypeNames.contains(DataTypeManager.getDataTypeName(when.getType()))) {
	            whenTypeNames.add(DataTypeManager.getDataTypeName(when.getType()));
	        }
	        if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(then.getType()))) {
	            thenTypeNames.add(DataTypeManager.getDataTypeName(then.getType()));
	        }
	    }
	    // Set the type of the else expression
	    if (elseExpr != null) {
	        setDesiredType(elseExpr, thenType, obj);
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

	private void setDesiredType(Expression obj, Class<?> type, LanguageObject surrounding) throws QueryResolverException {
		ResolverUtil.setDesiredType(obj, type, surrounding);
		//second pass resolving for functions
		if (!(obj instanceof Function)) {
			return;
		}
		if (unresolvedFunctions != null) {
			Function f = (Function)obj;
			if (f.getFunctionDescriptor() != null) {
				return;
			}
        	unresolvedFunctions.remove(obj);
			obj.acceptVisitor(this);
			QueryResolverException e = unresolvedFunctions.get(obj);
			if (e != null) {
				throw e;
			}
		}
	}

	void resolveSearchedCaseExpression(SearchedCaseExpression obj) throws QueryResolverException {
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
	        setDesiredType(then, thenType, obj);
	        if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(then.getType()))) {
	            thenTypeNames.add(DataTypeManager.getDataTypeName(then.getType()));
	        }
	    }
	    // Set the type of the else expression
	    if (elseExpr != null) {
	        setDesiredType(elseExpr, thenType, obj);
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
	
    public static void resolveLanguageObject(LanguageObject obj, QueryMetadataInterface metadata)
    throws MetaMatrixComponentException, QueryResolverException {
	    ResolverVisitor.resolveLanguageObject(obj, null, metadata);
	}
	
	public static void resolveLanguageObject(LanguageObject obj, Collection<GroupSymbol> groups, QueryMetadataInterface metadata)
	    throws MetaMatrixComponentException, QueryResolverException {
	    ResolverVisitor.resolveLanguageObject(obj, groups, null, metadata);
	}
	
	public static void resolveLanguageObject(LanguageObject obj, Collection<GroupSymbol> groups, GroupContext externalContext, QueryMetadataInterface metadata)
	    throws MetaMatrixComponentException, QueryResolverException {
	
	    if(obj == null) {
	        return;
	    }
	
	    // Resolve elements, deal with errors
	    ResolverVisitor elementsVisitor = new ResolverVisitor(metadata, groups, externalContext);
	    PostOrderNavigator.doVisit(obj, elementsVisitor);
	    elementsVisitor.throwException(true);
	}
    
}
