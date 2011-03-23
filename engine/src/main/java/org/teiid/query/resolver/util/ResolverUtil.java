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

package org.teiid.query.resolver.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.UnresolvedSymbolDescription;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.core.util.StringUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.GroupInfo;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.rules.RuleChooseJoinStrategy;
import org.teiid.query.optimizer.relational.rules.RuleRaiseAccess;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.AbstractCaseExpression;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SelectSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;


/**
 * Utilities used during resolution
 */
public class ResolverUtil {

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

	// Can't construct
    private ResolverUtil() {}

    /*
     *                        Type Conversion Utilities
     */

    /**
     * Gets the most specific type to which all the given types have an implicit
     * conversion. The method decides a common type as follows:
     * <ul>
     *   <li>If one or more of the given types is a candidate, then this method
     *       will return the candidate that occurs first in the given array.
     *       This is why the order of the names in the array is important. </li>
     *   <li>Otherwise, if none of them is a candidate, this method will attempt
     *       to find a common type to which all of them can be implicitly
     *       converted.</li>
     *   <li>Otherwise this method is unable to find a common type to which all
     *       the given types can be implicitly converted, and therefore returns
     *       a null.</li>
     * </ul>
     * @param typeNames an ordered array of unique type names.
     * @return a type name to which all the given types can be converted
     */
    public static String getCommonType(String[] typeNames) {
        if (typeNames == null || typeNames.length == 0) {
            return null;
        }
        // If there is only one type, then simply return it
        if (typeNames.length == 1) {
            return typeNames[0];
        }
        // A type can be implicitly converted to itself, so we put the implicit
        // conversions as well as the original type in the working list of
        // conversions.
        HashSet<String> commonConversions = new LinkedHashSet<String>();
        commonConversions.add(typeNames[0]);
        commonConversions.addAll(DataTypeManager.getImplicitConversions(typeNames[0]));
        for (int i = 1; i < typeNames.length; i++ ) {
            HashSet<String> conversions = new LinkedHashSet<String>(DataTypeManager.getImplicitConversions(typeNames[i]));
            conversions.add(typeNames[i]);
            // Equivalent to set intersection
            commonConversions.retainAll(conversions);
        }
        if (commonConversions.isEmpty()) {
            return null;
        }
        for (int i = 0; i < typeNames.length; i++) {
            if (commonConversions.contains(typeNames[i])) {
                return typeNames[i];
            }
        }
    	commonConversions.remove(DefaultDataTypes.STRING);
        commonConversions.remove(DefaultDataTypes.OBJECT);
        if (!commonConversions.isEmpty()) {
            return commonConversions.iterator().next(); 
        }
        return null;
    }

    /**
     * Gets whether there exists an implicit conversion from the source type to
     * the target type
     * @param fromType
     * @param toType
     * @return true if there exists an implicit conversion from the
     * <code>fromType</code> to the <code>toType</code>.
     */
    public static boolean canImplicitlyConvert(String fromType, String toType) {
        if (fromType.equals(toType)) return true;
        return DataTypeManager.isImplicitConversion(fromType, toType);
    }

    /**
     * Replaces a sourceExpression with a conversion of the source expression
     * to the target type. If the source type and target type are the same,
     * this method does nothing.
     * @param sourceExpression
     * @param targetTypeName
     * @return
     * @throws QueryResolverException 
     */
    public static Expression convertExpression(Expression sourceExpression, String targetTypeName, QueryMetadataInterface metadata) throws QueryResolverException {
        return convertExpression(sourceExpression,
                                 DataTypeManager.getDataTypeName(sourceExpression.getType()),
                                 targetTypeName, metadata);
    }

    /**
     * Replaces a sourceExpression with a conversion of the source expression
     * to the target type. If the source type and target type are the same,
     * this method does nothing.
     * @param sourceExpression
     * @param sourceTypeName
     * @param targetTypeName
     * @return
     * @throws QueryResolverException 
     */
    public static Expression convertExpression(Expression sourceExpression, String sourceTypeName, String targetTypeName, QueryMetadataInterface metadata) throws QueryResolverException {
        if (sourceTypeName.equals(targetTypeName)) {
            return sourceExpression;
        }
        
        if(canImplicitlyConvert(sourceTypeName, targetTypeName) 
                        || (sourceExpression instanceof Constant && convertConstant(sourceTypeName, targetTypeName, (Constant)sourceExpression) != null)) {
            return getConversion(sourceExpression, sourceTypeName, targetTypeName, true, metadata.getFunctionLibrary());
        }

        //Expression is wrong type and can't convert
        throw new QueryResolverException("ERR.015.008.0041", QueryPlugin.Util.getString("ERR.015.008.0041", new Object[] {targetTypeName, sourceExpression, sourceTypeName})); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static Constant convertConstant(String sourceTypeName,
                                           String targetTypeName,
                                           Constant constant) {
        if (!DataTypeManager.isTransformable(sourceTypeName, targetTypeName)) {
        	return null;
        }

        try {
	        //try to get the converted constant, if this fails then it is not in a valid format
	        Constant result = getProperlyTypedConstant(constant.getValue(), DataTypeManager.getDataTypeClass(targetTypeName));
	        
	        if (DataTypeManager.DefaultDataTypes.STRING.equals(sourceTypeName)) {
	        	if (DataTypeManager.DefaultDataTypes.CHAR.equals(targetTypeName)) {
	        		String value = (String)constant.getValue();
	        		if (value != null && value.length() != 1) {
	        			return null;
	        		}
	        	}
	        	return result;
	        }
	        
	        //for non-strings, ensure that the conversion is consistent
	        if (!DataTypeManager.isTransformable(targetTypeName, sourceTypeName)) {
	        	return null;
	        }
        
	        if (!(constant.getValue() instanceof Comparable)) {
	        	return null; //this is the case for xml constants
	        }
	        
	        Constant reverse = getProperlyTypedConstant(result.getValue(), constant.getType());
	        
	        if (((Comparable)constant.getValue()).compareTo(reverse.getValue()) == 0) {
	            return result;
	        }
        } catch (QueryResolverException e) {
        	
        }
            
        return null;
    }

    public static Function getConversion(Expression sourceExpression,
                                            String sourceTypeName,
                                            String targetTypeName,
                                            boolean implicit, FunctionLibrary library) {
        Class<?> srcType = DataTypeManager.getDataTypeClass(sourceTypeName);

        FunctionDescriptor fd = library.findTypedConversionFunction(srcType, DataTypeManager.getDataTypeClass(targetTypeName));

        Function conversion = new Function(fd.getName(), new Expression[] { sourceExpression, new Constant(targetTypeName) });
        conversion.setType(DataTypeManager.getDataTypeClass(targetTypeName));
        conversion.setFunctionDescriptor(fd);
        if (implicit) {
        	conversion.makeImplicit();
        }

        return conversion;
    }

    /**
     * Utility to set the type of an expression if it is a Reference and has a null type.
     * @param expression the expression to test
     * @param targetType the target type, if the expression's type is null.
     * @throws QueryResolverException if unable to set the reference type to the target type.
     */
    public static void setDesiredType(Expression expression, Class<?> targetType, LanguageObject surroundingExpression) throws QueryResolverException {
        if (expression instanceof Reference) {
        	Reference ref = (Reference)expression;
        	if (ref.isPositional() && ref.getType() == null) {
	        	if (targetType == null) {
	        		throw new QueryResolverException("ERR.015.008.0026", QueryPlugin.Util.getString("ERR.015.008.0026", surroundingExpression)); //$NON-NLS-1$ //$NON-NLS-2$
	        	}
	            ref.setType(targetType);
        	}
        } else if (expression instanceof Function) {
        	Function f = (Function)expression;
        	if (f.getType() == null) {
	        	f.setType(targetType);
        	}
        }
    }
    
	/**
	 * Attempt to resolve the order by throws QueryResolverException if the
	 * symbol is not of SingleElementSymbol type
	 * 
	 * @param orderBy
	 * @param fromClauseGroups
	 *            groups of the FROM clause of the query (for resolving
	 *            ambiguous unqualified element names), or empty List if a Set
	 *            Query Order By is being resolved
	 * @param knownElements
	 *            resolved elements from SELECT clause, which are only ones
	 *            allowed to be in ORDER BY
	 * @param metadata
	 *            QueryMetadataInterface
	 */
    public static void resolveOrderBy(OrderBy orderBy, QueryCommand command, QueryMetadataInterface metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {

    	List<SingleElementSymbol> knownElements = command.getProjectedQuery().getSelect().getProjectedSymbols();
    	
    	boolean isSimpleQuery = false;
    	List fromClauseGroups = Collections.emptyList();
        
        if (command instanceof Query) {
        	Query query = (Query)command;
        	isSimpleQuery = !query.getSelect().isDistinct() && !query.hasAggregates();
        	if (query.getFrom() != null) {
        		fromClauseGroups = query.getFrom().getGroups();
        	}
        }
    	
        // Cached state, if needed
        String[] knownShortNames = new String[knownElements.size()];
        List<Expression> expressions = new ArrayList<Expression>(knownElements.size());

        for(int i=0; i<knownElements.size(); i++) {
            SingleElementSymbol knownSymbol = knownElements.get(i);
            expressions.add(SymbolMap.getExpression(knownSymbol));
            if (knownSymbol instanceof ExpressionSymbol) {
                continue;
            }
            
            String name = knownSymbol.getShortName();
            
            knownShortNames[i] = name;
        }

        for (int i = 0; i < orderBy.getVariableCount(); i++) {
        	SingleElementSymbol sortKey = orderBy.getVariable(i);
        	if (sortKey instanceof ElementSymbol) {
        		ElementSymbol symbol = (ElementSymbol)sortKey;
        		String groupPart = null;
        		if (symbol.getGroupSymbol() != null) {
        			groupPart = symbol.getGroupSymbol().getName();
        		}
        		String symbolName = symbol.getName();
        		String shortName = symbol.getShortName();
        		if (groupPart == null) {
        			int position = -1;
    				SingleElementSymbol matchedSymbol = null;
    				// walk the SELECT col short names, looking for a match on the current ORDER BY 'short name'
    				for(int j=0; j<knownShortNames.length; j++) {
    					if( !shortName.equalsIgnoreCase( knownShortNames[j] )) {
    						continue;
    					}
    			        // if we already have a matched symbol, matching again here means it is duplicate/ambiguous
    			        if(matchedSymbol != null) {
    			        	if (!matchedSymbol.equals(knownElements.get(j))) {
    			        		throw new QueryResolverException("ERR.015.008.0042", QueryPlugin.Util.getString("ERR.015.008.0042", symbolName)); //$NON-NLS-1$ //$NON-NLS-2$
    			        	}
    			        	continue;
    			        }
    			        matchedSymbol = knownElements.get(j);
    			        position = j;
    				}
    				if (matchedSymbol != null) {
    				    TempMetadataID tempMetadataID = new TempMetadataID(symbol.getName(), matchedSymbol.getType());
    				    symbol.setMetadataID(tempMetadataID);
    				    symbol.setType(matchedSymbol.getType());
    				}
                    if (position != -1) {
                        orderBy.setExpressionPosition(i, position);
                        continue;
                    }
        		}
        	} else if (sortKey instanceof ExpressionSymbol) {
        		// check for legacy positional
    			ExpressionSymbol es = (ExpressionSymbol)sortKey;
        		if (es.getExpression() instanceof Constant) {
            		Constant c = (Constant)es.getExpression();
        		    int elementOrder = Integer.valueOf(c.getValue().toString()).intValue();
        		    // adjust for the 1 based index.
        		    if (elementOrder > knownElements.size() || elementOrder < 1) {
            		    throw new QueryResolverException(QueryPlugin.Util.getString("SQLParser.non_position_constant", c)); //$NON-NLS-1$
        		    }
        		    orderBy.setExpressionPosition(i, elementOrder - 1);
        		    continue;
        		}
        	}
        	//handle order by expressions        	
        	if (command instanceof SetQuery) {
    			throw new QueryResolverException(QueryPlugin.Util.getString("ResolverUtil.setquery_order_expression", sortKey)); //$NON-NLS-1$	 
    		}
        	for (ElementSymbol symbol : ElementCollectorVisitor.getElements(sortKey, false)) {
        		try {
        	    	ResolverVisitor.resolveLanguageObject(symbol, fromClauseGroups, command.getExternalGroupContexts(), metadata);
        	    } catch(QueryResolverException e) {
        	    	throw new QueryResolverException(e, "ERR.015.008.0043", QueryPlugin.Util.getString("ERR.015.008.0043", symbol.getName()) );//$NON-NLS-1$ //$NON-NLS-2$
        	    } 
			}
            ResolverVisitor.resolveLanguageObject(sortKey, metadata);
            
            int index = expressions.indexOf(SymbolMap.getExpression(sortKey));
            if (index == -1 && !isSimpleQuery) {
    	        throw new QueryResolverException(QueryPlugin.Util.getString("ResolverUtil.invalid_unrelated", sortKey)); //$NON-NLS-1$
        	}
        	orderBy.setExpressionPosition(i, index);
        }
    }
    
    /** 
     * Get the default value for the parameter, which could be null
     * if the parameter is set to NULLABLE.  If no default is available,
     * a QueryResolverException will be thrown.
     * 
     * @param symbol ElementSymbol retrieved from metadata, fully-resolved
     * @param metadata QueryMetadataInterface
     * @return expr param (if it is non-null) or default value (if there is one)
     * or null Constant (if parameter is optional and therefore allows this)
     * @throws QueryResolverException if expr is null, parameter is required and no
     * default value is defined
     * @throws QueryMetadataException for error retrieving metadata
     * @throws TeiidComponentException
     * @since 4.3
     */
	public static Expression getDefault(ElementSymbol symbol, QueryMetadataInterface metadata) throws TeiidComponentException, QueryMetadataException, QueryResolverException {
        //Check if there is a default value, if so use it
		Object mid = symbol.getMetadataID();
    	Class<?> type = symbol.getType();
		
        Object defaultValue = metadata.getDefaultValue(mid);
        
        if (defaultValue == null && !metadata.elementSupports(mid, SupportConstants.Element.NULL)) {
            throw new QueryResolverException(QueryPlugin.Util.getString("ResolverUtil.required_param", symbol.getOutputName())); //$NON-NLS-1$
        }
        
        return getProperlyTypedConstant(defaultValue, type);
	}    
    
    /** 
     * Construct a Constant with proper type, given the String default
     * value for the parameter and the parameter type.  Throw a
     * QueryResolverException if the String can't be transformed.
     * @param defaultValue, either null or a String
     * @param parameterType modeled type of parameter (MetaMatrix runtime type)
     * @return Constant with proper type and default value Object of proper Class.  Will
     * be null Constant if defaultValue is null.
     * @throws QueryResolverException if TransformationException is encountered
     * @since 4.3
     */
    private static Constant getProperlyTypedConstant(Object defaultValue,
                                                Class parameterType)
    throws QueryResolverException{
        try {
            Object newValue = DataTypeManager.transformValue(defaultValue, parameterType);
            return new Constant(newValue, parameterType);
        } catch (TransformationException e) {
            throw new QueryResolverException(e, QueryPlugin.Util.getString("ResolverUtil.error_converting_value_type", defaultValue, defaultValue.getClass(), parameterType)); //$NON-NLS-1$
        }
    }

    /**
     * Returns the resolved elements in the given group.  This method has the side effect of caching the resolved elements on the group object.
     * The resolved elements may not contain non-selectable columns depending on the metadata first used for resolving.
     * 
     */
    public static List<ElementSymbol> resolveElementsInGroup(GroupSymbol group, QueryMetadataInterface metadata)
    throws QueryMetadataException, TeiidComponentException {
        return new ArrayList<ElementSymbol>(getGroupInfo(group, metadata).getSymbolList());
    }
    
	static GroupInfo getGroupInfo(GroupSymbol group,
			QueryMetadataInterface metadata)
			throws TeiidComponentException, QueryMetadataException {
		String key = GroupInfo.CACHE_PREFIX + group.getCanonicalName();
		GroupInfo groupInfo = (GroupInfo)metadata.getFromMetadataCache(group.getMetadataID(), key);
    	
        if (groupInfo == null) {
        	group = group.clone();
            // get all elements from the metadata
            List elementIDs = metadata.getElementIDsInGroupID(group.getMetadataID());

    		LinkedHashMap<Object, ElementSymbol> symbols = new LinkedHashMap<Object, ElementSymbol>(elementIDs.size());
            
            for (Object elementID : elementIDs) {
            	String elementName = metadata.getName(elementID);
                // Form an element symbol from the ID
                ElementSymbol element = new ElementSymbol(elementName, DataTypeManager.getCanonicalString(StringUtil.toUpperCase(elementName)), group);
                element.setMetadataID(elementID);
                element.setType( DataTypeManager.getDataTypeClass(metadata.getElementType(element.getMetadataID())) );

                symbols.put(elementID, element);
            }
            groupInfo = new GroupInfo(symbols);
            metadata.addToMetadataCache(group.getMetadataID(), key, groupInfo);
        }
		return groupInfo;
	}
    
    /**
     * When access patterns are flattened, they are an approximation the user
     * may need to enter as criteria.
     *  
     * @param metadata
     * @param groups
     * @param flatten
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
	public static List getAccessPatternElementsInGroups(final QueryMetadataInterface metadata, Collection groups, boolean flatten) throws TeiidComponentException, QueryMetadataException {
		List accessPatterns = null;
		Iterator i = groups.iterator();
		while (i.hasNext()){
		    
		    GroupSymbol group = (GroupSymbol)i.next();
		    
		    //Check this group for access pattern(s).
		    Collection accessPatternIDs = metadata.getAccessPatternsInGroup(group.getMetadataID());
		    if (accessPatternIDs != null && accessPatternIDs.size() > 0){
		        Iterator j = accessPatternIDs.iterator();
		        if (accessPatterns == null){
		            accessPatterns = new ArrayList();
		        }
		        while (j.hasNext()) {
		        	List elements = metadata.getElementIDsInAccessPattern(j.next());
		        	GroupInfo groupInfo = getGroupInfo(group, metadata);
		        	List result = new ArrayList(elements.size());
		        	for (Iterator iterator = elements.iterator(); iterator.hasNext();) {
		    			Object id = iterator.next();
		    			ElementSymbol symbol = groupInfo.getSymbol(id);
		    			assert symbol != null;
		    			result.add(symbol);
		    		}
		        	if (flatten) {
		        		accessPatterns.addAll(result);
		        	} else {
		        		accessPatterns.add(new AccessPattern(result));
		        	}
		        }
		    }
		}
                
		return accessPatterns;
	}

    public static void resolveLimit(Limit limit) throws QueryResolverException {
        if (limit.getOffset() != null) {
            setDesiredType(limit.getOffset(), DataTypeManager.DefaultDataClasses.INTEGER, limit);
        }
        setDesiredType(limit.getRowLimit(), DataTypeManager.DefaultDataClasses.INTEGER, limit);
    }
    
    public static void resolveImplicitTempGroup(TempMetadataAdapter metadata, GroupSymbol symbol, List symbols) 
        throws TeiidComponentException, QueryResolverException {
        
        if (symbol.isImplicitTempGroupSymbol()) {
            if (metadata.getMetadataStore().getTempElementElementIDs(symbol.getCanonicalName())==null) {
                addTempGroup(metadata, symbol, symbols, true);
            }
            ResolverUtil.resolveGroup(symbol, metadata);
        }
    }

    public static TempMetadataID addTempGroup(TempMetadataAdapter metadata,
                                    GroupSymbol symbol,
                                    List<? extends SingleElementSymbol> symbols, boolean tempTable) throws QueryResolverException {
        HashSet<String> names = new HashSet<String>();
        for (SingleElementSymbol ses : symbols) {
            if (!names.add(ses.getShortCanonicalName())) {
                throw new QueryResolverException(QueryPlugin.Util.getString("ResolverUtil.duplicateName", symbol, ses.getShortName())); //$NON-NLS-1$
            }
        }
        
        if (tempTable) {
            resolveNullLiterals(symbols);
        }
        TempMetadataStore store = metadata.getMetadataStore();
        return store.addTempGroup(symbol.getName(), symbols, !tempTable, tempTable);
    }
    
    public static TempMetadataID addTempTable(TempMetadataAdapter metadata,
                                     GroupSymbol symbol,
                                     List<? extends SingleElementSymbol> symbols) throws QueryResolverException {
        return addTempGroup(metadata, symbol, symbols, true);
    }

    /** 
     * Look for a null literal in the SELECT clause and set it's type to STRING.  This ensures that 
     * the result set metadata retrieved for this query will be properly set to something other than
     * the internal NullType.  Added for defect 15437.
     * 
     * @param select The select clause
     * @since 4.2
     */
    public static void resolveNullLiterals(List symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            SelectSymbol selectSymbol = (SelectSymbol) symbols.get(i);
            
            if (!(selectSymbol instanceof SingleElementSymbol)) {
                continue;
            }
            
            SingleElementSymbol symbol = (SingleElementSymbol)selectSymbol;
            
            if(!DataTypeManager.DefaultDataClasses.NULL.equals(symbol.getType()) && symbol.getType() != null) {
                continue;
            }
                        
            setSymbolType(symbol, DataTypeManager.DefaultDataClasses.STRING);
        }
    }

	public static void setSymbolType(SingleElementSymbol symbol,
			Class<?> replacement) {
		if(symbol instanceof AliasSymbol) {
            symbol = ((AliasSymbol)symbol).getSymbol();
        }
		if(symbol instanceof ExpressionSymbol && !(symbol instanceof AggregateSymbol)) {
		    ExpressionSymbol exprSymbol = (ExpressionSymbol) symbol;
		    Expression expr = exprSymbol.getExpression();
		   	
		    if(expr instanceof Constant) {                	
		        exprSymbol.setExpression(new Constant(null, replacement));
		    } else if (expr instanceof AbstractCaseExpression) {
		        ((AbstractCaseExpression)expr).setType(replacement);
		    } else if (expr instanceof ScalarSubquery) {
		        ((ScalarSubquery)expr).setType(replacement);                                        
		    } else {
		    	try {
					ResolverUtil.setDesiredType(expr, replacement, symbol);
				} catch (QueryResolverException e) {
					//cannot happen
				}
		    }
		} else if(symbol instanceof ElementSymbol) {
		    ElementSymbol elementSymbol = (ElementSymbol)symbol;
		    Class elementType = elementSymbol.getType();
		    if(elementType != null && elementType.equals(DataTypeManager.DefaultDataClasses.NULL)) {
		        elementSymbol.setType(replacement);
		    }
		}
	}
    
    /**
     *  
     * @param groupContext
     * @param groups
     * @param metadata
     * @return the List of groups that match the given groupContext out of the supplied collection
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    public static List<GroupSymbol> findMatchingGroups(String groupContext,
                            Collection<GroupSymbol> groups,
                            QueryMetadataInterface metadata) throws TeiidComponentException,
                                                            QueryMetadataException {

        if (groups == null) {
            return null;
        }

        LinkedList<GroupSymbol> matchedGroups = new LinkedList<GroupSymbol>();

        if (groupContext == null) {
            matchedGroups.addAll(groups);
        } else {
        	for (GroupSymbol group : groups) {
                String fullName = group.getCanonicalName();
                if (nameMatchesGroup(groupContext, matchedGroups, group, fullName)) {
                    if (groupContext.length() == fullName.length()) {
                        return matchedGroups;
                    }
                    continue;
                }

                // don't try to vdb qualify temp metadata
                if (group.getMetadataID() instanceof TempMetadataID) {
                    continue;
                }

                String actualVdbName = metadata.getVirtualDatabaseName();

                if (actualVdbName != null) {
                    fullName = actualVdbName.toUpperCase() + ElementSymbol.SEPARATOR + fullName;
                    if (nameMatchesGroup(groupContext, matchedGroups, group, fullName)
                        && groupContext.length() == fullName.length()) {
                        return matchedGroups;
                    }
                }
            }
        }

        return matchedGroups;
    }

    
    public static boolean nameMatchesGroup(String groupContext,
                                            String fullName) {
        //if there is a name match, make sure that it is the full name or a proper qualifier
        if (fullName.endsWith(groupContext)) {
            int matchIndex = fullName.length() - groupContext.length();
            if (matchIndex == 0 || fullName.charAt(matchIndex - 1) == '.') {
                return true;
            }
        }
        return false;
    }
    
    private static boolean nameMatchesGroup(String groupContext,
                                            LinkedList<GroupSymbol> matchedGroups,
                                            GroupSymbol group,
                                            String fullName) {
        if (nameMatchesGroup(groupContext, fullName)) {
            matchedGroups.add(group);
            return true;
        }
        return false;
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
	static Expression resolveSubqueryPredicateCriteria(Expression expression, SubqueryContainer crit, QueryMetadataInterface metadata)
		throws QueryResolverException {
	
		// Check that type of the expression is same as the type of the
		// single projected symbol of the subquery
		Class exprType = expression.getType();
		if(exprType == null) {
	        throw new QueryResolverException("ERR.015.008.0030", QueryPlugin.Util.getString("ERR.015.008.0030", expression)); //$NON-NLS-1$ //$NON-NLS-2$
		}
		String exprTypeName = DataTypeManager.getDataTypeName(exprType);
	
		Collection<SingleElementSymbol> projectedSymbols = crit.getCommand().getProjectedSymbols();
		if (projectedSymbols.size() != 1){
	        throw new QueryResolverException("ERR.015.008.0032", QueryPlugin.Util.getString("ERR.015.008.0032", crit.getCommand())); //$NON-NLS-1$ //$NON-NLS-2$
		}
		Class<?> subqueryType = projectedSymbols.iterator().next().getType();
		String subqueryTypeName = DataTypeManager.getDataTypeName(subqueryType);
		Expression result = null;
	    try {
	        result = convertExpression(expression, exprTypeName, subqueryTypeName, metadata);
	    } catch (QueryResolverException qre) {
	        throw new QueryResolverException(qre, "ERR.015.008.0033", QueryPlugin.Util.getString("ERR.015.008.0033", crit)); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	    return result;
	}

	public static ResolvedLookup resolveLookup(Function lookup, QueryMetadataInterface metadata) throws QueryResolverException, TeiidComponentException {
		Expression[] args = lookup.getArgs();
		ResolvedLookup result = new ResolvedLookup();
	    // Special code to handle setting return type of the lookup function to match the type of the return element
	    if( !(args[0] instanceof Constant) || !(args[1] instanceof Constant) || !(args[2] instanceof Constant)) {
		    throw new QueryResolverException("ERR.015.008.0063", QueryPlugin.Util.getString("ERR.015.008.0063")); //$NON-NLS-1$ //$NON-NLS-2$
	    }
        // If code table name in lookup function refers to temp group throw exception
		GroupSymbol groupSym = new GroupSymbol((String) ((Constant)args[0]).getValue());
		try {
			groupSym.setMetadataID(metadata.getGroupID((String) ((Constant)args[0]).getValue()));
			if (groupSym.getMetadataID() instanceof TempMetadataID) {
				throw new QueryResolverException("ERR.015.008.0065", QueryPlugin.Util.getString("ERR.015.008.0065", ((Constant)args[0]).getValue())); //$NON-NLS-1$ //$NON-NLS-2$
			}
		} catch(QueryMetadataException e) {
			throw new QueryResolverException("ERR.015.008.0062", QueryPlugin.Util.getString("ERR.015.008.0062", ((Constant)args[0]).getValue())); //$NON-NLS-1$ //$NON-NLS-2$
		}
		result.setGroup(groupSym);
		
		List<GroupSymbol> groups = Arrays.asList(groupSym);
		
		String returnElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[1]).getValue(); //$NON-NLS-1$
		ElementSymbol returnElement = new ElementSymbol(returnElementName);
        try {
            ResolverVisitor.resolveLanguageObject(returnElement, groups, metadata);
        } catch(QueryMetadataException e) {
            throw new QueryResolverException("ERR.015.008.0062", QueryPlugin.Util.getString("ERR.015.008.0062", returnElementName)); //$NON-NLS-1$ //$NON-NLS-2$
        }
		result.setReturnElement(returnElement);
        
        String keyElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[2]).getValue(); //$NON-NLS-1$
        ElementSymbol keyElement = new ElementSymbol(keyElementName);
        try {
            ResolverVisitor.resolveLanguageObject(keyElement, groups, metadata);
        } catch(QueryMetadataException e) {
            throw new QueryResolverException("ERR.015.008.0062", QueryPlugin.Util.getString("ERR.015.008.0062", keyElementName)); //$NON-NLS-1$ //$NON-NLS-2$
        }
		result.setKeyElement(keyElement);
		args[3] = convertExpression(args[3], DataTypeManager.getDataTypeName(keyElement.getType()), metadata);
		return result;
	}

	private static QueryResolverException handleUnresolvedGroup(GroupSymbol symbol, String description) {
		UnresolvedSymbolDescription usd = new UnresolvedSymbolDescription(symbol.toString(), description);
	    QueryResolverException e = new QueryResolverException(usd.getDescription()+": "+usd.getSymbol()); //$NON-NLS-1$
	    e.setUnresolvedSymbols(Arrays.asList(usd));
	    return e;
	}

	public static void resolveGroup(GroupSymbol symbol, QueryMetadataInterface metadata)
	    throws TeiidComponentException, QueryResolverException {
	
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
	    	String[] parts = potentialID.split("\\.", 2); //$NON-NLS-1$
	    	if (parts.length > 1 && parts[0].equalsIgnoreCase(metadata.getVirtualDatabaseName())) {
	            try {
	                groupID = metadata.getGroupID(parts[1]);
	            } catch(QueryMetadataException e) {
	                // ignore - just didn't find it
	            } 
	            if(groupID != null) {
	            	potentialID = parts[1];
	            	vdbName = parts[0];
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
	                throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString("ERR.015.008.0055")); //$NON-NLS-1$
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
	        throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString("ERR.015.008.0056")); //$NON-NLS-1$
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
	
	public static void findKeyPreserved(Query query, Set<GroupSymbol> keyPreservingGroups, QueryMetadataInterface metadata)
	throws TeiidComponentException, QueryMetadataException {
		if (query.getFrom() == null) {
			return;
		}
		if (query.getFrom().getClauses().size() == 1) {
			findKeyPreserved((FromClause)query.getFrom().getClauses().get(0), keyPreservingGroups, metadata);
			return;
		}
		//non-ansi join
		Set<GroupSymbol> groups = new HashSet<GroupSymbol>(query.getFrom().getGroups());
		for (GroupSymbol groupSymbol : groups) {
			if (metadata.getUniqueKeysInGroup(groupSymbol.getMetadataID()).isEmpty()) {
				return;
			}
		}
		LinkedList<Expression> leftExpressions = new LinkedList<Expression>();
		LinkedList<Expression> rightExpressions = new LinkedList<Expression>();
		for (Criteria crit : Criteria.separateCriteriaByAnd(query.getCriteria())) {
			if (!(crit instanceof CompareCriteria)) {
				continue;
			}
			CompareCriteria cc = (CompareCriteria)crit;
			if (cc.getOperator() != CompareCriteria.EQ) {
				continue;
			}
			if (cc.getLeftExpression() instanceof ElementSymbol && cc.getRightExpression() instanceof ElementSymbol) {
				ElementSymbol left = (ElementSymbol)cc.getLeftExpression();
				ElementSymbol right = (ElementSymbol)cc.getRightExpression();
				int compare = left.getGroupSymbol().compareTo(right.getGroupSymbol());
				if (compare > 0) {
					leftExpressions.add(left);
					rightExpressions.add(right);
				} else if (compare != 0) {
					leftExpressions.add(right);
					rightExpressions.add(left);
				}
			}
		}
		HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits = createGroupMap(leftExpressions, rightExpressions);
		HashSet<GroupSymbol> tempSet = new HashSet<GroupSymbol>();
		HashSet<GroupSymbol> nonKeyPreserved = new HashSet<GroupSymbol>();
		for (GroupSymbol group : groups) {
			LinkedHashSet<GroupSymbol> visited = new LinkedHashSet<GroupSymbol>();
			LinkedList<GroupSymbol> toVisit = new LinkedList<GroupSymbol>();
			toVisit.add(group);
			while (!toVisit.isEmpty()) {
				GroupSymbol visiting = toVisit.removeLast();
				if (!visited.add(visiting) || nonKeyPreserved.contains(visiting)) {
					continue;
				}
				if (keyPreservingGroups.contains(visiting)) {
					visited.addAll(groups);
					break;
				}
				toVisit.addAll(findKeyPreserved(tempSet, Collections.singleton(visiting), crits, true, metadata, groups));
				toVisit.addAll(findKeyPreserved(tempSet, Collections.singleton(visiting), crits, false, metadata, groups));
			}
			if (visited.containsAll(groups)) {
				keyPreservingGroups.add(group);
			} else {
				nonKeyPreserved.add(group);
			}
		}
	}
		
	public static void findKeyPreserved(FromClause clause, Set<GroupSymbol> keyPreservingGroups, QueryMetadataInterface metadata)
	throws TeiidComponentException, QueryMetadataException {
		if (clause instanceof UnaryFromClause) {
			UnaryFromClause ufc = (UnaryFromClause)clause;
		    
			if (!metadata.getUniqueKeysInGroup(ufc.getGroup().getMetadataID()).isEmpty()) {
				keyPreservingGroups.add(ufc.getGroup());
			}
		} 
		if (clause instanceof JoinPredicate) {
			JoinPredicate jp = (JoinPredicate)clause;
			if (jp.getJoinType() == JoinType.JOIN_CROSS || jp.getJoinType() == JoinType.JOIN_FULL_OUTER) {
				return;
			}
			HashSet<GroupSymbol> leftPk = new HashSet<GroupSymbol>();
			findKeyPreserved(jp.getLeftClause(), leftPk, metadata);
			HashSet<GroupSymbol> rightPk = new HashSet<GroupSymbol>();
			findKeyPreserved(jp.getRightClause(), rightPk, metadata);
			
			if (leftPk.isEmpty() && rightPk.isEmpty()) {
				return;
			}
			
			HashSet<GroupSymbol> leftGroups = new HashSet<GroupSymbol>();
			HashSet<GroupSymbol> rightGroups = new HashSet<GroupSymbol>();
			jp.getLeftClause().collectGroups(leftGroups);
			jp.getRightClause().collectGroups(rightGroups);
			
			LinkedList<Expression> leftExpressions = new LinkedList<Expression>();
			LinkedList<Expression> rightExpressions = new LinkedList<Expression>();
			RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, leftExpressions, rightExpressions, jp.getJoinCriteria(), new LinkedList<Criteria>());
		    
			HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits = createGroupMap(leftExpressions, rightExpressions);
			if (!leftPk.isEmpty() && (jp.getJoinType() == JoinType.JOIN_INNER || jp.getJoinType() == JoinType.JOIN_LEFT_OUTER)) {
				findKeyPreserved(keyPreservingGroups, leftPk, crits, true, metadata, rightPk);
			} 
			if (!rightPk.isEmpty() && (jp.getJoinType() == JoinType.JOIN_INNER || jp.getJoinType() == JoinType.JOIN_RIGHT_OUTER)) {
				findKeyPreserved(keyPreservingGroups, rightPk, crits, false, metadata, leftPk);
			}
		}
	}

	private static HashMap<List<GroupSymbol>, List<HashSet<Object>>> createGroupMap(
			LinkedList<Expression> leftExpressions,
			LinkedList<Expression> rightExpressions) {
		HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits = new HashMap<List<GroupSymbol>, List<HashSet<Object>>>();
		
		for (int i = 0; i < leftExpressions.size(); i++) {
			Expression lexpr = leftExpressions.get(i);
			Expression rexpr = rightExpressions.get(i);
			if (!(lexpr instanceof ElementSymbol) || !(rexpr instanceof ElementSymbol)) {
				continue;
			}
			ElementSymbol les = (ElementSymbol)lexpr;
			ElementSymbol res = (ElementSymbol)rexpr;
			List<GroupSymbol> tbls = Arrays.asList(les.getGroupSymbol(), res.getGroupSymbol());
			List<HashSet<Object>> ids = crits.get(tbls);
			if (ids == null) {
				ids = Arrays.asList(new HashSet<Object>(), new HashSet<Object>());
				crits.put(tbls, ids);
			}
			ids.get(0).add(les.getMetadataID());
			ids.get(1).add(res.getMetadataID());
		}
		return crits;
	}

	static private HashSet<GroupSymbol> findKeyPreserved(Set<GroupSymbol> keyPreservingGroups,
		Set<GroupSymbol> pk,
		HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits, boolean left, QueryMetadataInterface metadata, Set<GroupSymbol> otherGroups)
		throws TeiidComponentException, QueryMetadataException {
		HashSet<GroupSymbol> result = new HashSet<GroupSymbol>();
		for (GroupSymbol gs : pk) {
			for (Map.Entry<List<GroupSymbol>, List<HashSet<Object>>> entry : crits.entrySet()) {
				if (!entry.getKey().get(left?0:1).equals(gs) || !otherGroups.contains(entry.getKey().get(left?1:0))) {
					continue;
				}
				if (RuleRaiseAccess.matchesForeignKey(metadata, entry.getValue().get(left?0:1), entry.getValue().get(left?1:0), gs, false)) {
					keyPreservingGroups.add(gs);
					result.add(entry.getKey().get(left?1:0));
				}
			}
		}
		return result;
	}

	/**
	 * This method will convert all elements in a command to their fully qualified name.
	 * @param command Command to convert
	 */
	public static void fullyQualifyElements(Command command) {
	    Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(command, false, true);
	    for (ElementSymbol element : elements) {
	        element.setDisplayFullyQualified(true);
	    }
	}
    
}
