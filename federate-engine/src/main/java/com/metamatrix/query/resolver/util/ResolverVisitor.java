/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.QueryPlugin;
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
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
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
    
    private Collection groups;
    private GroupContext externalContext;
    protected QueryMetadataInterface metadata;
    private MetaMatrixComponentException componentException;
    private QueryResolverException resolverException;
    
    /**
     * Constructor for ResolveElementsVisitor.
     * 
     * External groups are ordered from inner to outer most
     */
    public ResolverVisitor(QueryMetadataInterface metadata, Collection internalGroups, GroupContext externalContext) {
        this.groups = internalGroups;
        this.externalContext = externalContext;
        this.metadata = metadata;
    }

    /**
     * Constructor for ResolveElementsVisitor.
     */
    public ResolverVisitor(QueryMetadataInterface metadata, Collection internalGroups) {
        this(metadata, internalGroups, null);
    }
    
	public void setGroups(Collection groups) {
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
                LinkedList matchedGroups = new LinkedList();
                
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
            Collection matchedGroups = ResolverUtil.findMatchingGroups(groupContext, root.getGroups(), metadata);
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
                                  Collection matchedGroups) throws MetaMatrixComponentException,
                                                           QueryMetadataException {
        Assertion.assertTrue(matchedGroups.size() == 1);
        GroupSymbol group = (GroupSymbol)matchedGroups.iterator().next();
        ElementSymbol resolvedSymbol = new ElementSymbol(potentialID);
        resolvedSymbol.setGroupSymbol(group);
        String elementID = metadata.getFullElementName( metadata.getFullName(group.getMetadataID()), elementShortName );
        resolvedSymbol.setMetadataID( metadata.getElementID(elementID) );
        resolvedSymbol.setType( DataTypeManager.getDataTypeClass(metadata.getElementType(resolvedSymbol.getMetadataID())) );
        matches.add(new ElementMatch(resolvedSymbol, group));
    }

    private void resolveAgainstGroups(String elementShortName,
                                      Collection matchedGroups, LinkedList<ElementMatch> matches) throws QueryMetadataException,
                                                         MetaMatrixComponentException {
        for (Iterator i = matchedGroups.iterator(); i.hasNext();) {
            GroupSymbol group = (GroupSymbol)i.next();
            
            GroupInfo groupInfo = ResolverUtil.getGroupInfo(group, metadata);
            
            ElementSymbol result = groupInfo.getSymbol(elementShortName);
            if (result != null) {
            	matches.add(new ElementMatch(result, group));
            }
        }
    }
        
    public void visit(BetweenCriteria obj) {
        try {
            ResolverVisitorUtil.resolveBetweenCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(CompareCriteria obj) {
        try {
            ResolverVisitorUtil.resolveCompareCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(MatchCriteria obj) {
        try {
            ResolverVisitorUtil.resolveMatchCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SetCriteria obj) {
        try {
            ResolverVisitorUtil.resolveSetCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SubqueryCompareCriteria obj) {
        try {
            obj.setLeftExpression(ResolverVisitorUtil.resolveSubqueryPredicateCriteria(obj.getLeftExpression(), obj));
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }


    public void visit(SubquerySetCriteria obj) {
        try {
            obj.setExpression(ResolverVisitorUtil.resolveSubqueryPredicateCriteria(obj.getExpression(), obj));
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(IsNullCriteria obj) {
        try {
            ResolverVisitorUtil.resolveIsNullCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }
    
    public void visit(Function obj) {
        try {
            ResolverVisitorUtil.resolveFunction(obj, metadata);
        } catch(QueryResolverException e) {
            handleException(e);
        } catch(MetaMatrixComponentException e) {
            handleException(e);
        }
    }

    public void visit(CaseExpression obj) {
        try {
            ResolverVisitorUtil.resolveCaseExpression(obj);

        } catch(QueryResolverException e) {
            handleException(e);
        } 
    }
    
    public void visit(SearchedCaseExpression obj) {
        try {
            ResolverVisitorUtil.resolveSearchedCaseExpression(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        } 
    }
    
    public void visit(SetClause obj) {
    	String type = DataTypeManager.getDataTypeName(obj.getSymbol().getType());
    	try {
    		ResolverUtil.setTypeIfReference(obj.getValue(), obj.getSymbol().getType(), obj);
            obj.setValue(ResolverUtil.convertExpression(obj.getValue(), type));                    
        } catch(QueryResolverException e) {
            handleException(new QueryResolverException(e, QueryPlugin.Util.getString("SetClause.resolvingError", new Object[] {obj.getValue(), obj.getSymbol(), type})));
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

    public static void resolveLanguageObject(LanguageObject obj, QueryMetadataInterface metadata)
        throws MetaMatrixComponentException, QueryResolverException {
        ResolverVisitor.resolveLanguageObject(obj, null, metadata);
    }

    public static void resolveLanguageObject(LanguageObject obj, Collection groups, QueryMetadataInterface metadata)
        throws MetaMatrixComponentException, QueryResolverException {
        ResolverVisitor.resolveLanguageObject(obj, groups, null, metadata);
    }

    public static void resolveLanguageObject(LanguageObject obj, Collection groups, GroupContext externalContext, QueryMetadataInterface metadata)
        throws MetaMatrixComponentException, QueryResolverException {

        if(obj == null) {
            return;
        }

        // Resolve elements, deal with errors
        ResolverVisitor elementsVisitor = new ResolverVisitor(metadata, groups, externalContext);
        PostOrderNavigator.doVisit(obj, elementsVisitor);
        if(elementsVisitor.getComponentException() != null) {
            throw elementsVisitor.getComponentException();
        }

        if(elementsVisitor.getResolverException() != null) {
            throw elementsVisitor.getResolverException();
        }

    }
    
}
