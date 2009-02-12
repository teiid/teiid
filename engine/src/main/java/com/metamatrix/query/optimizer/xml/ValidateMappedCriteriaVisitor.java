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

package com.metamatrix.query.optimizer.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.mapping.xml.MappingCriteriaNode;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.mapping.xml.MappingRecursiveElement;
import com.metamatrix.query.mapping.xml.MappingVisitor;
import com.metamatrix.query.mapping.xml.Navigator;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.sql.visitor.StaticSymbolMappingVisitor;

/**
 * Validate the criteria specified on the elements.
 */
public class ValidateMappedCriteriaVisitor extends MappingVisitor {
    XMLPlannerEnvironment planEnv;
    
    public ValidateMappedCriteriaVisitor( XMLPlannerEnvironment planEnv) {
        this.planEnv = planEnv;
    }
    
    public void visit(MappingCriteriaNode element) {
        String criteriaStr = element.getCriteria();
        Map symbolMap = element.getSourceNode().buildFullSymbolMap();
        Criteria criteria = resolveCriteria(criteriaStr, symbolMap);
        if (criteria != null) {
            List groupNames = getCriteriaGroups(criteria);
            element.setCriteriaNode(criteria);
            element.setGroupsInCriteria(groupNames);
        }
    }

    public void visit(MappingRecursiveElement element) {
        String criteriaStr = element.getCriteria();
        Map symbolMap = element.getSourceNode().buildFullSymbolMap();
        Criteria criteria = resolveCriteria(criteriaStr, symbolMap);
        if (criteria != null) {
            List groupNames = getCriteriaGroups(criteria);
            element.setCriteriaNode(criteria);
            element.setGroupsInCriteria(groupNames);
        }
    }

    Criteria resolveCriteria(String criteriaString, Map symbolMap) {
        if (criteriaString != null && criteriaString.length() > 0) {
            try {
                Criteria crit = QueryParser.getQueryParser().parseCriteria(criteriaString);                               
                StaticSymbolMappingVisitor.mapSymbols(crit, symbolMap);
                ResolverVisitor.resolveLanguageObject(crit, null, planEnv.getGlobalMetadata());
                return crit;
            } catch (Exception e) {
                throw new MetaMatrixRuntimeException(e);
            }
        }
        return null;
    }
    
    private static List getCriteriaGroups(Criteria criteria) {
        Collection criteriaGroups = GroupsUsedByElementsVisitor.getGroups(criteria);
        List names = new ArrayList(criteriaGroups.size());
        Iterator iter = criteriaGroups.iterator();
        while(iter.hasNext()) {
            names.add( ((GroupSymbol)iter.next()).getName().toUpperCase() );
        }
        return names;
    }     
    
    
    public static void validateAndCollectCriteriaElements(MappingDocument doc, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        
        try {
            ValidateMappedCriteriaVisitor visitor = new ValidateMappedCriteriaVisitor(planEnv);
            doc.acceptVisitor(new Navigator(true, visitor));
        } catch (MetaMatrixRuntimeException e) {
            if (e.getCause() instanceof QueryPlannerException) {
                throw (QueryPlannerException)e.getCause();
            }
            else if (e.getCause() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getCause();
            }
            else if (e.getCause() instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException)e.getCause();
            }
            throw e;
        }
    }     
}
