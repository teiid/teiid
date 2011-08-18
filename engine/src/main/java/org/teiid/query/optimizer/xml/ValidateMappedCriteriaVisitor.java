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

package org.teiid.query.optimizer.xml;

import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.mapping.xml.MappingCriteriaNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.visitor.StaticSymbolMappingVisitor;


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
            element.setCriteriaNode(criteria);
        }
    }

    public void visit(MappingRecursiveElement element) {
        String criteriaStr = element.getCriteria();
        Map symbolMap = element.getSourceNode().buildFullSymbolMap();
        Criteria criteria = resolveCriteria(criteriaStr, symbolMap);
        if (criteria != null) {
            element.setCriteriaNode(criteria);
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
                throw new TeiidRuntimeException(e);
            }
        }
        return null;
    }
    
    public static void validateAndCollectCriteriaElements(MappingDocument doc, XMLPlannerEnvironment planEnv) 
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        
        try {
            ValidateMappedCriteriaVisitor visitor = new ValidateMappedCriteriaVisitor(planEnv);
            doc.acceptVisitor(new Navigator(true, visitor));
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof QueryPlannerException) {
                throw (QueryPlannerException)e.getCause();
            }
            else if (e.getCause() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getCause();
            }
            else if (e.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getCause();
            }
            throw e;
        }
    }     
}
