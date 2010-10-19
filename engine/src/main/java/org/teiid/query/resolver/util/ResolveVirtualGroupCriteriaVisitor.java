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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.proc.TranslateCriteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 */
public class ResolveVirtualGroupCriteriaVisitor extends LanguageVisitor {

    private List virtualGroup;

    private QueryMetadataInterface metadata;

    /**
     * Constructor for ResolveElementsVisitor with no specified groups.  In this
     * case every element's group will be looked up based on the group name.
     * @param iterator
     */
    public ResolveVirtualGroupCriteriaVisitor(GroupSymbol virtualGroup,  QueryMetadataInterface metadata) {
        this.virtualGroup = Arrays.asList(new Object[] {virtualGroup});
        this.metadata = metadata;
    }

    public void visit(TranslateCriteria obj) {
    	if(obj.hasTranslations()) {
    		Iterator transIter = obj.getTranslations().iterator();
    		while(transIter.hasNext()) {
				CompareCriteria ccrit = (CompareCriteria) transIter.next();
				ElementSymbol element = (ElementSymbol) ccrit.getLeftExpression();
				try {
                    ResolverVisitor.resolveLanguageObject(element, virtualGroup, metadata);
				} catch(QueryResolverException e) {
                    throw new TeiidRuntimeException(e);
				} catch(TeiidComponentException e) {
                    throw new TeiidRuntimeException(e);
				}
    		}
    	}
    }

    public void visit(CriteriaSelector obj) {
    	if(obj.hasElements()) {
			Iterator elmntIter = obj.getElements().iterator();
			while(elmntIter.hasNext()) {
				ElementSymbol virtualElement = (ElementSymbol) elmntIter.next();
                try {
                    ResolverVisitor.resolveLanguageObject(virtualElement, virtualGroup, metadata);
                } catch(QueryResolverException e) {
                    throw new TeiidRuntimeException(e);
                } catch(TeiidComponentException e) {
                    throw new TeiidRuntimeException(e);
                }
			}
    	}
    }

    public static void resolveCriteria(LanguageObject obj, GroupSymbol virtualGroup,  QueryMetadataInterface metadata)
        throws TeiidComponentException, QueryResolverException {
        if(obj == null) {
            return;
        }

        // Resolve elements, deal with errors
        ResolveVirtualGroupCriteriaVisitor resolveVisitor = new ResolveVirtualGroupCriteriaVisitor(virtualGroup, metadata);
        
        try {
            DeepPreOrderNavigator.doVisit(obj, resolveVisitor);
        } catch (TeiidRuntimeException e) {
            if (e.getChild() instanceof QueryResolverException) {
                throw (QueryResolverException)e.getChild();
            }
            if (e.getChild() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getChild();
            }
            throw e;
        }
    }

}
