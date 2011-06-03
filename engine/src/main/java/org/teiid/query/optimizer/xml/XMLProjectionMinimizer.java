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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingCriteriaNode;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SelectSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;

public class XMLProjectionMinimizer {

	/**
	 * Implements projection minimization through two passes over the document
	 */
	static void minimizeProjection(final XMLPlannerEnvironment planEnv) {
	    final HashMap<MappingSourceNode, LinkedHashSet<ElementSymbol>> neededElements = new HashMap<MappingSourceNode, LinkedHashSet<ElementSymbol>>();
	    
	    //collect included elements
	    MappingVisitor visitor = new Navigator(true, new MappingVisitor() {
	    	@Override
	    	public void visit(MappingAttribute attribute) {
	    		collectElementSymbol(attribute);
	    	}
	
			private void collectElementSymbol(MappingNode node) {
				if (node.isExcluded() || node.getElementSymbol() == null) {
	    			return;
	    		}
	    		MappingSourceNode msn = node.getSourceNode();
	    		ElementSymbol es = node.getElementSymbol();
	    		collectElementSymbol(msn, es);
			}
	
			private void collectElementSymbol(
					MappingSourceNode msn, ElementSymbol es) {
				msn = getActualSourceNode(msn);
				LinkedHashSet<ElementSymbol> elems = neededElements.get(msn);
	    		if (elems == null) {
	    			elems = new LinkedHashSet<ElementSymbol>();
	    			neededElements.put(msn, elems);
	    		}
	    		elems.add(es);
			}
	    	
	    	@Override
	    	public void visit(MappingElement element) {
	    		collectElementSymbol(element);
	    	}
	    	
	    	@Override
	    	public void visit(MappingSourceNode element) {
	    		try {
	        		QueryNode node = QueryUtil.getQueryNode(element.getResultName(), planEnv.getGlobalMetadata());
	                            
	                Collection<ElementSymbol> bindings = QueryUtil.getBindingElements(node);
	                MappingSourceNode parent = element.getParentSourceNode();
	                collectElementSymbols(element, bindings, parent);
	    		} catch (TeiidException e) {
	    			throw new TeiidRuntimeException(e);
	    		}
	    	}
	
			private void collectElementSymbols(
					MappingSourceNode element,
					Collection<ElementSymbol> bindings, MappingSourceNode parent) {
				for (ElementSymbol elementSymbol : bindings) {
					if (element != null) {
						elementSymbol = element.getMappedSymbol(elementSymbol);
					}
					while (parent != null) {
						if (parent.getActualResultSetName().equalsIgnoreCase(elementSymbol.getGroupSymbol().getNonCorrelationName())) {
							collectElementSymbol(parent, elementSymbol);
							break;
						}
						parent = parent.getParentSourceNode();
					}
				}
			}
	    	
	    	@Override
	    	public void visit(MappingCriteriaNode element) {
	    		Criteria crit = element.getCriteriaNode();
	    		if (crit == null) {
	    			return;
	    		}
	    		collectElementSymbols(null, ElementCollectorVisitor.getElements(crit, true), element.getSourceNode());
	    	}
	    	
	    	@Override
	    	public void visit(MappingRecursiveElement element) {
	    		Criteria crit = element.getCriteriaNode();
	    		if (crit == null) {
	    			return;
	    		}
	    		collectElementSymbols(null, ElementCollectorVisitor.getElements(crit, true), element.getSourceNode());
	    	}
	
	    });
	    planEnv.mappingDoc.acceptVisitor(visitor);
	    
	    visitor = new Navigator(true, new MappingVisitor() {
	    	@Override
	    	public void visit(MappingSourceNode element) {
	    		try {
	        		ResultSetInfo rsInfo = element.getResultSetInfo();
	        		Query rsQuery = (Query)rsInfo.getCommand();
	        		if (rsQuery.getSelect().isDistinct()) {
	        			return;
	        		}
	        		LinkedHashSet<ElementSymbol> elements = neededElements.get(element);
	        		if (elements != null) {
	            		rsQuery.setSelect(new Select(LanguageObject.Util.deepClone(elements, ElementSymbol.class)));
	        		} else {
	        			String alias = element.getAliasResultName();
	        			if (alias == null) {
	        				rsQuery.setSelect(new Select(Arrays.asList(new ExpressionSymbol("foo", new Constant(1))))); //$NON-NLS-1$
	        			} else {
	        				MappingSourceNode actual = getActualSourceNode(element);
	        				elements = neededElements.get(actual);
	        				if (elements != null) {
								Map reverseMap = QueryUtil.createSymbolMap(new GroupSymbol(element.getAliasResultName()), 
										rsInfo.getResultSetName(),
										ResolverUtil.resolveElementsInGroup(QueryUtil.createResolvedGroup(element.getAliasResultName(), planEnv.getGlobalMetadata()), planEnv.getGlobalMetadata()));
								Select select = new Select(new ArrayList<SelectSymbol>(elements));
								ExpressionMappingVisitor.mapExpressions(select, reverseMap);
								rsQuery.setSelect(select);
							}
	        			}
	        		}
	    		} catch (TeiidException e) {
	    			throw new TeiidRuntimeException(e);
	    		}
	    	}

	    });
	    planEnv.mappingDoc.acceptVisitor(visitor);
	}
	
	private static MappingSourceNode getActualSourceNode(MappingSourceNode element) {
		if (element.getAliasResultName() == null) {
			return element;
		}
		String actual = element.getActualResultSetName();
		MappingSourceNode parent = element.getParentSourceNode();
		while (parent != null) {
			if (parent.getActualResultSetName().equalsIgnoreCase(actual) ) {
				return parent;
			}
			parent = parent.getParentSourceNode();
		}
		return null;
	}

}
