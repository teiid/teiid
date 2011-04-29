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

package org.teiid.query.resolver.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.StringUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.AllInGroupSymbol;
import org.teiid.query.sql.symbol.AllSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SelectSymbol;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;


/**
 */
public class XMLQueryResolver implements CommandResolver {

    /**
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, TempMetadataAdapter, boolean)
     */
	public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
		throws QueryMetadataException, QueryResolverException, TeiidComponentException {

		Query query = (Query) command;

		// set isXML flag
		query.setIsXML(true);

		// get the group on this query
		Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(query, true);
		GroupSymbol group = groups.iterator().next();

		//external groups
        GroupContext externalGroups = query.getExternalGroupContexts();

		// valid elements for select
		List<ElementSymbol> validSelectElems = getElementsInDocument(group, metadata);
		resolveXMLSelect(query, group, validSelectElems, metadata);

		// valid elements for criteria and order by
		Collection<ElementSymbol> validCriteriaElements = collectValidCriteriaElements(group, metadata);

		Criteria crit = query.getCriteria();
		OrderBy orderBy = query.getOrderBy();
        
        List<Command> commands = CommandCollectorVisitor.getCommands(query);
        for (Command subCommand : commands) {
            QueryResolver.setChildMetadata(subCommand, command);
            
            QueryResolver.resolveCommand(subCommand, metadata.getMetadata());
        }
        
		if(crit != null) {
			resolveXMLCriteria(crit, externalGroups, validCriteriaElements, metadata);
			// Resolve functions in current query
			ResolverVisitor.resolveLanguageObject(crit, metadata);
		}

		// resolve any orderby specified on the query
		if(orderBy != null) {
			resolveXMLOrderBy(orderBy, externalGroups, validCriteriaElements, metadata);
		}
        
        //we throw exceptions in these cases, since the clauses will not be resolved
        if (query.getGroupBy() != null) {
            throw new QueryResolverException(QueryPlugin.Util.getString("ERR.015.012.0031")); //$NON-NLS-1$
        }
        
        if (query.getHaving() != null) {
            throw new QueryResolverException(QueryPlugin.Util.getString("ERR.015.012.0032")); //$NON-NLS-1$
        }	
    }

    /**
     * Method resolveXMLSelect.
     * @param select Select clause in user command
     * @param group GroupSymbol
     * @param externalGroups Collection of external groups
     * @param validElements Collection of valid elements
     * @param metadata QueryMetadataInterface the metadata(for resolving criteria on temp groups)
     * @throws QueryResolverException if resolving order by fails
     * @throws QueryMetadataException if resolving fails
     * @throws TeiidComponentException if resolving fails
     */
	void resolveXMLSelect(Query query, GroupSymbol group, List<ElementSymbol> validElements, QueryMetadataInterface metadata)
		throws QueryMetadataException, TeiidComponentException, QueryResolverException {
        
        GroupContext externalGroups = null;

		Select select = query.getSelect();
		// Allow SELECT DISTINCT, which is ignored.  It is meaningless except for
		// self-entity relation using relate() functionality

		List elements = select.getSymbols();
		for (int i = 0; i < elements.size(); i++) {
			SelectSymbol ss = (SelectSymbol) elements.get(i);

			if (ss instanceof ElementSymbol) {
				// Here we make an assumption that: all elements named with "xml" must use qualified name
				// rather than a simple "xml" in order to distinguish it from "SELECT xml" and
				// "SELECT model.document.xml" case, both of whom stand for selecting the whole document.

				// Then "SELECT xml" or "SELECT model.document.xml" can only stand for one meaning with two cases:
				// 1) whole document
				// 2) whole document, root name = "xml", too

				// There are other cases of "xml", such as, element name = "xml",
				// but those are ok because those will be resolved later as normal elements
				String symbolName = ss.getName();
				if(symbolName.equalsIgnoreCase("xml") || symbolName.equalsIgnoreCase(group.getName() + ".xml")) { //$NON-NLS-1$ //$NON-NLS-2$
					if(elements.size() != 1) {
						throw new QueryResolverException(QueryPlugin.Util.getString("XMLQueryResolver.xml_only_valid_alone")); //$NON-NLS-1$
					}
					select.clearSymbols();
                    AllSymbol all = new AllSymbol();
                    all.setElementSymbols(validElements);
					select.addSymbol(all);
					query.setSelect(select);
					return;
				}
                // normal elements
				resolveElement((ElementSymbol)ss, validElements, externalGroups, metadata);
			} else if (ss instanceof AllInGroupSymbol) {
				// Resolve the element with "*" case. such as "A.*"
				// by stripping off the ".*" part,
				String symbolName = ss.getName();
				int index = symbolName.indexOf("*"); //$NON-NLS-1$
				String elementPart = symbolName.substring(0, index-1);

                // Check for case where we have model.doc.*
                if(elementPart.equalsIgnoreCase(group.getName())) {
                    select.clearSymbols();
                    AllSymbol all = new AllSymbol();
                    all.setElementSymbols(validElements);
                    select.addSymbol(all);
                    query.setSelect(select);
                } else {
                    // resovlve the node which is specified
                    ElementSymbol elementSymbol = new ElementSymbol(elementPart);
                    resolveElement(elementSymbol, validElements, externalGroups, metadata);

                    // now find all the elements under this node and set as elements.
                    List<ElementSymbol> elementsInNode = getElementsUnderNode(elementSymbol, validElements, metadata);
                    ((AllInGroupSymbol)ss).setElementSymbols(elementsInNode);
                }
			} else if (ss instanceof AllSymbol) {
                AllSymbol all =  (AllSymbol)ss;
                all.setElementSymbols(validElements);
				return;
			} else if (ss instanceof ExpressionSymbol) {
                throw new QueryResolverException(QueryPlugin.Util.getString("XMLQueryResolver.no_expressions_in_select")); //$NON-NLS-1$
            } else if (ss instanceof AliasSymbol) {
                throw new QueryResolverException("ERR.015.008.0070", QueryPlugin.Util.getString("ERR.015.008.0070")); //$NON-NLS-1$ //$NON-NLS-2$
            }
            
		}
	}
        
    /**
     * Collect all fully-qualified valid elements.  These can then be used to
     * validate elements used in the query.  It's easier to look up the valid
     * elements because the user is allow to used any partially-qualified name,
     * which makes the logic for doing lookups essentially impossible with the
     * existing metadata interface.
     * @param group Document group
     * @param metadata Metadata interface
     * @return Collection of ElementSymbol for each possible valid element
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     * @throws QueryResolverException
     */
    public static Collection<ElementSymbol> collectValidCriteriaElements(GroupSymbol group, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException, QueryResolverException {

        // Get all groups and elements
        List<ElementSymbol> validElements = getElementsInDocument(group, metadata);

        // Create GroupSymbol for temp groups and add to groups
        Collection tempGroups = metadata.getXMLTempGroups(group.getMetadataID());
        Iterator tempGroupIter = tempGroups.iterator();
        while(tempGroupIter.hasNext()) {
            Object tempGroupID = tempGroupIter.next();
            String name = metadata.getFullName(tempGroupID);
            GroupSymbol tempGroup = new GroupSymbol(name);
            tempGroup.setMetadataID(tempGroupID);

            validElements.addAll(ResolverUtil.resolveElementsInGroup(tempGroup, metadata));
        }
        return validElements;
    }


    /**
     * <p> Resolve the criteria specified on the XML query. The elements specified on the criteria should
     * be present on one of the mapping node objects passed to this method, or else be an element on a
     * temporary table at the root of the document model (if a temp table exists there).</p>
     * <p>A QueryResolverException will be thrown under the following circumstances:
     * <ol>
     * <li>the elements of the XML criteria cannot be resolved</li>
     * <li>the "@" attribute prefix is used to specify that the node is an attribute, but
     * a document node is found that is an element</li>
     * <li>an element is supplied in the criteria and is ambiguous (multiple
     * document nodes and/or root temp table elements exist which have that name)</li>
     * </ol></p>
     * <p>If an element is supplied in the criteria and is ambiguous (multiple document nodes and/or
     * root temp table elements of that name exist)
     * @param criteria The criteria object that should be resolved
     * @param group The group on the query.
     * @param metadata QueryMetadataInterface the metadata(for resolving criteria on temp groups)
     * @throws QueryResolverException if any of the above fail conditions are met
     */
    public static void resolveXMLCriteria(Criteria criteria,GroupContext externalGroups, Collection<ElementSymbol> validElements, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException, QueryResolverException {

        // Walk through each element in criteria and check against valid elements
        Collection<ElementSymbol> critElems = ElementCollectorVisitor.getElements(criteria, false);
        for (ElementSymbol critElem : critElems) {
            if(! critElem.isExternalReference()) {
                resolveElement(critElem, validElements, externalGroups, metadata);
            }
        }
    }

    /**
     * Resolve OrderBy clause specified on the XML Query.
     * @param orderBy Order By clause in user command
     * @param group GroupSymbol
     * @param externalGroups Collection of external groups
     * @param validElements Collection of valid elements
     * @param metadata QueryMetadataInterface the metadata(for resolving criteria on temp groups)
     * @throws QueryResolverException if resolving order by fails
     * @throws QueryMetadataException if resolving fails
     * @throws TeiidComponentException if resolving fails
     */
    static void resolveXMLOrderBy(OrderBy orderBy, GroupContext externalGroups, Collection<ElementSymbol> validElements, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException, QueryResolverException {

        // Walk through each element in OrderBy clause and check against valid elements
        Collection<ElementSymbol> orderElems = ElementCollectorVisitor.getElements(orderBy, false);
        for (ElementSymbol orderElem : orderElems) {
            resolveElement(orderElem, validElements, externalGroups, metadata);
        }
    }

	/**
	 * Resolve Element method.
	 * @param elem
	 * @param validElements
	 * @param externalGroups
	 * @param metadata
	 * @throws QueryResolverException
	 * @throws QueryMetadataException
	 * @throws TeiidComponentException
	 */
    static void resolveElement(ElementSymbol elem, Collection<ElementSymbol> validElements, GroupContext externalGroups, QueryMetadataInterface metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        
        // Get exact matching name
        String critElemName = elem.getName();
        String critElemNameSuffix = "." + elem.getCanonicalName(); //$NON-NLS-1$

        // Prepare results
        ElementSymbol exactMatch = null;
        List<ElementSymbol> partialMatches = new ArrayList<ElementSymbol>(2);     // anything over 1 is an error and should be rare

        //List of XML attributes that might match the criteria element,
        //if the criteria is specified without the optional "@" sign
        List<ElementSymbol> attributeMatches = new ArrayList<ElementSymbol>(2);

        // look up name based on ID match - will work for uuid version
        try {

            Object elementID = metadata.getElementID(critElemName);

            if(elementID != null) {
                critElemName = metadata.getFullName(elementID);
            }
        } catch(QueryMetadataException e) {
            //e.printStackTrace();
            // ignore and go on
        }

        // Walk through each valid element looking for a match
        for (ElementSymbol currentElem : validElements) {
            // Look for exact match
            if(currentElem.getName().equalsIgnoreCase(critElemName)) {
                exactMatch = currentElem;
                break;
            }

            if(currentElem.getName().toUpperCase().endsWith(critElemNameSuffix)) {
                partialMatches.add(currentElem);
            } else {
                // The criteria element might be referring to an
                // XML attribute, but might not have the optional
                // "@" sign
                String currentElemName = currentElem.getName();
                int atSignIndex = currentElemName.indexOf("@"); //$NON-NLS-1$
                if (atSignIndex != -1){
                    currentElemName = StringUtil.replace(currentElemName, "@", ""); //$NON-NLS-1$ //$NON-NLS-2$
                    if(currentElemName.equalsIgnoreCase(critElemName)) {
                        attributeMatches.add(currentElem);
                    } else {
                        currentElemName = currentElemName.toUpperCase();
                        if(currentElemName.endsWith(critElemNameSuffix)) {
                            attributeMatches.add(currentElem);
                        }
                    }
                }
            }
        }

        // Check for single partial match
        if(exactMatch == null){
            if (partialMatches.size() == 1) {
                exactMatch = partialMatches.get(0);
            } else if (partialMatches.size() == 0 && attributeMatches.size() == 1){
                exactMatch = attributeMatches.get(0);
            }
        }

        if(exactMatch != null) {
            String name = elem.getOutputName();
            // Resolve based on exact match
            elem.setShortName(exactMatch.getShortName());
            elem.setShortCanonicalName(exactMatch.getShortCanonicalName());
            elem.setMetadataID(exactMatch.getMetadataID());
            elem.setType(exactMatch.getType());
            elem.setGroupSymbol(exactMatch.getGroupSymbol());
            elem.setOutputName(name);
        } else if(partialMatches.size() == 0 && attributeMatches.size() == 0){
            try {
                ResolverVisitor.resolveLanguageObject(elem, Collections.EMPTY_LIST, externalGroups, metadata);
            } catch (QueryResolverException e) {
                throw new QueryResolverException(e, "ERR.015.008.0019", QueryPlugin.Util.getString("ERR.015.008.0019", critElemName)); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } else {
            // Found multiple matches
            throw new QueryResolverException("ERR.015.008.0020", QueryPlugin.Util.getString("ERR.015.008.0020", critElemName)); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    static List<ElementSymbol> getElementsInDocument(GroupSymbol group, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {
        return ResolverUtil.resolveElementsInGroup(group, metadata);
    }
    
    static List<ElementSymbol> getElementsUnderNode(ElementSymbol node, List<ElementSymbol> validElements, QueryMetadataInterface metadata) 
        throws TeiidComponentException, QueryMetadataException {
        
        List<ElementSymbol> elements = new ArrayList<ElementSymbol>();
        String nodeName = metadata.getFullName(node.getMetadataID());
        for (ElementSymbol validElement : validElements) {
            String qualifiedName = validElement.getName();
            if (qualifiedName.equals(nodeName) || qualifiedName.startsWith(nodeName+ElementSymbol.SEPARATOR)) {
                elements.add(validElement);
            }
        }
        return elements;
    }

}
