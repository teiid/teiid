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

package org.teiid.query.optimizer.relational.rules;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


/**
 * This rule finds all SOURCE nodes and associates ACCESS patterns, ACCESS nodes, and aliases.
 */
public final class RulePlaceAccess implements
                                  OptimizerRule {

    private static final String RECONTEXT_STRING = "__"; //$NON-NLS-1$

    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryMetadataException,
                                                   TeiidComponentException,
                                                   QueryPlannerException {

        Set<String> groups = context.getGroups();

        boolean[] addtionalRules = new boolean[2];

        for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
            addAccessNode(metadata, sourceNode, capFinder, addtionalRules);
            addAlias(sourceNode, groups, metadata);
        }

        if (addtionalRules[0]) {
            rules.addLast(RuleConstants.ACCESS_PATTERN_VALIDATION);
        }
        if (addtionalRules[1]) {
            rules.addLast(RuleConstants.VALIDATE_WHERE_ALL);
        }
        return plan;
    }

    /**
     * Adds a access node if the node is a source leaf node.  
     * 
     * @param metadata
     * @param sourceNode
     * @return true if the source node has an access pattern
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private void addAccessNode(QueryMetadataInterface metadata,
                                  PlanNode sourceNode, CapabilitiesFinder finder, boolean[] additionalRules) throws QueryMetadataException,
                                                      TeiidComponentException {
        boolean isInsert = false;
        Object req = sourceNode.getProperty(NodeConstants.Info.ATOMIC_REQUEST);
        if (req == null) {
            req = sourceNode.getProperty(NodeConstants.Info.NESTED_COMMAND);
        }
        
        if (sourceNode.getProperty(NodeConstants.Info.TABLE_FUNCTION) != null) {
        	return;
        }

        if (req instanceof Insert) {
            isInsert = true;
        } else {
            PlanNode parent = sourceNode.getParent();
            if(parent.getType() == NodeConstants.Types.PROJECT && parent.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
                isInsert = true;
            }
        }

        PlanNode apNode = sourceNode;

        if (sourceNode.getChildCount() == 0) {
            // Create the access node and insert
            PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
            accessNode.addGroups(sourceNode.getGroups());

            copyDependentHints(sourceNode, accessNode);

            Object hint = sourceNode.removeProperty(NodeConstants.Info.IS_OPTIONAL);
            if (hint != null) {
                accessNode.setProperty(NodeConstants.Info.IS_OPTIONAL, hint);
            }
            
            Object modelId = sourceNode.removeProperty(NodeConstants.Info.MODEL_ID);
            if (modelId != null) {
                accessNode.setProperty(NodeConstants.Info.MODEL_ID, modelId);
            }

            // Insert
            sourceNode.addAsParent(accessNode);

            apNode = accessNode;
            
            // set hint as to if the model mandates a where clause
    		for (GroupSymbol group : accessNode.getGroups()) {
                Object modelID = metadata.getModelID(group.getMetadataID());
                if (CapabilitiesUtil.requiresCriteria(modelID, metadata, finder)) {
                	additionalRules[1] = true;
                	break;
                }
            }
        }

        // Add access pattern(s), if any, as property of access node
        if (!isInsert && addAccessPatternsProperty(apNode, metadata)) {
            additionalRules[0] = true;
        }
    }

    /**
     * Ensures that the group is uniquely named within the current optimizer run
     * 
     * @param sourceNode
     * @param groups
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     * @throws QueryPlannerException
     */
    private void addAlias(PlanNode sourceNode,
                          Set<String> groups,
                          QueryMetadataInterface metadata) throws QueryMetadataException,
                                                          TeiidComponentException,
                                                          QueryPlannerException {
        // select with no from
        if (sourceNode.getGroups().isEmpty()) {
            return;
        }

        // insert, update, delete, create, etc.
        if (FrameUtil.getNonQueryCommand(sourceNode.getParent()) != null) {
            return;
        }

        PlanNode parentProject = NodeEditor.findParent(sourceNode, NodeConstants.Types.PROJECT);

        // the source over a project into cannot conflict with any other groups
        if (parentProject.hasProperty(NodeConstants.Info.INTO_GROUP)) {
            return;
        }

        GroupSymbol group = sourceNode.getGroups().iterator().next();

        if (groups.add(group.getName().toUpperCase())) {
            return; // this is the first instance of the group
        }

        List<PlanNode> childProjects = null;
        if (sourceNode.getChildCount() > 0) {
            childProjects = NodeEditor.findAllNodes(sourceNode.getFirstChild(),
                                                    NodeConstants.Types.PROJECT,
                                                    NodeConstants.Types.SOURCE);
        }

        GroupSymbol newGroup = recontextSymbol(group, groups);

        //the expressions in the map will all be element symbols
        Map<ElementSymbol, Expression> replacementSymbols = FrameUtil.buildSymbolMap(group, newGroup, metadata);

        FrameUtil.convertFrame(sourceNode, group, new HashSet<GroupSymbol>(Arrays.asList(newGroup)), replacementSymbols, metadata);

        // correct the lower symbol map
        if (childProjects != null) {
            SymbolMap symbolMap = (SymbolMap)sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);

            SymbolMap replacementMap = new SymbolMap();
            for (Map.Entry<ElementSymbol, Expression> entry : symbolMap.asMap().entrySet()) {
                replacementMap.addMapping((ElementSymbol)replacementSymbols.get(entry.getKey()), entry.getValue());
            }
            sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, replacementMap);
        }

    }

    /**
     * Creates a uniquely named group symbol given the old symbol
     * 
     * @param oldSymbol
     * @param allUpperNames a set of all known groups in upper case
     * @return
     */
    public static GroupSymbol recontextSymbol(GroupSymbol oldSymbol,
                                              Set<String> allUpperNames) {
        // Create new unique name
        String oldName = oldSymbol.getName();
        int dotIndex = oldName.lastIndexOf("."); //$NON-NLS-1$
        if (dotIndex >= 0) {
            oldName = oldName.substring(dotIndex + 1);
        }

        int recontextNumber = 1;
        int recontextIndex = oldName.lastIndexOf(RECONTEXT_STRING);
        if (recontextIndex >= 0) {
            // Probably already been recontexted
            try {
                recontextNumber = Integer.parseInt(oldName.substring(recontextIndex + RECONTEXT_STRING.length())) + 1;
                oldName = oldName.substring(0, recontextIndex);
            } catch (Exception e) {
                // Ignore - ID must contain RECONTEXT_STRING already, so just append a new suffix
            }
        }

        // Ensure uniqueness within these frames
        String newName = null;
        do {
            newName = oldName + RECONTEXT_STRING + recontextNumber++;
        } while (!allUpperNames.add(newName.toUpperCase()));

        // Determine the definition
        String newDefinition = null;
        if (oldSymbol.getDefinition() == null) {
            newDefinition = oldSymbol.getName();
        } else {
            newDefinition = oldSymbol.getDefinition();
        }

        // Create the new symbol
        GroupSymbol newSymbol = oldSymbol.clone();
        newSymbol.setName(newName);
        newSymbol.setDefinition(newDefinition);
        return newSymbol;
    }

    static void copyDependentHints(PlanNode node,
                                   PlanNode copyTo) {
        // Copy the make dependent hint if necessary
        Object hint = node.getProperty(NodeConstants.Info.MAKE_DEP);
        if (hint != null) {
            copyTo.setProperty(NodeConstants.Info.MAKE_DEP, hint);
        }
        hint = node.getProperty(NodeConstants.Info.MAKE_NOT_DEP);
        if (hint != null) {
            copyTo.setProperty(NodeConstants.Info.MAKE_NOT_DEP, hint);
        }
        hint = node.getProperty(NodeConstants.Info.MAKE_IND);
        if (hint != null) {
            copyTo.setProperty(NodeConstants.Info.MAKE_IND, hint);
        }
    }

    /**
     * This method checks for access patterns and attaches those patterns as a property of the PlanNode. (The property will be a
     * Collection of Collections - each inner Collection will be the ElementSymbols comprising a single access pattern.)
     * 
     * @param node
     *            PlanNode
     * @param metadata
     *            source of metadata
     * @throws QueryMetadataException
     *             if there is an exception accessing metadata
     * @throws QueryResolverException
     *             if the GroupSymbol of an atomic command cannot be resolved
     * @throws TeiidComponentException
     *             indicating some unexpected non-business exception
     */
    static boolean addAccessPatternsProperty(final PlanNode node,
                                             final QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                                   TeiidComponentException {

        if (node.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
            return false;
        }

        // Each internal Collection is one access pattern
        List patternElements = ResolverUtil.getAccessPatternElementsInGroups(metadata, node.getGroups(), false);

        if (patternElements == null) {
            return false;
        }
        Collections.sort(patternElements);
        node.setProperty(NodeConstants.Info.ACCESS_PATTERNS, patternElements);
        return true;
    }

    /**
     * Return rule name
     * 
     * @return Rule name
     */
    public String toString() {
        return "PlaceAccess"; //$NON-NLS-1$
    }

}
