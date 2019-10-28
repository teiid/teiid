/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.optimizer.relational.rules;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.SourceHint;
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

    public static final String CONFORMED_SOURCES = AbstractMetadataRecord.RELATIONAL_PREFIX + "conformed-sources"; //$NON-NLS-1$
    public static final String RECONTEXT_STRING = "__"; //$NON-NLS-1$

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
            addAlias(sourceNode, context, groups, metadata);
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

            copyProperties(sourceNode, accessNode);
            SourceHint sourceHint = (SourceHint)sourceNode.removeProperty(Info.SOURCE_HINT);
            //TODO: trim the hint to only the sources possible under this model (typically 1, but could be more in
            //multi-source
            accessNode.setProperty(Info.SOURCE_HINT, sourceHint);
            Object hint = sourceNode.removeProperty(NodeConstants.Info.IS_OPTIONAL);
            if (hint != null) {
                accessNode.setProperty(NodeConstants.Info.IS_OPTIONAL, hint);
            }

            Object modelId = null;
            if (sourceNode.getGroups().size() == 1) {
                GroupSymbol gs = sourceNode.getGroups().iterator().next();
                modelId = gs.getModelMetadataId();
                if (modelId != null) {
                    accessNode.setProperty(NodeConstants.Info.MODEL_ID, modelId);
                }
            }
            if (req instanceof Create || req instanceof Drop) {
                modelId = TempMetadataAdapter.TEMP_MODEL;
            } else {
                modelId = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
            }
            if (modelId != null) {
                boolean multiSource = metadata.isMultiSource(modelId);
                if (multiSource) {
                    accessNode.setProperty(Info.IS_MULTI_SOURCE, multiSource);
                }
                accessNode.setProperty(NodeConstants.Info.MODEL_ID, modelId);
            }

            if (req == null && modelId != null) {
                //add "conformed" sources if they exist
                GroupSymbol group = sourceNode.getGroups().iterator().next();
                Object gid = group.getMetadataID();
                String sources = metadata.getExtensionProperty(gid, CONFORMED_SOURCES, false);
                if (sources != null) {
                    Set<Object> conformed = new LinkedHashSet<Object>();
                    conformed.add(modelId);
                    for (String source : StringUtil.split(sources, ",")) { //$NON-NLS-1$
                        Object mid = metadata.getModelID(source.trim());
                        if (metadata.isVirtualModel(mid)) {
                            //TODO: could validate this up-front
                            throw new QueryMetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31148, metadata.getName(mid), group));
                        }
                        conformed.add(mid);
                    }
                    accessNode.setProperty(Info.CONFORMED_SOURCES, conformed);
                }
            }

            // Insert
            sourceNode.addAsParent(accessNode);

            apNode = accessNode;

            // set additional information
            for (GroupSymbol group : accessNode.getGroups()) {
                if (group.getCheckMatViewStatus() != null) {
                    LinkedHashSet<Object> viewsToCheck = new LinkedHashSet<Object>();
                    viewsToCheck.add(group.getCheckMatViewStatus());
                    accessNode.setProperty(Info.CHECK_MAT_VIEW, viewsToCheck);
                }
                Object modelID = metadata.getModelID(group.getMetadataID());
                if (CapabilitiesUtil.requiresCriteria(modelID, metadata, finder)) {
                    additionalRules[1] = true;
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
    private void addAlias(PlanNode sourceNode, CommandContext cc,
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

        if (groups.add(group.getName())) {
            if (group.getDefinition() != null) {
                cc.getAliasMapping().put(group.getName(), group.getName());
            }
            return; // this is the first instance of the group
        }

        List<PlanNode> childProjects = null;
        if (sourceNode.getChildCount() > 0) {
            childProjects = NodeEditor.findAllNodes(sourceNode.getFirstChild(),
                                                    NodeConstants.Types.PROJECT,
                                                    NodeConstants.Types.SOURCE);
        }

        GroupSymbol newGroup = recontextSymbol(group, groups);

        if (group.getDefinition() != null) {
            cc.getAliasMapping().put(newGroup.getName(), group.getName());
        }

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
     * @param names a case insensitive set of all known groups
     * @return
     */
    public static GroupSymbol recontextSymbol(GroupSymbol oldSymbol,
                                              Set<String> names) {
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
        } while (!names.add(newName));

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

    static void copyProperties(PlanNode node,
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
        Set<Object> toCheck = (Set<Object>) node.getProperty(NodeConstants.Info.CHECK_MAT_VIEW);
        if (toCheck != null) {
            Set<Object> existing = (Set<Object>) copyTo.getProperty(NodeConstants.Info.CHECK_MAT_VIEW);
            if (existing != null) {
                existing.addAll(toCheck);
            } else {
                copyTo.setProperty(NodeConstants.Info.CHECK_MAT_VIEW, new LinkedHashSet<Object>(toCheck));
            }
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
     * @throws QueryMetadataException
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
