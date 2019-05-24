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

package org.teiid.query.optimizer.relational.plantree;

import java.util.*;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


public class PlanNode {

    // --------------------- Node State --------------------------

    /** The type of node, as defined by NodeConstants.Types. */
    private int type;

    private boolean modified;

    /** The parent of this node, null if root. */
    private PlanNode parent;

    /** Child nodes, usually just 1 or 2, but occasionally more */
    private LinkedList<PlanNode> children = new LinkedList<PlanNode>();

    private List<PlanNode> childrenView = Collections.unmodifiableList(children);

    /** Type-specific node properties, as defined in NodeConstants.Info. */
    private Map<NodeConstants.Info, Object> nodeProperties;

    // --------------------- Planning Info --------------------------

    /** The set of groups that this node deals with. */
    private Set<GroupSymbol> groups = new LinkedHashSet<GroupSymbol>();

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    public PlanNode() {
    }

    // =========================================================================
    //                     A C C E S S O R      M E T H O D S
    // =========================================================================

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public PlanNode getParent() {
        return parent;
    }

    private void setParent(PlanNode parent) {
        if (this.parent != null) {
            this.parent.children.remove(this);
        }
        this.modified = true;
        this.parent = parent;
    }

    public List<PlanNode> getChildren() {
        return this.childrenView;
    }

    public List<PlanNode> removeAllChildren() {
        ArrayList<PlanNode> childrenCopy = new ArrayList<PlanNode>(children);
        for (Iterator<PlanNode> childIter = this.children.iterator(); childIter.hasNext();) {
            PlanNode child = childIter.next();
            childIter.remove();
            child.parent = null;
        }
        this.modified = true;
        return childrenCopy;
    }

    public int getChildCount() {
        return this.children.size();
    }

    public PlanNode getFirstChild() {
        if ( getChildCount() > 0 ) {
            return this.children.getFirst();
        }
        return null;
    }

    public PlanNode getLastChild() {
        if ( getChildCount() > 0 ) {
            return this.children.getLast();
        }
        return null;
    }

    public void addFirstChild(PlanNode child) {
        this.modified = true;
        this.children.addFirst(child);
        child.setParent(this);
    }

    public void addLastChild(PlanNode child) {
        this.modified = true;
        this.children.addLast(child);
        child.setParent(this);
    }

    public void addChildren(Collection<PlanNode> otherChildren) {
        for (PlanNode planNode : otherChildren) {
            this.addLastChild(planNode);
        }
    }

    public PlanNode removeFromParent() {
        this.modified = true;
        PlanNode result = this.parent;
        if (result != null) {
            result.removeChild(this);
        }
        return result;
    }

    public boolean removeChild(PlanNode child) {
        boolean result = this.children.remove(child);
        if (result) {
            child.parent = null;
            modified = true;
        }
        return result;
    }

    public Object getProperty(NodeConstants.Info propertyID) {
        if(nodeProperties == null) {
            return null;
        }
        Object result = nodeProperties.get(propertyID);
        if (result != null) {
            modified = true; //we may modify this object
        }
        return result;
    }

    public Object setProperty(NodeConstants.Info propertyID, Object value) {
        if(nodeProperties == null) {
            nodeProperties = new LinkedHashMap<NodeConstants.Info, Object>();
        }
        modified = true;
        return nodeProperties.put(propertyID, value);
    }

    public Object removeProperty(Object propertyID) {
        if(nodeProperties == null) {
            return null;
        }
        modified = true;
        return nodeProperties.remove(propertyID);
    }

    /**
     * Indicates if there is a non-null value for the property
     * key or not
     * @param propertyID one of the properties from {@link NodeConstants}
     * @return whether this node has a non-null value for that property
     */
    public boolean hasProperty(NodeConstants.Info propertyID) {
        return (getProperty(propertyID) != null);
    }

    /**
     * Indicates if there is a non-null and non-empty Collection value for the property
     * key or not
     * @param propertyID one of the properties from {@link NodeConstants} which is
     * known to be a Collection object of some sort
     * @return whether this node has a non-null and non-empty Collection
     * value for that property
     */
    public boolean hasCollectionProperty(NodeConstants.Info propertyID) {
        Collection<Object> value = (Collection<Object>)getProperty(propertyID);
        return (value != null && !value.isEmpty());
    }

    public void addGroup(GroupSymbol groupID) {
        modified = true;
        groups.add(groupID);
    }

    public void addGroups(Collection<GroupSymbol> newGroups) {
        modified = true;
        this.groups.addAll(newGroups);
    }

    public Set<GroupSymbol> getGroups() {
        return groups;
    }

    // =========================================================================
    //            O V E R R I D D E N    O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Print plantree structure starting at this node
     * @return String representing this node and all children under this node
     */
    public String toString() {
        StringBuilder str = new StringBuilder();
        getRecursiveString(str, 0, null);
        return str.toString();
    }

    /**
     * Get the single node in full or recursive which considers modifications.
     * @return String representing just this node
     */
    public String nodeToString(boolean recusive) {
        StringBuilder str = new StringBuilder();
        if (!recusive) {
            getNodeString(str, null);
        } else {
            getRecursiveString(str, 0, this.modified);
        }
        return str.toString();
    }

    // Define a single tab
    private static final String TAB = "  "; //$NON-NLS-1$

    private static void setTab(StringBuilder str, int tabStop) {
        for(int i=0; i<tabStop; i++) {
            str.append(TAB);
        }
    }

    void getRecursiveString(StringBuilder str, int tabLevel, Boolean mod) {
        setTab(str, tabLevel);
        getNodeString(str, mod);
        str.append(")\n");  //$NON-NLS-1$

        // Recursively add children at one greater tab level
        for (PlanNode child : children) {
            child.getRecursiveString(str, tabLevel+1, mod==null?null:child.modified);
        }
    }

    void getNodeString(StringBuilder str, Boolean mod) {
        str.append(NodeConstants.getNodeTypeString(this.type));
        str.append("(groups="); //$NON-NLS-1$
        str.append(this.groups);
        if (!Boolean.FALSE.equals(mod)) {
            if(nodeProperties != null) {
                str.append(", props="); //$NON-NLS-1$
                String props = nodeProperties.toString();
                if (props.length() > 100000) {
                    props = props.substring(0, 100000) + "..."; //$NON-NLS-1$
                }
                str.append(props);
            }
            if (Boolean.TRUE.equals(mod)) {
                modified = false;
            }
        }
    }

    public boolean hasBooleanProperty(NodeConstants.Info propertyKey) {
        return Boolean.TRUE.equals(getProperty(propertyKey));
    }

    public void replaceChild(PlanNode child, PlanNode replacement) {
        modified = true;
        int i = this.children.indexOf(child);
        this.children.set(i, replacement);
        child.setParent(null);
        replacement.setParent(this);
    }

    /**
     * Add the node as this node's parent.
     * @param node
     */
    public void addAsParent(PlanNode node) {
        modified = true;
        if (this.parent != null) {
            this.parent.replaceChild(this, node);
        }
        assert node.getChildCount() == 0;
        node.addLastChild(this);
    }

    public List<SymbolMap> getCorrelatedReferences() {
        List<SubqueryContainer<?>> containers = getSubqueryContainers();
        if (containers.isEmpty()) {
            return Collections.emptyList();
        }
        ArrayList<SymbolMap> result = new ArrayList<SymbolMap>(containers.size());
        for (SubqueryContainer<?> container : containers) {
            SymbolMap map = container.getCommand().getCorrelatedReferences();
            if (map != null) {
                result.add(map);
            }
        }
        return result;
    }

    public List<SymbolMap> getAllReferences() {
        List<SymbolMap> refMaps = new ArrayList<SymbolMap>(getCorrelatedReferences());
        refMaps.addAll(getExportedCorrelatedReferences());
        return refMaps;
    }

    public List<SymbolMap> getExportedCorrelatedReferences() {
        if (type != NodeConstants.Types.JOIN) {
            return Collections.emptyList();
        }
        LinkedList<SymbolMap> result = new LinkedList<SymbolMap>();
        for (PlanNode child : NodeEditor.findAllNodes(this, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS)) {
            SymbolMap references = (SymbolMap)child.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            if (references == null) {
                continue;
            }
            Set<GroupSymbol> correlationGroups = GroupsUsedByElementsVisitor.getGroups(references.getValues());
            PlanNode joinNode = NodeEditor.findParent(child, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
            while (joinNode != null) {
                if (joinNode.getGroups().containsAll(correlationGroups)) {
                    if (joinNode == this) {
                        result.add(references);
                    }
                    break;
                }
                joinNode = NodeEditor.findParent(joinNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
            }
        }
        return result;
    }

    public Set<ElementSymbol> getCorrelatedReferenceElements() {
        List<SymbolMap> maps = getCorrelatedReferences();

        if(maps.isEmpty()) {
            return Collections.emptySet();
        }
        HashSet<ElementSymbol> result = new HashSet<ElementSymbol>();
        for (SymbolMap symbolMap : maps) {
            List<Expression> values = symbolMap.getValues();
            for (Expression expr : values) {
                ElementCollectorVisitor.getElements(expr, result);
            }
        }
        return result;
    }

    public List<SubqueryContainer<?>> getSubqueryContainers() {
        Collection<? extends LanguageObject> toSearch = Collections.emptyList();
        switch (this.getType()) {
            case NodeConstants.Types.SELECT: {
                Criteria criteria = (Criteria) this.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                toSearch = Arrays.asList(criteria);
                break;
            }
            case NodeConstants.Types.PROJECT: {
                toSearch = (Collection) this.getProperty(NodeConstants.Info.PROJECT_COLS);
                break;
            }
            case NodeConstants.Types.JOIN: {
                toSearch = (List<Criteria>) this.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                break;
            }
            case NodeConstants.Types.SOURCE: {
                TableFunctionReference tfr = (TableFunctionReference)this.getProperty(NodeConstants.Info.TABLE_FUNCTION);
                if (tfr != null) {
                    toSearch = Arrays.asList(tfr);
                } else {
                    Command cmd = (Command) this.getProperty(Info.VIRTUAL_COMMAND);
                    if (cmd != null) {
                        toSearch = Arrays.asList(cmd);
                    }
                }
                break;
            }
            case NodeConstants.Types.GROUP: {
                SymbolMap groupMap = (SymbolMap)this.getProperty(Info.SYMBOL_MAP);
                toSearch = groupMap.getValues();
                break;
            }
            case NodeConstants.Types.SORT: {
                OrderBy orderBy = (OrderBy) this.getProperty(NodeConstants.Info.SORT_ORDER);
                if (orderBy != null) {
                    toSearch = orderBy.getOrderByItems();
                }
            }
        }
        return ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(toSearch);
    }

    public float getCardinality() {
        Float cardinality = (Float) this.getProperty(NodeConstants.Info.EST_CARDINALITY);
        if (cardinality == null) {
            return -1f;
        }
        return cardinality;
    }

    public void recordDebugAnnotation(String annotation, Object modelID, String resolution, AnalysisRecord record, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        if (record != null && record.recordAnnotations()) {
            boolean current = this.modified;
            this.modified = true;
            record.addAnnotation(Annotation.RELATIONAL_PLANNER, annotation + (modelID != null?" " + (metadata!=null?metadata.getName(modelID):modelID):""), resolution + " " + this.nodeToString(false), Priority.LOW); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            this.modified = current;
        }
    }

    @Override
    public PlanNode clone() {
        PlanNode node = new PlanNode();
        node.type = this.type;
        node.groups = new HashSet<GroupSymbol>(this.groups);
        if (this.nodeProperties != null) {
            node.nodeProperties = new LinkedHashMap<NodeConstants.Info, Object>(this.nodeProperties);
        }
        return node;
    }

}
