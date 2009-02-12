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

package com.metamatrix.query.optimizer.relational;

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.metadata.*;
import com.metamatrix.query.optimizer.CommandTreeNode;
import com.metamatrix.query.optimizer.CommandTreeProcessor;
import com.metamatrix.query.optimizer.relational.plantree.*;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.*;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;

/**
 * Merge neighboring relational nodes by combining canonical plans 
 */
public class MergeTreeNodeProcessor implements CommandTreeProcessor {

	/**
	 * @see com.metamatrix.query.optimizer.CommandTreeProcessor#process(com.metamatrix.query.optimizer.CommandTreeNode)
	 */
	public CommandTreeNode process(CommandTreeNode node, QueryMetadataInterface metadata) 
	throws QueryMetadataException, MetaMatrixComponentException {
		MergeTreeNodeProcessor.processRecursive(node, metadata);
		return node;
	}

	/**
	 * Recursively, in depth-first order, examines the tree of CommandTreeNode
	 * objects.  
	 * @param node
	 * @param metadata
	 * @throws QueryMetadataException
	 * @throws MetaMatrixComponentException
	 */
	private static void processRecursive(CommandTreeNode node, QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException{

        // process children
		Iterator children = node.getChildren().iterator();
		while (children.hasNext()) {
			CommandTreeNode child = (CommandTreeNode) children.next();
			processRecursive(child, metadata);
		}
        
        // Use command-level metadata if necessary
        QueryMetadataInterface fullMetadata = metadata;
        Map commandMetadata = node.getCommand().getTemporaryMetadata();
        if(commandMetadata != null && commandMetadata.size() > 0) {
            fullMetadata = new TempMetadataAdapter(metadata, new TempMetadataStore(commandMetadata));    
        }
        
		// process node	
		List oldChildren = new ArrayList(node.getChildren());
		int childCount = oldChildren.size();
		for (int childIndex=0; childIndex < childCount; childIndex++){
			CommandTreeNode child = (CommandTreeNode)oldChildren.get(childIndex);                        
			checkNode(node, child, childIndex, fullMetadata);
		}
	}
	
	/**
	 * Checks if parent and child both contain relational commands.  If so, 
	 * they will be merged - unless the child node is a subquery of a
	 * criteria or scalar subquery of parent command; these have to remain unmerged.
	 * @param parent 
	 * @param child
	 * @param childIndex index of child in the parent's List of children
	 * @param metadata source of metadata
	 * @throws QueryMetadataException
	 * @throws MetaMatrixComponentException
	 */
	private static void checkNode(CommandTreeNode parent, CommandTreeNode child, int childIndex, QueryMetadataInterface metadata) 
	throws QueryMetadataException, MetaMatrixComponentException{
		if (parent.getCommandType() == CommandTreeNode.TYPE_RELATIONAL_COMMAND &&
		    child.getCommandType() == CommandTreeNode.TYPE_RELATIONAL_COMMAND){
		    	
			Command childCommand = child.getCommand();
			Command parentCommand = parent.getCommand();
			
			Iterator i = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(parentCommand).iterator();
			while (i.hasNext()) {
                SubqueryContainer crit = (SubqueryContainer) i.next();
				if (crit.getCommand() == childCommand){
					//Don't merge these two nodes
					return;
				}
			}

			removeChildNode(parent, child);
			mergeRelationalPlans(parent, child, metadata);
            mergeTempMetadata(childCommand, parentCommand);
		}
	}

    /**
     * Adds temp metadata (if any) of child command to temp metadata
     * (if any) of parent command.
     * @param childCommand 
     * @param parentCommand
     */
    private static void mergeTempMetadata(
        Command childCommand,
        Command parentCommand) {
        Map childTempMetadata = childCommand.getTemporaryMetadata();
        if (childTempMetadata != null && !childTempMetadata.isEmpty()){
            // Add to parent temp metadata
            Map parentTempMetadata = parentCommand.getTemporaryMetadata();
            if (parentTempMetadata == null){
                parentCommand.setTemporaryMetadata(new HashMap(childTempMetadata));
            } else {
                parentTempMetadata.putAll(childTempMetadata);
            }
        }
    }
	
	/**
	 * The canonical plan stored in child must be grafted onto the appropriate
	 * source node in the canonical plan of parent - since the other previous
     * children have already been merged, this must occur on the first eligible
     * node.
	 * @param parent a node containing a relational plan
	 * @param child a node containing a relational plan
	 * @param metadata source of metadata
	 */
	private static void mergeRelationalPlans(
		CommandTreeNode parent,
		CommandTreeNode child, 
		QueryMetadataInterface metadata) 
	throws QueryMetadataException, MetaMatrixComponentException {
			
		PlanNode parentPlan = (PlanNode)parent.getCanonicalPlan();
		Iterator sourceNodes = NodeEditor.findAllNodes(parentPlan, NodeConstants.Types.SOURCE).iterator();
		while (sourceNodes.hasNext()){
			PlanNode sourceNode = (PlanNode)sourceNodes.next();
            if(sourceNode.getChildCount()>0) {
                continue;
            }
            
            Command command = (Command)sourceNode.getProperty(NodeConstants.Info.NESTED_COMMAND);
            /* Defect 13296 - Check for UPDATE PROCEDURE Source nodes, since an update procedure
             * is also considered a virtual group, 
             */
            if(command == null || command.getType() == Command.TYPE_UPDATE_PROCEDURE) {
                continue;
            }

			GroupSymbol sourceGroup = sourceNode.getGroups().iterator().next();
			// Only source nodes representing virtual groups are eligible to
			// be merged with.
			if (metadata.isVirtualGroup(sourceGroup.getMetadataID())){
                // Attach nodes
                PlanNode childPlan = (PlanNode) child.getCanonicalPlan();
				sourceNode.addFirstChild(childPlan);
                
                // Create symbol map from virtual group to child plan
                List projectCols = child.getCommand().getProjectedSymbols();
                sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, SymbolMap.createSymbolMap(sourceNode.getGroups().iterator().next(), projectCols));
                
                // Combine hints
                combineHints(parent, child);
                
                // done        
				break;
			}
		}
	}
    
    /**
     * Method combineHints.
     * @param parent
     * @param child
     */
    private static void combineHints(CommandTreeNode parent, CommandTreeNode child) {
        PlanHints parentHints = (PlanHints) parent.getProperty(RelationalPlanner.HINTS);
        PlanHints childHints = (PlanHints) child.getProperty(RelationalPlanner.HINTS);

        if(childHints.hasCriteria) {
            parentHints.hasCriteria = true;
        }        
        if(childHints.hasJoin) {
            parentHints.hasJoin = true;
        }        
        if(childHints.hasSort) {
            parentHints.hasSort = true;
        }        
        if(childHints.hasVirtualGroups) {
            parentHints.hasVirtualGroups = true;
        }        
        if(childHints.isUpdate) {
            parentHints.isUpdate = true;
        }        
        if(childHints.isUpdate) {
            parentHints.isUpdate = true;
        }      
        if(childHints.hasSetQuery) {
            parentHints.hasSetQuery = true;
        }
        if(childHints.hasAggregates) {
            parentHints.hasAggregates = true;
        }
        if(childHints.makeDepGroups != null && childHints.makeDepGroups.size() > 0) {
            if(parentHints.makeDepGroups == null) {
                parentHints.makeDepGroups = childHints.makeDepGroups;
            } else {
                parentHints.makeDepGroups.addAll(childHints.makeDepGroups);
            }    
        }  
        if(childHints.makeNotDepGroups != null && childHints.makeNotDepGroups.size() > 0) {
            if(parentHints.makeNotDepGroups == null) {
                parentHints.makeNotDepGroups = childHints.makeNotDepGroups;
            } else {
                parentHints.makeNotDepGroups.addAll(childHints.makeNotDepGroups);
            }    
        }  
        if(childHints.hasLimit) {
            parentHints.hasLimit = true;
        }
        if(childHints.hasOptionalJoin) {
            parentHints.hasOptionalJoin = true;
        }
        if(childHints.hasRelationalProc) {
            parentHints.hasRelationalProc = true;
        }
    }
	        
	/**
	 * All of child's children become children of parent
	 */
	private static final void removeChildNode(CommandTreeNode parent, CommandTreeNode child) {
		Assertion.isNotNull(parent);
		Assertion.isNotNull(child);

		// Get all existing children from parent
		List newChildren = new LinkedList(parent.getChildren());

		// get child's children
		List orphans = child.getChildren();
        
		// Find the child being removed and replace with the child's children
		ListIterator childIter = newChildren.listIterator();
		while(childIter.hasNext()) {
			CommandTreeNode possibleChild = (CommandTreeNode) childIter.next();        
			if(possibleChild == child) {
				childIter.remove();
				child.setParent(null);
				Iterator orphanIter = orphans.iterator();
				while(orphanIter.hasNext()) {
					CommandTreeNode orphan = (CommandTreeNode) orphanIter.next();
					childIter.add(orphan);
				}                
			}
		}
        		
		// Remove all children from parent and re-add from newChildren
		Iterator removeIter = new LinkedList(parent.getChildren()).iterator();
		while(removeIter.hasNext()) {
			CommandTreeNode removeNode = (CommandTreeNode) removeIter.next();
			parent.removeChild(removeNode);
		}
		parent.addChildren(newChildren);
        		
		// set new children's parent to new parent
		Iterator iter = newChildren.iterator();
		while(iter.hasNext()) {
			CommandTreeNode newChild = (CommandTreeNode) iter.next();
			newChild.setParent(parent);
		}
        
		// Remove old children from child
		removeIter = new LinkedList(child.getChildren()).iterator();
		while(removeIter.hasNext()) {
			child.removeChild((CommandTreeNode)removeIter.next());
		}
	}
}
