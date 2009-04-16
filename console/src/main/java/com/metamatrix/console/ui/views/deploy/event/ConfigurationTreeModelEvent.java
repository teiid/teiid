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

//#############################################################################
package com.metamatrix.console.ui.views.deploy.event;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.console.ui.views.deploy.model.ConfigurationTreeModel.HostWrapper;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

/**
 * The <code>ConfigurationTreeModelEvent</code> is used to notify
 * {@link ConfigurationTreeModelListener}s that a change in a tree model
 * has occurred.
 * @since Golden Gate
 * @version 1.0
 * @author Dan Florian
 */
public final class ConfigurationTreeModelEvent
    extends ConfigurationChangeEvent {

    ///////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////

    /** Ancestor nodes starting with the parent. */
    private DefaultTreeNode[] ancestorNodes;

    /** The configuration node where the change occurred. */
    private DefaultTreeNode sourceNode;

    /** The configuration node where the change occurred. */
    private DefaultTreeNode configNode;

    ///////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////

    /**
     * Constructs a <code>ConfigurationTreeModelEvent</code> of the given type.
     * @param theType the event type
     * @param theChangedNode the node whose state has changed
     * @param theConfigNode the configuration node where the change occurred
     * @param theAncestorNodes the ancestor nodes starting with the parent
     * @throws IllegalArgumentException if type is not valid, if the
     * changed node content is <code>null</code>, or if the configuration node
     * content is <code>null</code>.
     * @throws NullPointerException if the changed node is <code>null</code>,
     * or if the configuration node is <code>null</code>.
     */
    public ConfigurationTreeModelEvent(
        int theType,
        DefaultTreeNode theChangedNode,
        DefaultTreeNode theConfigNode,
        DefaultTreeNode[] theAncestorNodes)
        throws IllegalArgumentException,
               NullPointerException {

        super(theType,
              theChangedNode.getContent(),
              (Configuration)theConfigNode.getContent());

        sourceNode = theChangedNode;
        configNode = theConfigNode;
        ancestorNodes = theAncestorNodes;

        if (ancestorNodes != null) {
            Object[] ancestors = new Object[theAncestorNodes.length];
            for (int i=0; i<theAncestorNodes.length; i++) {
                Object ancestor = theAncestorNodes[i].getContent();
//                if (ancestor instanceof PscWrapper) {
//                    ancestor = ((PscWrapper)ancestor).getPsc();
//                }
//                else 
                if (ancestor instanceof HostWrapper) {
                    ancestor = ((HostWrapper)ancestor).getHost();
                }
                ancestors[i] = ancestor;
            }
            setAncestors(ancestors);
        }
    }

    ///////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////

    /**
     * Gets the ancestors of the changed object.
     * @return the ancestor nodes
     */
    public DefaultTreeNode[] getAncestorNodes() {
        return ancestorNodes;
    }

    /**
     * Gets the configuration node where the change took place.
     * @return the configuration node
     */
    public DefaultTreeNode getConfigurationNode() {
        return configNode;
    }

    /**
     * Gets the deployed PSC node if either a deployed PSC node was the event
     * source or an ancestor.
     * @return the deployed PSC node or <code>null</code>
     */
    public DefaultTreeNode getDeployedPscNode() {
        DefaultTreeNode pscNode = null;
        if (isDeployedPscChange()) {
            pscNode = (DefaultTreeNode)getSource();
        }
        else {
            int index = getAncestorIndex(DEPLOYED_PSC);
            if (index != -1) {
                pscNode = ancestorNodes[index];
            }
        }
        return pscNode;
    }

    /**
     * Gets the deployed service node if either a deployed service node was
     * the event source or an ancestor.
     * @return the deployed service node or <code>null</code>
     */
    public DefaultTreeNode getDeployedServiceNode() {
        DefaultTreeNode serviceNode = null;
        if (isDeployedServiceChange()) {
            serviceNode = (DefaultTreeNode)getSource();
        }
        return serviceNode;
    }

    /**
     * Gets the host node if either a host node was the event source or an ancestor.
     * @return the host node or <code>null</code>
     */
    public DefaultTreeNode getHostNode() {
        DefaultTreeNode hostNode = null;
        if (isHostChange()) {
            hostNode = (DefaultTreeNode)getSource();
        }
        else {
            int index = getAncestorIndex(HOST);
            if (index != -1) {
                hostNode = ancestorNodes[index];
            }
        }
        return hostNode;
    }

    /**
     * Gets the process node if either a process node was the event source or
     * an ancestor.
     * @return the process node or <code>null</code>
     */
    public DefaultTreeNode getProcessNode() {
        DefaultTreeNode processNode = null;
        if (isProcessChange()) {
            processNode = (DefaultTreeNode)getSource();
        }
        else {
            int index = getAncestorIndex(PROCESS);
            if (index != -1) {
                processNode = ancestorNodes[index];
            }
        }
        return processNode;
    }

    /**
     * Gets the product node if either a product node was the event source or
     * an ancestor.
     * @return the product node or <code>null</code>
     */
    public DefaultTreeNode getProductNode() {
        DefaultTreeNode productNode = null;
        if (isProductChange()) {
            productNode = (DefaultTreeNode)getSource();
        }
        else {
            int index = getAncestorIndex(PRODUCT);
            if (index != -1) {
                productNode = ancestorNodes[index];
            }
        }
        return productNode;
    }

    /**
     * Gets the PSC definition node if either a PSC definition node was the
     * event source or an ancestor.
     * @return the PSC definition node or <code>null</code>
     */
    public DefaultTreeNode getPscDefinitionNode() {
        DefaultTreeNode pscNode = null;
        if (isPscDefinitionChange()) {
            pscNode = (DefaultTreeNode)getSource();
        }
        else {
            int index = getAncestorIndex(PSC_DEFN);
            if (index != -1) {
                pscNode = ancestorNodes[index];
            }
        }
        return pscNode;
    }

    /**
     * Gets the service definition node if either a service definition node was
     * the event source or an ancestor.
     * @return the service definition node or <code>null</code>
     */
    public DefaultTreeNode getServiceDefinitionNode() {
        DefaultTreeNode serviceNode = null;
        if (isServiceDefinitionChange()) {
            serviceNode = (DefaultTreeNode)getSource();
        }
        return serviceNode;
    }

    /**
     * The source node of the event.
     * @return the source node
     */
    public DefaultTreeNode getSourceNode() {
        return sourceNode;
    }

    /**
     * The source node of the event.
     * @return the source node
     */
    public DefaultTreeNode getSourceNodeParent() {
        return ancestorNodes[0];
    }

    /**
     * Sets the ancestor object of the changed object.
     * @param theAncestors the ancestor objects
     */
    protected void setAncestors(Object[] theAncestors) {
        ancestors = theAncestors;
        Object source = getSource();

//        if (source instanceof PscWrapper) {
//            if (ancestors[0] instanceof ProductType) {
//                type |= PSC_DEFN;
//            }
//            else {
//                type |= DEPLOYED_PSC;
//            }
//        }
//        else 
        if (source instanceof HostWrapper) {
            type |= HOST;
        }
        else {
            super.setAncestors(theAncestors);
        }
    }

}
