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
package com.metamatrix.console.ui.views.deploy.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.swing.SwingUtilities;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ui.tree.SortableChildrenNode;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationTreeModelEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationTreeModelListener;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;

import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeModel;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

/**
 * The <code>ConfigurationTreeModel</code> is the tree model used for
 * deployments and Product Service Configurations (PSC) configuration
 * objects.
 * @since Golden Gate
 * @version 1.0
 * @author Dan Florian
 */
public final class ConfigurationTreeModel
    extends DefaultTreeModel
    implements ConfigurationChangeListener {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    /** The text of all the deployements header nodes. */
    public static final String DEPLOYMENTS_HDR =
        DeployPkgUtils.getString("dtm.deploymentshdrnode"); //$NON-NLS-1$

    /** The text of all the PSC definitions header nodes. */
    public static final String PSC_DEFS_HDR =
        DeployPkgUtils.getString("dtm.pscdefshdrnode"); //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    //key=object, value=DefaultTreeNode
    private HashMap objNodeMap = new HashMap();

    //key=ConfigurationID, value=DefaultTreeNode for Deployments Header
    private HashMap configDeployHdrMap = new HashMap();

    //key=ConfigurationID, value=DefaultTreeNode for PSC Definitions Header
    private HashMap configPscDefHdrMap = new HashMap();

    //key=hostID, value=HashMap (key=ConfigurationID, value=node-HostWrapper as content)
    private HashMap hostConfigMap = new HashMap();

    //key=ComponentTypeID of product, value=HashMap (key=ConfigurationID, value=DefaultTreeNode)
    private HashMap prodConfigMap = new HashMap();

    // deployed pscs
    //key=VMComponentDefnID, value=(key=ProductServiceConfig ID,value=node-PscWrapper)
    private HashMap procPscMap = new HashMap();

    // psc defns
    //key=ProductServiceConfigID, value=(key=ConfigurationID, value=node-PscWrapper)
    private HashMap pscConfigMap = new HashMap();

    /** The root node. This node will not be shown. */
    private DefaultTreeNode root;

    /** A list of all configuration change listeners. */
    private ArrayList listeners = new ArrayList();

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Constructs and <code>ConfigurationTreeModel</code>.
     * @throws ExternalException if problems occur during construction
     */
    public ConfigurationTreeModel()
        throws ExternalException {

        super(new DefaultTreeNode());
        root = (DefaultTreeNode)getRoot();
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Adds a configuration node to the model.
     * @param theConfig the user object of the configuration node being created
     */
    private void addConfig(Configuration theConfig) {
        // create configuration node
        SortableChildrenNode configNode = createNode(theConfig, root);

        // add deployments header
        SortableChildrenNode deployHdrNode = new SortableChildrenNode(DEPLOYMENTS_HDR, theConfig);
        configNode.addChild(deployHdrNode);
        configDeployHdrMap.put(theConfig.getID(), deployHdrNode);

        // add psc defs header
        SortableChildrenNode pscDefsHdrNode = new SortableChildrenNode(PSC_DEFS_HDR, theConfig);
        configNode.addChild(pscDefsHdrNode);
        configPscDefHdrMap.put(theConfig.getID(), pscDefsHdrNode);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            configNode,
                                            configNode,
                                            null);
        fireConfigurationChange(event);
    }

    /**
     * Adds the given listener to those being notified.
     * @param theListener the listener who wants to be notified
     */
    public void addConfigurationTreeModelListener(
        ConfigurationTreeModelListener theListener) {

        listeners.add(theListener);
    }

    /**
     * Adds a host node to the model.
     * @param theHost the user object of the host node being created
     * @param theConfig the user object of the new node's configuration
     * ancestor node
     */
    public void addDeployedHost(
        Host theHost,
        Configuration theConfig) {

        DefaultTreeNode configNode = getUserObjectNode(theConfig);
        DefaultTreeNode deployHdrNode =
            (DefaultTreeNode)configDeployHdrMap.get(theConfig.getID());
        DefaultTreeNode hostNode =
            createHostNode(theHost, theConfig, deployHdrNode);
        HashMap map = (HashMap)hostConfigMap.get(theHost.getID());
        if (map == null) {
            // has not been deployed
            map = new HashMap();
            hostConfigMap.put(theHost.getID(), map);
        }
        map.put(theConfig.getID(), hostNode);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            hostNode,
                                            configNode,
                                            new DefaultTreeNode[] {configNode});
        fireConfigurationChange(event);
    }

    /**
     * Adds a process node to the model.
     * @param theProcess the user object of the process node being created
     * @param theHost the user object of the new node's host ancestor node
     * is being added
     * @param theConfig the user object of the new node's configuration
     * ancestor node
     */
    public void addDeployedProcess(
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        DefaultTreeNode configNode = getUserObjectNode(theConfig);
        DefaultTreeNode hostNode = getHostNode(theHost, theConfig);
        if (hostNode != null) {
            DefaultTreeNode processNode = createNode(theProcess, hostNode);
    
            ConfigurationTreeModelEvent event = 
                new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW, 
                    processNode,
                    configNode, 
                    new DefaultTreeNode[] {hostNode, configNode});
            fireConfigurationChange(event);
        }
    }

    /**
     * Adds a deployed PSC node to the model.
     * 
     * @param thePsc the user object of the deployed PSC node being created
     * @param theProcess the user object of the new node's process ancestor node
     * @param theHost the user object of the new node's host ancestor node
     * @param theConfig the user object of the new node's configuration ancestor
     *            node
     */
    public void addDeployedPsc(
        ProductServiceConfig thePsc,
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        // event fired by the createPscNode method
        createPscNode(thePsc, theProcess, theHost, theConfig);
    }

    /**
     * Adds a deployed service node to the model.
     * @param theService the user object of the deployed service node being
     * created
     * @param thePsc the user object of the new node's deployed PSC ancestor node
     * @param theProcess the user object of the new node's process ancestor node
     * @param theConfig the user object of the new node's configuration ancestor
     * node
     */
/*    public void addDeployedService(
        DeployedComponent theService,
        ProductServiceConfig thePsc,
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        DefaultTreeNode pscNode = getPscNode(thePsc, theProcess);
        DefaultTreeNode serviceNode = createNode(theService, pscNode);
        DefaultTreeNode processNode = serviceNode.getParent();
        DefaultTreeNode hostNode = processNode.getParent();
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            serviceNode,
                                            configNode,
                                            new DefaultTreeNode[] {pscNode,
                                                                   processNode,
                                                                   hostNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }
*/
    /**
     * Adds a product node to the model.
     * @param theProduct the user object of the product node being created
     * @param theConfig the user object of the new node's configuration
     * ancestor node
     */
    private void addProduct(
        ProductType theProduct,
        Configuration theConfig) {

        DefaultTreeNode configNode = getUserObjectNode(theConfig);
        DefaultTreeNode pscDefHdrNode =
            (DefaultTreeNode)configPscDefHdrMap.get(theConfig.getID());
        DefaultTreeNode productNode = createNode(theProduct, pscDefHdrNode);

        HashMap map = (HashMap)prodConfigMap.get(theProduct.getID());
        if (map == null) {
            // has no PSCs for any config
            map = new HashMap();
            prodConfigMap.put(theProduct.getID(), map);
        }
        map.put(theConfig.getID(), productNode);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            productNode,
                                            configNode,
                                            new DefaultTreeNode[] {configNode});
        fireConfigurationChange(event);
    }

    /**
     * Adds a PSC definition node to the model.
     * @param thePsc the user object of the PSC definition node being created
     * @param theProduct the user object of the new node's product ancestor node
     * @param theConfig the user object of the new node's configuration
     * ancestor node
     */
    public void addPscDefn(
        ProductServiceConfig thePsc,
        ProductType theProduct,
        Configuration theConfig) {

        // event fired by the createPscNode method
        createPscNode(thePsc, theProduct, theConfig);
    }

    /**
     * Adds a service definition node to the model.
     * @param theService the user object of the service definition node
     * being created
     * @param thePsc the user object of the new node's PSC definition
     * ancestor node
     * @param theProduct the users object of the new node's product ancestor node
     * @param theConfig the user object of the new node's configuration
     * ancestor node
     */
    public void addServiceDefinition(
        ServiceComponentDefn theService,
        ProductServiceConfig thePsc,
        ProductType theProduct,
        Configuration theConfig) {

        DefaultTreeNode pscNode = getPscNode(thePsc, theConfig);
        DefaultTreeNode serviceNode = createNode(theService, pscNode);
        DefaultTreeNode productNode = serviceNode.getParent();
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            serviceNode,
                                            configNode,
                                            new DefaultTreeNode[] {pscNode,
                                                                   productNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }

    /**
     * Invoked when a {@link ConfigurationChangeEvent} occurs.
     * @param theEvent the event to process
     */
    public void configurationChanged(final ConfigurationChangeEvent theEvent) {
		//Update the underlying tree model in the Swing Thread
        Runnable runnable = new Runnable() {
            public void run() {
                Configuration config = theEvent.getConfiguration();
        
                if (theEvent.isNew()) {
                    if (theEvent.isConfigurationChange()) {
                        addConfig(config);
                    }
                    else if (theEvent.isHostChange()) {
                        addDeployedHost(theEvent.getHost(), config);
                    }
                    else if (theEvent.isProcessChange()) {
                        addDeployedProcess(theEvent.getProcess(),
                                           theEvent.getHost(),
                                           config);
                    }
                    else if (theEvent.isPscDefinitionChange()) {
                        addPscDefn(theEvent.getPscDefinition(),
                                   theEvent.getProduct(),
                                   config);
                    }
                    else if (theEvent.isDeployedPscChange()) {
                        addDeployedPsc(theEvent.getDeployedPsc(),
                                       theEvent.getProcess(),
                                       theEvent.getHost(),
                                       config);
                    }
                    else if (theEvent.isProductChange()) {
                        addProduct(theEvent.getProduct(), config);
                    }
                }
                else if (theEvent.isDeleted()) {
                    if (theEvent.isHostChange()) {
                        deleteHost(theEvent.getHost(), config);
                    }
                    else if (theEvent.isProcessChange()) {
                        deleteDeployedProcess(theEvent.getProcess(),
                                              theEvent.getHost(),
                                              config);
                    }
                    else if (theEvent.isPscDefinitionChange()) {
                            deletePscDefintion(theEvent.getPscDefinition(),
                                               theEvent.getProduct(),
                                               config);
                        }
                    else if (theEvent.isDeployedPscChange()) {
                            deleteDeployedPsc(theEvent.getDeployedPsc(),
                                              theEvent.getProcess(),
                                              theEvent.getHost(),
                                              theEvent.getConfiguration());
                    }
                }
                else if (theEvent.isModified()) {
                    if (theEvent.isHostChange()) {
                        modifyHost(theEvent.getHost(), config);
                    }
                    else if (theEvent.isProcessChange()) {
                        modifyDeployedProcess(theEvent.getProcess(),
                                              theEvent.getHost(),
                                              theEvent.getConfiguration());
                    }
                    else if (theEvent.isPscDefinitionChange()) {
                        modifyPscDefinition(theEvent.getPscDefinition(),
                                            theEvent.getProduct(),
                                            config);
                    }
                    else if (theEvent.isServiceDefinitionChange()) {
                        modifyServiceDefintion(theEvent.getServiceDefinition(),
                                               theEvent.getPscDefinition(),
                                               theEvent.getProduct(),
                                               config);
                    }
                }
            }
        };
        
        SwingUtilities.invokeLater(runnable);
    }

    public boolean contains(DefaultTreeNode theNode) {
        Object obj = theNode.getContent();
        return (objNodeMap.get(obj) != null);
    }

    /**
     * Creates a tree node with the given user object and tree node parent.
     * @param theUserObject the user object of the new node
     * @param theParent the parent node of the new node
     */
    private SortableChildrenNode createNode(
        Object theUserObject,
        DefaultTreeNode theParent) {

        SortableChildrenNode child = new SortableChildrenNode(theUserObject);
        theParent.addChild(child);
        objNodeMap.put(theUserObject, child);
        fireNodeAddedEvent(this, child);
        return child;
    }

    private SortableChildrenNode createHostNode(
        Host theHost,
        Configuration theConfig,
        DefaultTreeNode theParent) {

        HostWrapper wrap = new HostWrapper(theHost, theConfig);
        return createNode(wrap, theParent);
    }

    /**
     * Creates a PSC tree node used for PSC definitions.
     * @param thePsc the user object of the new node
     * @param theProduct the user object of the new node's parent node
     * @param theConfig the user object of the new node's configuration node
     */
    private void createPscNode(
        ProductServiceConfig thePsc,
        ProductType theProduct,
        Configuration theConfig) {

        PscWrapper wrapper = new PscWrapper(thePsc, theConfig);

        HashMap map = (HashMap)prodConfigMap.get(theProduct.getID());
        DefaultTreeNode productNode =
            (DefaultTreeNode)map.get(theConfig.getID());
        DefaultTreeNode pscNode = createNode(wrapper, productNode);
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        map = (HashMap)pscConfigMap.get(thePsc.getID());
        if (map == null) {
            // new psc
            map = new HashMap();
            pscConfigMap.put(thePsc.getID(), map);
        }
        map.put(theConfig.getID(), pscNode);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            pscNode,
                                            configNode,
                                            new DefaultTreeNode[] {productNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }

    /**
     * Creates a PSC tree node used for deployed PSCs.
     * @param thePsc the user object of the new node
     * @param theProcess the user object of the new node's parent node
     * @param theHost the user object of the new node's host ancestor node
     * @param theConfig the user object of the new node's configuration ancestor
     * node
     */
    private void createPscNode(
        ProductServiceConfig thePsc,
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        PscWrapper wrapper = new PscWrapper(thePsc, theProcess);
        DefaultTreeNode processNode = getUserObjectNode(theProcess);
        if (processNode == null) {
            return;
        }
        
        DefaultTreeNode pscNode = createNode(wrapper, processNode);
        DefaultTreeNode hostNode = pscNode.getParent();
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        HashMap map = (HashMap)procPscMap.get(theProcess.getID());
        if (map == null) {
            // process doesn't have any PSCs yet
            map = new HashMap();
            procPscMap.put(theProcess.getID(), map);
        }
        map.put(thePsc.getID(), pscNode);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
                                            pscNode,
                                            configNode,
                                            new DefaultTreeNode[] {processNode,
                                                                   hostNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }

    /**
     * Deletes a host node from the model.
     * @param theHost the user object of the host node being deleted
     * @param theConfig the user object of the deleted node's configuration
     * ancestor node
     */
    public void deleteHost(
        Host theHost,
        Configuration theConfig) {

        DefaultTreeNode hostNode = getHostNode(theHost, theConfig);
        if (hostNode != null) {
            DefaultTreeNode configNode = getUserObjectNode(theConfig);
    
            // delete from model
            removeUserObject(hostNode.getContent());
    
            // notify tree model listeners
            ConfigurationTreeModelEvent event =
                new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.DELETED,
                                                hostNode,
                                                configNode,
                                                new DefaultTreeNode[] {configNode});
            fireConfigurationChange(event);
        }
    }

    /**
     * Deletes a process node from the model.
     * @param theProcess the user object of the process node being created
     * @param theHost the user object of the deleted node's host ancestor node
     * @param theConfig the user object of the deleted node's configuration
     * ancestor node
     */
    public void deleteDeployedProcess(
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        DefaultTreeNode processNode = getUserObjectNode(theProcess);
        DefaultTreeNode hostNode = processNode.getParent();
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        // delete from model
        removeUserObject(theProcess);

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.DELETED,
                                            processNode,
                                            configNode,
                                            new DefaultTreeNode[] {hostNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }

    /**
     * Deletes a deployed PSC node from the model.
     * @param thePsc the user object of the deployed PSC node being created
     * @param theProcess the user object of the deleted node's process
     * ancestor node
     * @param theHost the user object of the deleted node's host ancestor node
     * @param theConfig the user object of the deleted node's configuration
     * ancestor node
     */
    public void deleteDeployedPsc(
        ProductServiceConfig thePsc,
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        DefaultTreeNode pscNode = getPscNode(thePsc, theProcess);
        DefaultTreeNode processNode = pscNode.getParent();
        DefaultTreeNode hostNode = processNode.getParent();
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        // delete from model
        removeUserObject(pscNode.getContent());
        HashMap map = (HashMap)procPscMap.get(theProcess.getID());
        map.remove(thePsc.getID());

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.DELETED,
                                            pscNode,
                                            configNode,
                                            new DefaultTreeNode[] {processNode,
                                                                   hostNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }

    /**
     * Deletes a PSC definition node from the model.
     * @param thePsc the user object of the PSC definition node being deleted
     * @param theProduct the user object of the deleted node's product
     * ancestor node
     * @param theConfig the user object of the deleted node's configuration
     * ancestor node
     */
    public void deletePscDefintion(
        ProductServiceConfig thePsc,
        ProductType theProduct,
        Configuration theConfig) {

        DefaultTreeNode pscNode = getPscNode(thePsc, theConfig);
        DefaultTreeNode productNode = pscNode.getParent();
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        // delete from model
        removeUserObject(pscNode.getContent());

        // notify tree model listeners
        ConfigurationTreeModelEvent event =
            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.DELETED,
                                            pscNode,
                                            configNode,
                                            new DefaultTreeNode[] {productNode,
                                                                   configNode});
        fireConfigurationChange(event);
    }

    /**
     * Notifies all registered {@link ConfigurationChangeListener}s that the
     * tree model has changed.
     * @param theEvent the event being sent to the listeners
     */
    private void fireConfigurationChange(ConfigurationTreeModelEvent theEvent) {
        LogManager.logDetail(
            LogContexts.PSCDEPLOY,
            "ConfigurationTreeModelEvent=" + theEvent.paramString()); //$NON-NLS-1$
        for (int size=listeners.size(), i=0; i<size; i++) {
            ConfigurationTreeModelListener l =
                (ConfigurationTreeModelListener)listeners.get(i);
            l.treeNodesChanged(theEvent);
        }
    }

    /**
     * Gets the deployments header node for the given configuration.
     * @param theConfig the configuration whose header node is being requested
     * @return the deployments header node
     */
    public DefaultTreeNode getDeploymentsHeaderNode(Configuration theConfig) {
        return (DefaultTreeNode)configDeployHdrMap.get(theConfig.getID());
    }

    /**
     * Gets the host node of the given configuration. 
     * May return null if <code>theHost</code> is null.
     * @param theHost the host whose node is being requested
     * @param theConfig the configuration of the host node
     */
    private DefaultTreeNode getHostNode(
        Host theHost,
        Configuration theConfig) {

        getUserObjectNode(theConfig);
        HashMap map = (HashMap)hostConfigMap.get(theHost.getID());
        DefaultTreeNode result = null;
        if (map != null) {
            result = (DefaultTreeNode)map.get(theConfig.getID());
        }
        return result;
    }

    /**
     * Gets the PSC definitions header node for the given configuration.
     * @param theConfig the configuration whose header node is being requested
     * @return the PSC definitions header node
     */
    public DefaultTreeNode getPscDefinitionsHeaderNode(Configuration theConfig) {
        return (DefaultTreeNode)configPscDefHdrMap.get(theConfig.getID());
    }

    /**
     * Gets the requested PSC definition node.
     * @param thePsc the user object of the PSC definition node
     * @param theConfig the user object of the PSC definition node's configuration
     * ancestor node
     */
    public DefaultTreeNode getPscNode(
        ProductServiceConfig thePsc,
        Configuration theConfig) {

        HashMap map = (HashMap)pscConfigMap.get(thePsc.getID());
        return (DefaultTreeNode)map.get(theConfig.getID());
    }

    /**
     * Gets the requested deployed PSC node.
     * @param thePsc the user object of the deployed PSC node
     * @param theProcess the user object of the deployed PSC node's process
     * ancestor node
     */
    public DefaultTreeNode getPscNode(
        ProductServiceConfig thePsc,
        VMComponentDefn theProcess) {

        HashMap map = (HashMap)procPscMap.get(theProcess.getID());
        return (DefaultTreeNode)map.get(thePsc.getID());
    }

    /**
     * Gets the requested user object's node.
     * @param theUserObject the user object whose node is being requested
     * @return the requested user object's node
     */
    public DefaultTreeNode getUserObjectNode(Object theUserObject) {
        return (DefaultTreeNode)objNodeMap.get(theUserObject);
    }

    /**
     * Indicates if the given node is a deployments header node or a
     * PSC definitions header node.
     * @param theNode the node being checked
     * @return <code>true</code> if the node is a header node;
     * <code>false</code> otherwise.
     */
    public boolean isHeaderNode(DefaultTreeNode theNode) {
        return (isDeploymentsHeaderNode(theNode) ||
                isPscDefinitionsHeaderNode(theNode));
    }

    /**
     * Indicates if the given node is a deployments header node.
     * @param theNode the node being checked
     * @return <code>true</code> if the node is a deployments header node;
     * <code>false</code> otherwise.
     */
    public boolean isDeploymentsHeaderNode(DefaultTreeNode theNode) {
        return configDeployHdrMap.containsValue(theNode);
    }

    /**
     * Indicates if the given node is a PSC definitions header node.
     * @param theNode the node being checked
     * @return <code>true</code> if the node is a PSC definitions header node;
     * <code>false</code> otherwise.
     */
    public boolean isPscDefinitionsHeaderNode(DefaultTreeNode theNode) {
        return configPscDefHdrMap.containsValue(theNode);
    }

    /**
     * Modifies a deployed host node in the model.
     * @param theProcess the user object of the node being modified
     * @param theHost the user object of the modified node's host ancestor node
     * @param theConfig the user object of the modified node's configuration
     * ancestor node
     */
    public void modifyDeployedProcess(
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {

        // not sure if anything needs to be done here
    }

    /**
     * Modifies a host node in the model.
     * @param theHost the user object of the node being modified
     * @param theConfig the user object of the modified node's configuration
     * ancestor node
     */
    public void modifyHost(
        Host theHost,
        Configuration theConfig) {

        // hosts are currently not a configuration object
        // but appear to the user in each configuration
        HashMap map = (HashMap)hostConfigMap.get(theHost.getID());
        if (map != null) {
            // modify all configs
            Iterator itr = map.values().iterator();
            while (itr.hasNext()) {
                DefaultTreeNode hostNode = (DefaultTreeNode)itr.next();
                if (theHost instanceof HostWrapper) {
                    hostNode.setContent(theHost);
                } else {
                    HostWrapper wrap = new HostWrapper(theHost, theConfig);
                    hostNode.setContent(wrap);
                }
                
            }
        }
    }

    /**
     * Modifies a PSC definitions node in the model.
     * @param thePsc the user object of the node being modified
     * @param theProduct the user object of the modified node's product
     * ancestor node
     * @param theConfig the user object of the modified node's configuration
     * ancestor node
     */
    public void modifyPscDefinition(
        ProductServiceConfig thePsc,
        ProductType theProduct,
        Configuration theConfig) {

        // not sure if anything needs to be done here
    }

    /**
     * Modifies a service definitions node in the model.
     * @param theService the user object of the node being modified
     * @param thePsc the user object of the modified node's PSC definition
     * ancestor node
     * @param theProduct the user object of the modified node's product
     * ancestor node
     * @param theConfig the user object of the modified node's configuration
     * ancestor node
     */
    public void modifyServiceDefintion(
        ServiceComponentDefn theService,
        ProductServiceConfig thePsc,
        ProductType theProduct,
        Configuration theConfig) {

        // not sure if anything needs to be done here
    }

    /**
     * Clears all nodes and all caches.
     */
    public void refresh() {
        root.removeAllChildren();
        objNodeMap.clear();
        configDeployHdrMap.clear();
        configPscDefHdrMap.clear();
        hostConfigMap.clear();
        prodConfigMap.clear();
        procPscMap.clear();
        pscConfigMap.clear();
        fireModelChangedEvent(this, root);
    }

    /**
     * Removes the given listener from those being notified.
     * @param theListener the listener being removed
     */
    public void removeConfigurationTreeModelListener(
        ConfigurationTreeModelListener theListener) {

        listeners.remove(theListener);
    }

    /**
     * Removes the user object's node and all it's children nodes. All
     * corresponding objects being cached are also removed from the cache.
     * @param theUserObject the user object being removed
     */
    public void removeUserObject(Object theUserObject) {
        DefaultTreeNode node = getUserObjectNode(theUserObject);
        List kids = node.getChildren();
        if ((kids != null) && (!kids.isEmpty())) {
            for (int size=kids.size(), i=size-1; i>=0; i--) {
                DefaultTreeNode childNode = (DefaultTreeNode)kids.get(i);
                removeUserObject(childNode.getContent());
            }
        }
        objNodeMap.remove(theUserObject);
        removeNode(node);
    }

    ///////////////////////////////////////////////////////////////////////////
    // PscWrapper INNER CLASS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * The <code>PscWrapper</code> class wraps a {@link ProductServiceConfig} so
     * that PSC definitions and deployed PSCs can be distinguished.
     */
    public class PscWrapper {

        ///////////////////////////////////////////////////////////////////////
        // FIELDS
        ///////////////////////////////////////////////////////////////////////

        private ProductServiceConfig psc;
        private VMComponentDefn proc;
        private Configuration config;

        ///////////////////////////////////////////////////////////////////////
        // CONSTRUCTORS
        ///////////////////////////////////////////////////////////////////////

        /**
         * Constructs a <code>PscWrapper</code> that wraps a deployed PSC.
         * @param thePsc the user object of the deployed PSC
         * @param theProcess the user object of the deployed PSC's process
         */
        public PscWrapper(
            ProductServiceConfig thePsc,
            VMComponentDefn theProcess) {

            psc = thePsc;
            proc = theProcess;
        }

        /**
         * Constructs a <code>PscWrapper</code> that wraps a PSC definition.
         * @param thePsc the user object of the deployed PSC
         * @param theConfig the user object of the deployed PSC's configuration
         */
        public PscWrapper(
            ProductServiceConfig thePsc,
            Configuration theConfig) {

            psc = thePsc;
            config = theConfig;
        }

        ///////////////////////////////////////////////////////////////////////
        // METHODS
        ///////////////////////////////////////////////////////////////////////

        /**
         * Indicates if the given input parameter is equal to this object.
         * @param theObject the object being compared
         * @return <code>true</code> if equal; <code>false</code> otherwise.
         */
        public boolean equals(Object theObject) {
            if (theObject instanceof PscWrapper) {
                PscWrapper otherObj = (PscWrapper)theObject;
                if (psc.equals(otherObj.getPsc())) {
                    if (isDefinition() == otherObj.isDefinition()) {
                        if (isDefinition()) {
                            return (config.equals(otherObj.getConfig()));
                        }
                        return (proc.equals(otherObj.getProcess()));
                    }
                }
                return false;

            }
            return false;
        }

        /**
         * Gets the associated configuration. Only used if wrapping a PSC
         * definition.
         * @return the associated configuration of the PSC definition or
         * <code>null</code> if wrapping a deployed PSC.
         */
        public Configuration getConfig() {
            return config;
        }

        /**
         * Gets the associated process. Only used if wrapping a deployed PSC.
         * @return the associated process of the deployed PSC or
         * <code>null</code> if wrapping a PSC definition.
         */
        public VMComponentDefn getProcess() {
            return proc;
        }

        /**
         * Gets the wrapped PSC object.
         * @return the wrapped PSC object
         */
        public ProductServiceConfig getPsc() {
            return psc;
        }

        /**
         * Indicates if this object wraps a PSC definition.
         * @return <code>true</code> if this object wraps a PSC definition;
         * <code>false</code> otherwise.
         */
        public boolean isDefinition() {
            return (config != null);
        }

        /**
         * Indicates if this object wraps a deployed PSC.
         * @return <code>true</code> if this object wraps a deployed PSC;
         * <code>false</code> otherwise.
         */
        public boolean isDeployed() {
            return (proc != null);
        }

        /**
         * Gets a hash code value.
         * @return a hash code value
         */
        public int hashCode() {
            int result = psc.hashCode();
            if (isDefinition()) {
                return result + config.hashCode();
            }
            return result + proc.hashCode();
        }

        /**
         * Gets this object's string representation which is it's associated
         * {@link ProductServiceConfig}'s <code>toString</code>.
         * @return the string representation
         */
        public String toString() {
            return psc.toString();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // HostWrapper INNER CLASS
    ///////////////////////////////////////////////////////////////////////////

    public class HostWrapper {
        private Host host;
        private Configuration config;
        public HostWrapper(
            Host theHost,
            Configuration theConfig) {
            host = theHost;
            config = theConfig;
        }
        public boolean equals(Object theObject) {
            if (theObject instanceof HostWrapper) {
                HostWrapper otherObj = (HostWrapper)theObject;
                if (host.equals(otherObj.getHost())) {
                    return (config.equals(otherObj.getConfig()));
                }
            }
            return false;
        }

        public Host getHost() {
            return host;
        }
        public Configuration getConfig() {
            return config;
        }
        public int hashCode() {
            return (host.hashCode() + config.hashCode());
        }
        public String toString() {
            return host.toString();
        }
    }
}
