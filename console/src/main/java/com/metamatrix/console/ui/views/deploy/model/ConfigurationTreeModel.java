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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.swing.SwingUtilities;

import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
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


    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    //key=object, value=DefaultTreeNode
    private HashMap objNodeMap = new HashMap();

    //key=hostID, value=HashMap (key=ConfigurationID, value=node-HostWrapper as content)
    private HashMap hostConfigMap = new HashMap();

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
        System.out.println("root");
    }
    
   public void init(Configuration theConfig) {
	   addConfig(theConfig, false);
     	Collection objs = theConfig.getHosts();
    	for (Iterator<Host> it=objs.iterator(); it.hasNext();) {
    		Host h = (Host)it.next();
    		addDeloyedHost(h, theConfig, false);
        	Collection vms = theConfig.getVMsForHost((HostID)h.getID()) ;
        	for (Iterator<VMComponentDefn> vmit=vms.iterator(); vmit.hasNext();) {
        		VMComponentDefn vm = vmit.next();
        		this.addDeployedProcess(vm, h, theConfig, false);
        		
//            	Collection svcs = theConfig.getDeployedServicesForVM(vm);
//            	for (Iterator<DeployedComponent> svcit=svcs.iterator(); svcit.hasNext();) {
//            		DeployedComponent dep = svcit.next();
//            		this.addDeployedService(dep, vm, h, theConfig, false);
//             		
//            	}
        		
        	}  
    		
    	}
  	
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Adds a configuration node to the model.
     * @param theConfig the user object of the configuration node being created
     */
    private void addConfig(Configuration theConfig, boolean fireevent) {
        // create configuration node
        SortableChildrenNode configNode = createNode(theConfig, root);

        // notify tree model listeners
        if (fireevent) {
	        ConfigurationTreeModelEvent event =
	            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
	                                            configNode,
	                                            configNode,
	                                            null);
	        fireConfigurationChange(event);
        }
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
    	
    	addDeloyedHost(theHost, theConfig, true);
    	
    }
    
    private void addDeloyedHost(Host theHost, Configuration theConfig, boolean fireevent) {

        DefaultTreeNode configNode = getUserObjectNode(theConfig);
        DefaultTreeNode hostNode =
            createHostNode(theHost, theConfig, configNode);
        HashMap map = (HashMap)hostConfigMap.get(theHost.getID());
        if (map == null) {
            // has not been deployed
            map = new HashMap();
            hostConfigMap.put(theHost.getID(), map);
        }
        map.put(theConfig.getID(), hostNode);

        // notify tree model listeners
        if (fireevent) {
	        ConfigurationTreeModelEvent event =
	            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
	                                            hostNode,
	                                            configNode,
	                                            new DefaultTreeNode[] {configNode});
	        fireConfigurationChange(event);
        }
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
    	addDeployedProcess(theProcess, theHost, theConfig, true);
    }
    
    private void addDeployedProcess(VMComponentDefn theProcess,
    		Host theHost,
    		Configuration theConfig,
    		boolean fireevent) {

        DefaultTreeNode configNode = getUserObjectNode(theConfig);
        DefaultTreeNode hostNode = getHostNode(theHost, theConfig);
        if (hostNode != null) {
            DefaultTreeNode processNode = createNode(theProcess, hostNode);
    
            if (fireevent) {
	            ConfigurationTreeModelEvent event = 
	                new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW, 
	                    processNode,
	                    configNode, 
	                    new DefaultTreeNode[] {hostNode, configNode});
	            fireConfigurationChange(event);
            }
        }
    }

    /**
     * Adds a service definition node to the model.
     * @param theService the user object of the deployed service definition node
     * being created
     * @param theProcess the user object of the new node's VMComponentDefn
     * ancestor node
     * @param theHost the users object of the new node's Host ancestor node
     * @param theConfig the user object of the new node's configuration
     * ancestor node
     */
    public void addDeployedService(
        DeployedComponent theService,
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {
    	addDeployedService(theService, theProcess, theHost, theConfig, true);
    }
    
    private void addDeployedService(
            DeployedComponent theService,
            VMComponentDefn theProcess,
            Host theHost,
            Configuration theConfig,
            boolean fireevent) {

         
        DefaultTreeNode processNode = getUserObjectNode(theProcess);

        DefaultTreeNode serviceNode = createNode(theService, processNode);
        DefaultTreeNode hostNode = getHostNode(theHost, theConfig);
        DefaultTreeNode configNode = getUserObjectNode(theConfig);

        // notify tree model listeners
        if (fireevent) {
	        ConfigurationTreeModelEvent event =
	            new ConfigurationTreeModelEvent(ConfigurationTreeModelEvent.NEW,
	                                            serviceNode,
	                                            configNode,
	                                            new DefaultTreeNode[] {processNode,
	                                                                   hostNode,
	                                                                   configNode});
	        fireConfigurationChange(event);
        }
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
                        addConfig(config, true);
                    }
                    else if (theEvent.isHostChange()) {
                        addDeployedHost(theEvent.getHost(), config);
                    }
                    else if (theEvent.isProcessChange()) {
                        addDeployedProcess(theEvent.getProcess(),
                                           theEvent.getHost(),
                                           config);
                    }
//                    else if (theEvent.isDeployedServiceChange()) {
//                        addDeployedService(theEvent.getDeployedService(),
//                                       theEvent.getProcess(),
//                                       theEvent.getHost(),
//                                       config);
//                    }
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
//                    else if (theEvent.isDeployedServiceChange()) {
//                            deleteDeployedService(theEvent.getDeployedService(),
//                                              theEvent.getProcess(),
//                                              theEvent.getHost(),
//                                              theEvent.getConfiguration());
//                    }
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
                    else if (theEvent.isDeployedServiceChange()) {
                        modifyDeployedService(theEvent.getDeployedService(),
                        		   theEvent.getProcess(),
                                   theEvent.getHost(),
                                   theEvent.getConfiguration());
                    }
                    else if (theEvent.isServiceDefinitionChange()) {
                        modifyServiceDefintion(theEvent.getServiceDefinition(),
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
    	return (theNode == root);
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
    public void modifyDeployedService(
        DeployedComponent theService,
        VMComponentDefn theProcess,
        Host theHost,
        Configuration theConfig) {


        // not sure if anything needs to be done here
    }    

    /**
     * Clears all nodes and all caches.
     */
    public void refresh() {
        root.removeAllChildren();
        objNodeMap.clear();
        hostConfigMap.clear();
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
