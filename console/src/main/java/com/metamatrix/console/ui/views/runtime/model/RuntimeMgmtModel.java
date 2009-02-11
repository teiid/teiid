/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.console.ui.views.runtime.model;

import java.awt.Cursor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.ui.layout.PanelsTree;
import com.metamatrix.console.ui.tree.SortableChildrenNode;
import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeModel;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;


public final class RuntimeMgmtModel
    extends DefaultTreeModel
    implements StatisticsConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

	private static final long MIN_REFRESH_DELAY = 5000;
	
    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private DefaultTreeNode root;

    //key=object, value=DefaultTreeNode
    private HashMap objNodeMap = new HashMap();
    //key=HostData, value=HostStatistics
    private HashMap hostStatisticsMap = new HashMap();
    private ArrayList serviceList = new ArrayList();

    // the date of the last state change (based on the service change date)
    private Date lastChangeDate = null;
    
    private ConnectionInfo connectionInfo = null;
    
    private long lastRefresh;
    private Timer timer = new Timer(true);

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public RuntimeMgmtModel(ConnectionInfo connectionInfo)
        throws ExternalException {

        super(new DefaultTreeNode());
        this.connectionInfo = connectionInfo;
        root = (DefaultTreeNode)getRoot();
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    private void addHost(HostData theHost) {
        DefaultTreeNode hostNode = createNode(theHost, root);
        HostStatistics stats = new HostStatistics(theHost);
        hostStatisticsMap.put(theHost, stats);
        Collection procs = theHost.getProcesses();
        if (procs != null) {
            Iterator procItr = procs.iterator();
            while (procItr.hasNext()) {
                addProcess((ProcessData)procItr.next(), hostNode, stats);
            }
        }
    }

    private void addProcess(
        ProcessData theProcess,
        DefaultTreeNode theHostNode,
        HostStatistics theStats) {

        theStats.addProcess(theProcess);
        DefaultTreeNode procNode = createNode(theProcess, theHostNode);
        Collection pscs = theProcess.getPSCs();
        if (pscs != null) {
            Iterator pscItr = pscs.iterator();
            while (pscItr.hasNext()) {
                addPsc((PSCData)pscItr.next(), procNode, theStats);
            }
        }
    }

    private void addPsc(
        PSCData thePsc,
        DefaultTreeNode theProcessNode,
        HostStatistics theStats) {

        DefaultTreeNode pscNode = createNode(thePsc, theProcessNode);
        Collection services = thePsc.getServices();
        if (services != null) {
            Iterator serviceItr = services.iterator();
            while (serviceItr.hasNext()) {
                addService((ServiceData)serviceItr.next(), pscNode, theStats);
            }
        }
    }

    private void addService(
        ServiceData theService,
        DefaultTreeNode thePscNode,
        HostStatistics theStats) {
        theStats.addService(theService);
        serviceList.add(theService);
        Date changeDate = theService.getStateChangeTime();
        if (lastChangeDate == null) {
            lastChangeDate = changeDate;
        }
        else {
            if (changeDate.after(lastChangeDate)) {
                lastChangeDate = changeDate;
            }
        }
        createNode(theService, thePscNode);
    }

    public ArrayList getServiceList(){
        return serviceList;
    }

    private DefaultTreeNode createNode(
        Object theUserObject,
        DefaultTreeNode theParent) {

        DefaultTreeNode child = new SortableChildrenNode(theUserObject);
        theParent.addChild(child);
        objNodeMap.put(theUserObject, child);
        fireNodeAddedEvent(this, child);
        return child;
    }

    public Date getLastChangeDate() {
        return lastChangeDate;
    }

    public DefaultTreeNode getUserObjectNode(Object theUserObject) {
        return (DefaultTreeNode)objNodeMap.get(theUserObject);
    }

    public Map getStatistics() {
        return hostStatisticsMap;
    }

    public void refresh() {
    	this.timer.schedule(new RefreshRunner(), 0);
    }
   
    private void refreshImpl() {

        try {
            RuntimeMgmtManager manager = ModelManager.getRuntimeMgmtManager(connectionInfo);
            if( manager == null) {
                return;
            }
            SystemState state = manager.getServerState();
            Collection hosts = state.getHosts();
            if ((hosts == null) || hosts.isEmpty()) {
                LogManager.logCritical(
                    LogContexts.RUNTIME,
                    "RuntimeMgmtModel.updateSystemState:No hosts found."); //$NON-NLS-1$
            }
            else {
                objNodeMap.clear();
                hostStatisticsMap.clear();
                serviceList.clear();
                root.removeAllChildren();
                Iterator hostItr = hosts.iterator();
                while (hostItr.hasNext()) {
                    HostData host = (HostData)hostItr.next();
                    addHost(host);
                }
            }
            fireModelChangedEvent(this, root);
        }
        catch (Exception theException) {
            LogManager.logError(LogContexts.RUNTIME,
                                theException,
                                "RuntimeMgmtModel.refreshImpl"); //$NON-NLS-1$
            ExceptionUtility.showMessage(
                RuntimeMgmtUtils.getString("refreshproblem.msg"), //$NON-NLS-1$
                theException.getMessage(), //$NON-NLS-1$
                theException);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class RefreshRunner extends TimerTask {
        public void run() {
        	if (System.currentTimeMillis() - lastRefresh > MIN_REFRESH_DELAY){
	        	lastRefresh = System.currentTimeMillis();
				PanelsTree tree = PanelsTree.getInstance(connectionInfo);
				tree.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
	            refreshImpl();
				tree.setCursor(Cursor.getDefaultCursor());
        	}
        }
    }
}
