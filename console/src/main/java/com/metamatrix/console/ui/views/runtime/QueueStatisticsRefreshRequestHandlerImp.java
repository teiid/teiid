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

package com.metamatrix.console.ui.views.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import javax.swing.JOptionPane;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.views.runtime.util.ServiceStateConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.platform.vm.controller.VMStatistics;

public class QueueStatisticsRefreshRequestHandlerImp 
		implements QueueStatisticsRefreshRequestHandler, ServiceStateConstants{
		
	private QueueStatisticsDisplayHandler qsdh;
    private VMStatisticsDisplayHandler vmsdh;
    private boolean refreshFlag = false;
    private ServiceData currentServiceData = null;
    private ConnectionInfo connection;
    
    public QueueStatisticsRefreshRequestHandlerImp(ConnectionInfo conn) {
    	super();
    	this.connection = conn;
    }
    
    public void refreshRequested(ServiceData sd) {
        refreshFlag = true;
        try {
            if (currentServiceData != null) {
                if (currentServiceData.getCurrentState() != sd.getCurrentState()
                        && currentServiceData.getServiceID().equals(
                        sd.getServiceID())) {
                    sd = currentServiceData;
                }
            }
            QueueStatistics[] qss = getQueueStatistics(sd);
            if (qss.length != 0) {
                qsdh.refreshDisplayForService(sd.getName(),sd,qss);
            }
            refreshFlag = false;
            
        } catch (Exception theException) {
            logException(theException, "QueueStatisticsRefreshRequestHandlerImp.refreshRequested"); //$NON-NLS-1$
		}
	}

	public void refreshProcessRequested(ProcessData pd) {
        try {
			if (getProcessStatistics(pd) != null) {
                VMStatistics vmstats = getProcessStatistics(pd);
                vmsdh.refreshDisplayForProcess(pd.getName(), pd, vmstats);
            }
        } catch (Exception theException) {
            logException(theException, "QueueStatisticsRefreshRequestHandlerImp.refreshRequested"); //$NON-NLS-1$
		}
	}

    public void runGarbageCollection(ProcessData pd) {
        try {
            //api = getAdminAPI();
            VMControllerID cntrlrID = pd.getProcessID();
            getAdminAPI().runGC(cntrlrID);
        } catch (Exception theException) {
            displayException(theException, "QueueStatisticsRefreshRequestHandlerImp.runGarbageCollection"); //$NON-NLS-1$
        }
    }

    private RuntimeStateAdminAPI getAdminAPI() {
        RuntimeStateAdminAPI api = null;
        try {
            api = ModelManager.getRuntimeStateAPI(connection);
        } catch (Exception theException) {
            logException(theException, "QueueStatisticsRefreshRequestHandlerImp.getAdminAPI"); //$NON-NLS-1$
        }
        return api;
    }

    public boolean isRefresh() {
        return refreshFlag;
    }

    public void setServiceData(ServiceData s) {
        this.currentServiceData = s;
    }


    public QueueStatistics[] getQueueStatistics(ServiceData sd) {
        Collection serviceCollection = null;
        serviceCollection = getServiceCollection(sd);
        QueueStatistics[] qss = new QueueStatistics[serviceCollection.size()];
        if (serviceCollection.isEmpty() ) {
            return qss;
        }
        if (serviceCollection != null) {
        	Iterator iter = serviceCollection.iterator();
            int i = 0;
            while ( i < serviceCollection.size()) {
                if (iter.hasNext()) {
                    WorkerPoolStats wps = (WorkerPoolStats)iter.next();
                    qss[i] = new QueueStatistics(wps.name, wps.queued,
                            0, (int)wps.totalSubmitted,
                            (int)wps.totalCompleted, wps.threads);
                    i++;
                }
            }
        }
        return qss;
    }

    public HashMap getServiceMap(ArrayList serviceList) {
        HashMap serviceHM = new HashMap();
        Collection sqs = null;
        Iterator iter = serviceList.iterator();
        while (iter.hasNext()) {
            ServiceData sd = (ServiceData)iter.next();
            int state = sd.getCurrentState();
            if (state == 1 || state == 3) {
                sqs = getServiceCollection(sd);
            }
            if (sqs != null) {
                serviceHM.put(sd, new Integer(sqs.size()));
            } else {
                serviceHM.put(sd, new Integer(0));
            }
        }
		return serviceHM;
    }

    private Collection getServiceCollection(ServiceData sd) {
        Collection sqs = null;
        if (refreshFlag) {
            if (sd.getCurrentState() != OPEN) {
                if (sd.getCurrentState() == 3) {
                    JOptionPane.showMessageDialog(ConsoleMainFrame.getInstance(),
                    		"Service " + sd.getName() + "currently suspended. " //$NON-NLS-1$ //$NON-NLS-2$
                    		+ "Queue statistics is unavailable", "Information", //$NON-NLS-1$ //$NON-NLS-2$
                    		JOptionPane.INFORMATION_MESSAGE);
                } else {
                    JOptionPane.showMessageDialog(ConsoleMainFrame.getInstance(),
                    		"Service " + sd.getName() + "currently stopped. " +  //$NON-NLS-1$ //$NON-NLS-2$
                    		"Queue statistics is unavailable", "Information", //$NON-NLS-1$ //$NON-NLS-2$
                    		JOptionPane.INFORMATION_MESSAGE);
                }
                QueueStatisticsFrame qsd = 
                		(QueueStatisticsFrame)qsdh.getDialogs().get(sd);
                qsd.dispose();
                return Collections.EMPTY_SET;
            }
        }
        try {
             sqs = getAdminAPI().getServiceQueueStatistics(sd.getServiceID());
        } catch (Exception theException) {
            displayException(theException, "QueueStatisticsRefreshRequestHandlerImp.getServiceCollection"); //$NON-NLS-1$
        }
        return sqs;
    }

	public VMStatistics getProcessStatistics(ProcessData pd) {
        try {
            return getAdminAPI().getVMStatistics(pd.getProcessID());
        } catch (Exception theException) {
            displayException(theException, "QueueStatisticsRefreshRequestHandlerImp.getVMStatistics"); //$NON-NLS-1$
            return null;
        }
    }

    public void setDisplayHandler(QueueStatisticsDisplayHandler qsdh) {
        this.qsdh = qsdh;
    }

    public void setDisplayVMHandler(VMStatisticsDisplayHandler vmsdh) {
        this.vmsdh = vmsdh;
    }
    
    
    
    /**
     * Log an internationalized exception message. 
     * @param theException
     * @param messageCode  Key of the message in i18n.properties
     * @since 4.3
     */
    private void logException(Exception theException, String messageCode) {
        String message = ConsolePlugin.Util.getString(messageCode);
        LogManager.logError(LogContexts.RUNTIME, theException, message);
    }
    
    /**
     * Display and log an internationalized exception message. 
     * @param theException
     * @param messageCode  Key of the message in i18n.properties
     * @since 4.3
     */
    private void displayException(Exception theException, String messageCode) {
        String message = ConsolePlugin.Util.getString(messageCode);
        ExceptionUtility.showMessage("", message, theException); //$NON-NLS-1$
        LogManager.logError(LogContexts.RUNTIME, theException, message);

    }
} 