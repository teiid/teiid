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

package com.metamatrix.console.models;
/*
 * (c) Copyright 2000-2002 MetaMatrix, Inc.
 * All Rights Reserved.
 */



import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.util.LogContextsUtil;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.logsetup.SystemLogSetUpPanel;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
/**
 * Extension of Manager to manage the Log tab.
 */
public class ServerLogManager extends TimedManager {
    private String[] messageLevelDisplayNames = null;
    private Set /*<String>*/ allContexts = null;
   


    /**
     * Construct a ServerLogManager
     */
    public ServerLogManager(ConnectionInfo connection) {
        super(connection);
        super.init();
        
    }

    public void init() {
        super.init();
    }

    /**
     * Get all available contexts, in sorted order. 
     * @return
     * @since 4.3
     */
    public Set /*<String>*/ getAllContexts() {
        if (allContexts == null) {
            allContexts = new TreeSet(LogContextsUtil.ALL_CONTEXTS);        	
        }
        return allContexts;
    }

    private Configuration getConfigurationForIndex(int index)
            throws AuthorizationException, ExternalException{
        Configuration config = null;
        try {
            switch (index) {
                case SystemLogSetUpPanel.STARTUP_INDEX:
                    config = getConfigAPI().getStartupConfiguration();
                    break;
                case SystemLogSetUpPanel.NEXT_STARTUP_INDEX:
                    config = getConfigAPI().getNextStartupConfiguration();
                    break;
            }
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException("Error retrieving configuration.", ex); //$NON-NLS-1$
        }
        return config;
    }

    private Collection /*<String>*/ convertDiscardedToReported(
            Collection /*<String>*/ discardedContexts) {
        int totalContextsCount = allContexts.size();
        ArrayList allContextsCopy = new ArrayList(totalContextsCount);
        Iterator it = allContexts.iterator();
        while (it.hasNext()) {
            allContextsCopy.add(it.next());
        }
        for (int i = totalContextsCount - 1; i >= 0; i--) {
            String curContext = (String)allContextsCopy.get(i);
            if (discardedContexts.contains(curContext)) {
                allContextsCopy.remove(i);
            }
        }
        return allContextsCopy;
    }

    public Collection /*<String>*/ getContextsForConfigurationIndex(int index)
            throws AuthorizationException, ExternalException {
        Configuration config = getConfigurationForIndex(index);
        LogConfiguration logConfig = config.getLogConfiguration();
        Collection /*<String>*/ discardedContexts =
                logConfig.getDiscardedContexts();
        Collection /*<String>*/ reportedContexts =
                convertDiscardedToReported(discardedContexts);
        return reportedContexts;
    }

    public void setContextsForConfigurationIndex(int index,
            Collection /*<String>*/ contexts)
            throws AuthorizationException, ExternalException {
        Configuration config = getConfigurationForIndex(index);
        LogConfiguration logConfig = config.getLogConfiguration();
        //We will discard all contexts, then add back the ones we want
        logConfig.discardContexts(allContexts);
        logConfig.recordContexts(contexts);
        ConfigurationObjectEditor coe = null;
        try {
            coe = getConfigAPI().createEditor();
            coe.setLogConfiguration(config, logConfig);
            ModificationActionQueue maq = coe.getDestination();
            java.util.List actions = maq.popActions();
            getRuntimeAPI().setLoggingConfiguration(config, logConfig, actions);
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    public int getLoggingLevelForConfigurationIndex(int index)
            throws AuthorizationException, ExternalException {
        Configuration config = getConfigurationForIndex(index);
        LogConfiguration logConfig = config.getLogConfiguration();
        int level = logConfig.getMessageLevel();
        return level;
    }

    public void setLoggingLevelForConfigurationIndex(int index, int level)
            throws AuthorizationException, ExternalException {
        Configuration config = getConfigurationForIndex(index);
        LogConfiguration logConfig = config.getLogConfiguration();
        logConfig.setMessageLevel(level);
        ConfigurationObjectEditor coe = null;
        try {
            coe = getConfigAPI().createEditor();
            coe.setLogConfiguration(config, logConfig);
            ModificationActionQueue maq = coe.getDestination();
            java.util.List actions = maq.popActions();
            getRuntimeAPI().setLoggingConfiguration(config, logConfig, actions);
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    

    public String[] getMessageLevelDisplayNames() {
        if (messageLevelDisplayNames == null) {
            Collection displayNames = MessageLevel.getDisplayNames();
            messageLevelDisplayNames = new String[displayNames.size()];
            Iterator it = displayNames.iterator();
            for (int i = 0; it.hasNext(); i++) {
                messageLevelDisplayNames[i] = (String)it.next();
            }
        }
        return messageLevelDisplayNames;
    }


    /**
     * Get the log entries that match the specified criteria. 
     * @param startTime
     * @param endTime  If null, will ignore this criterion.
     * @param levels List of Integers
     * @param levels List of Strings.  If null, will ignore this criterion and return entries with any context.
     * @param maxRows
     * @return List of LogEntry objects
     * @since 4.3
     */
    public List getLogEntries(Date startTime,
                                Date endTime,
                                List levels,
                                List contexts,
                                int maxRows) throws AuthorizationException, ExternalException{
        try {
            return getRuntimeAPI().getLogEntries(startTime, endTime, levels, contexts, maxRows);
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
    }
    
   
    
    
    private ConfigurationAdminAPI getConfigAPI() {
        return ModelManager.getConfigurationAPI(getConnection());
    }
    
    private RuntimeStateAdminAPI getRuntimeAPI() {
        return ModelManager.getRuntimeStateAPI(getConnection());
    }

}
