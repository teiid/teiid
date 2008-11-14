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

package com.metamatrix.connector.exec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

/**
 * Implementation of text connector.
 */
public class ExecConnector implements Connector {
    
    private ConnectorLogger logger;
    private ConnectorEnvironment env;
    private boolean start = false;
    private List exclusionList= Collections.EMPTY_LIST;
    private String exclusionFile;

    

    /**
     * Initialization with environment.
     */
    public void initialize( ConnectorEnvironment environment ) throws ConnectorException {
        logger = environment.getLogger();
        this.env = environment;
        
        exclusionFile = environment.getProperties().getProperty("exclusionFile"); //$NON-NLS-1$
        if(exclusionFile != null && exclusionFile.trim().length() > 0)
        {
            loadExclusionFile(exclusionFile);
        }        

        // logging
        logger = environment.getLogger();
        logger.logInfo("Exec Connector is intialized."); //$NON-NLS-1$
    }

    public void stop() {
        if(!start){
            return;
        }

        start = false;
        logger.logInfo("Exec Connector is stoped."); //$NON-NLS-1$
    }

    public void start() {
        start = true;
        logger.logInfo("Exec Connector is started."); //$NON-NLS-1$
    }

    /*
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(SecurityContext context) throws ConnectorException {
        return new ExecConnection(this.env, exclusionList);
    }
    
    protected void loadExclusionFile(String file)
    throws ConnectorException
{
    try
    {
        if (!ExtensionModuleManager.getInstance().isSourceInUse(file)) {
            return;
        }
        byte data[] = ExtensionModuleManager.getInstance().getSource(file);
        java.io.InputStream is = ObjectConverterUtil.convertToInputStream(data);
        Properties props = new Properties();
        props.load(is);
        exclusionList = new ArrayList(props.size());
        String key;
        for(Iterator it = props.keySet().iterator(); it.hasNext(); exclusionList.add(((String)props.get(key)).trim().toLowerCase()))
        {
            key = (String)it.next();
            logger.logInfo("Exec Connector - exclude: " + props.get(key));//$NON-NLS-1$
        }

    }
    catch(IOException err)
    {
        throw new ConnectorException(err, ExecPlugin.Util.getString("ExecConnector.Error_loading_exclusion_properties", file)); //$NON-NLS-1$
    }
    catch(ExtensionModuleNotFoundException err)
    {
        throw new ConnectorException(ExecPlugin.Util.getString("ExecConnector.Exclusion_file_not_found", file)); //$NON-NLS-1$
    }
    catch(MetaMatrixComponentException err1)
    {
        throw new ConnectorException(err1, ExecPlugin.Util.getString("ExecConnector.Unable_to_load_extension_module", file)); //$NON-NLS-1$
    }
}
    
    protected void setExclusionList(List list) {
        this.exclusionList = new ArrayList(list);
    }
    
    
}
