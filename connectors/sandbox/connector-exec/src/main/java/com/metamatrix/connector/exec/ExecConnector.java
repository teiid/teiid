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

package com.metamatrix.connector.exec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicConnector;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.core.util.ObjectConverterUtil;

/**
 * Implementation of text connector.
 */
public class ExecConnector extends BasicConnector {
    
    private ConnectorLogger logger;
    private ConnectorEnvironment env;
    private boolean start = false;
    private List exclusionList= Collections.EMPTY_LIST;
    private String exclusionFile;

    /**
     * Initialization with environment.
     */
    @Override
    public void start( ConnectorEnvironment environment ) throws ConnectorException {
        logger = environment.getLogger();
        this.env = environment;
        
        exclusionFile = environment.getProperties().getProperty("exclusionFile"); //$NON-NLS-1$
        if(exclusionFile != null && exclusionFile.trim().length() > 0)
        {
            loadExclusionFile(exclusionFile);
        }        

        // logging
        logger = environment.getLogger();
        start = true;
        logger.logInfo("Exec Connector is started."); //$NON-NLS-1$
    }

    public void stop() {
        if(!start){
            return;
        }

        start = false;
        logger.logInfo("Exec Connector is stoped."); //$NON-NLS-1$
    }
    
    @Override
    public ConnectorCapabilities getCapabilities() {
    	return new ExecCapabilities();
    }

    /*
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(ExecutionContext context) throws ConnectorException {
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
