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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;


/**
 * Implmentation of text connector.
 */
public class FakeExecConnector extends ExecConnector {
    
    

     
    protected void loadExclusionFile(String file)
    throws ConnectorException
{
    try
    {
        FileInputStream is = new FileInputStream(file);
//        
//        byte data[] = ExtensionModuleManager.getInstance().getSource(file);
//        java.io.InputStream is = ObjectConverterUtil.convertToInputStream(data);
        Properties props = new Properties();
        props.load(is);
        List exclusionList = new ArrayList(props.size());
        String key;
        for(Iterator it = props.keySet().iterator(); it.hasNext(); exclusionList.add(((String)props.get(key)).trim().toLowerCase()))
        {
            key = (String)it.next();
        }
        this.setExclusionList(exclusionList);

    }
    catch(IOException err)
    {
        throw new ConnectorException(err, ExecPlugin.Util.getString("ExecConnector.Error_loading_exclusion_properties", file)); //$NON-NLS-1$
    }
}
    
    
}
