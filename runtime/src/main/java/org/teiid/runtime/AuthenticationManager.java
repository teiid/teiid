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
package org.teiid.runtime;

import static org.teiid.services.SessionServiceImpl.concatenation;
import static org.teiid.adminapi.impl.VDBMetaData.AUTHENTICATION_MAX_SESSIONS_ALLOWED_PER_USER_PROPERTY;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.services.SessionServiceImpl;

public class AuthenticationManager implements VDBLifeCycleListener {
    
    private SessionServiceImpl sessionService;
    
    public AuthenticationManager(SessionServiceImpl sessionService) {
        this.sessionService = sessionService;
    }

    @Override
    public void added(String name, CompositeVDB vdb) {
    }

    @Override
    public void beforeRemove(String name, CompositeVDB vdb) {
    }

    @Override
    public void removed(String name, CompositeVDB vdb) {
        
        String authenticationProperties = vdb.getVDB().getPropertyValue(AUTHENTICATION_MAX_SESSIONS_ALLOWED_PER_USER_PROPERTY);
        if(authenticationProperties != null && !authenticationProperties.equals("")) { //$NON-NLS-1$
            Properties authenticationProps = loadAuthenticationProps(authenticationProperties);
            Enumeration<?> en = authenticationProps.propertyNames();
            while(en.hasMoreElements()){
                String user = (String) en.nextElement();
                this.sessionService.getPermissionMap().remove(concatenation(name, user));
            }
        }
    }

    @Override
    public void finishedDeployment(String name, CompositeVDB vdb) {

        String authenticationProperties = vdb.getVDB().getPropertyValue(AUTHENTICATION_MAX_SESSIONS_ALLOWED_PER_USER_PROPERTY);
        if(authenticationProperties != null && !authenticationProperties.equals("")) { //$NON-NLS-1$
            Properties authenticationProps = loadAuthenticationProps(authenticationProperties);
            try {
                Enumeration<?> en = authenticationProps.propertyNames();
                while(en.hasMoreElements()){
                    String user = (String) en.nextElement();
                    this.sessionService.getPermissionMap().put(concatenation(name, user), Long.parseLong(authenticationProps.getProperty(user)));
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, authenticationProperties), e);
            }
        }
    }
    
    private Properties loadAuthenticationProps(String authenticationProperties) {
        authenticationProperties = authenticationProperties.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$
        authenticationProperties = authenticationProperties.replaceAll(";", "\n");
        InputStream is = new ByteArrayInputStream(authenticationProperties.getBytes()); //$NON-NLS-1$ //$NON-NLS-2$
        Properties authenticationProps = new Properties();
        try {
            authenticationProps.load(is);
        } catch (IOException e) {
            throw new IllegalArgumentException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, authenticationProperties), e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
            }
        }
        return authenticationProps;
    }

}
