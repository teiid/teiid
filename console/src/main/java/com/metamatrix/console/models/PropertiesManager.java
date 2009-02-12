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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.object.PropertyDefinition;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.properties.ConsolePropertiedEditor;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;

public class PropertiesManager extends Manager {

    private final static String[] UNDISPLAYED_PROPERTIES = new String[] {
            "Essential Service"}; //$NON-NLS-1$

    /**
     * PropertyModel used to get and set the two GDD related properties
     *in this case: gdd name and gdd version
     */
    private ConfigurationObjectEditor coeConfigEditor = null;

	public PropertiesManager(ConnectionInfo connection) {
		super(connection);
	}
	
    public void init() {
        super.init();
        this.setIsStale(false);
    }

    public Map getProperties()
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        try {
            return getConfiguration().getProperties();
        } catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }

    }

    public String getProperty(String key)
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        try {
            Object o = getConfiguration().getProperties().get(key);
            if ( o != null ) {
                return o.toString();
            }
            return null;
        } catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }
    }

    public void setProperty(String name, String value, int cfgNumber)
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        try {
            if (cfgNumber == ConsolePropertiedEditor.NSUCONFIGINDICATOR) {
                setProperty(name, value,getNextStartUpConfiguration() );
            }
        } catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }
    }

    public void setProperty(String name, String value, Configuration cfg)
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        try {
            getObjectEditor().setProperty(cfg, name, value);
			applyUpdates();
            this.fireModelChangedEvent(Manager.MODEL_CHANGED);
        } catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }
    }

    private ConfigurationAdminAPI getConfigAPI() {
        return ModelManager.getConfigurationAPI(getConnection());
//        return configAPI;
    }

    public Configuration getNextStartUpConfiguration()
            throws AuthorizationException, InvalidSessionException,
            ConfigurationException, MetaMatrixComponentException {
        
        Configuration nsuConfig = ModelManager.getConfigurationManager(getConnection()).getConfig(Configuration.NEXT_STARTUP_ID);
        
//        Configuration nsuConfig = getConfigAPI().getNextStartupConfiguration();
        return nsuConfig;
    }

    public Collection getNextStartUpDefn()
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        ArrayList nextStartUpDefn = new ArrayList();
        ComponentTypeDefn compTypeDefn = null;
        PropertyDefinition nDefinition = null;
        try {
            Configuration nextStartupConfig = getNextStartUpConfiguration();   //loads from server
            if ( nextStartupConfig == null) {
                //should not ne here
                throw new RuntimeException("nextStartupConfig should not be null"); //$NON-NLS-1$
            }
            ComponentTypeID configTypeID = nextStartupConfig.getComponentTypeID();
            Collection componentTypeDefns =
                getConfigAPI().getAllComponentTypeDefinitions(configTypeID); //loads from server
            Iterator iterator = componentTypeDefns.iterator();

            while (iterator.hasNext()) {
                compTypeDefn = (ComponentTypeDefn)iterator.next();
                nDefinition = compTypeDefn.getPropertyDefinition();
                if (!undisplayed(nDefinition)) {
                    nextStartUpDefn.add(nDefinition);
                }
            }

        } catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ExternalException(e);
        }
        return nextStartUpDefn;
    }

    public Properties getNSUProperties()
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        Properties nextStartupProps = null;
        try {
            Configuration nextStartupConfig = getNextStartUpConfiguration();
                //configAPI.getNextStartupConfiguration();   //loads from server
            nextStartupProps = nextStartupConfig.getProperties();
		} catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        return  nextStartupProps;
    }

    // NEW methods:

    private void applyUpdates() throws AuthorizationException,
            InvalidSessionException, ComponentNotFoundException,
            ConfigurationException, ModificationException,
            MetaMatrixComponentException {
        ModificationActionQueue maq = getObjectEditor().getDestination();
        List lstActions = maq.popActions();
//        cfgCurrConfig = getConfigurationLocked();
        ModelManager.getConfigurationAPI(getConnection()).executeTransaction( lstActions );
//		cfgCurrConfig.getID();
    }

    public  Configuration getStartUpConfiguration()
            throws AuthorizationException, InvalidSessionException,
            ComponentNotFoundException, ConfigurationException,
            ModificationException,
            MetaMatrixComponentException {
        Configuration suConfig = ModelManager.getConfigurationManager(getConnection()).getConfig(Configuration.STARTUP_ID);
        
//        Configuration suConfig = configAPI.getStartupConfiguration();
        return suConfig;
    }

    public Collection getStartUpDefn() throws ExternalException,
            AuthorizationException, ComponentNotFoundException {
        ArrayList startupDefn = new ArrayList();
        ComponentTypeDefn compTypeDefn = null;
        PropertyDefinition nDefinition = null;
        try {
            Configuration startupConfig = getStartUpConfiguration();   //loads from server
            ComponentTypeID configTypeID = startupConfig.getComponentTypeID();
            Collection componentTypeDefns = 
                    getConfigAPI().getAllComponentTypeDefinitions(configTypeID); //loads from server
            Iterator iterator = componentTypeDefns.iterator();

            while (iterator.hasNext()) {
                compTypeDefn = (ComponentTypeDefn)iterator.next();
                nDefinition = compTypeDefn.getPropertyDefinition();
                if (!undisplayed(nDefinition)) {
                    startupDefn.add(nDefinition);
                }
            }

        } catch (AuthorizationException e) {
            throw(e);
        } catch (ComponentNotFoundException e) {
            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        return startupDefn;
    }

    public Properties getSUProperties() throws ExternalException,
            AuthorizationException, ComponentNotFoundException {
        Properties startupProps = null;
        try {
            
            Configuration startupConfig = getStartUpConfiguration();
 //           Configuration startupConfig = getConfigAPI().getStartupConfiguration();   //loads from server
            startupProps = startupConfig.getProperties();
//		} catch (AuthorizationException e) {
//            throw(e);
//        } catch (ComponentNotFoundException e) {
//            throw(e);
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        return  startupProps;
    }

    private Configuration getConfiguration() throws AuthorizationException,
            InvalidSessionException, ComponentNotFoundException,
            ConfigurationException, ModificationException,
            MetaMatrixComponentException {
        // default config is NOT locked:
        return getConfigurationNotLocked();
    }


    private Configuration getConfigurationNotLocked()
            throws AuthorizationException, InvalidSessionException,
            ComponentNotFoundException, ConfigurationException,
            ModificationException,
            MetaMatrixComponentException {
        return getConfiguration( false );
    }

    private Configuration getConfiguration( boolean bGetLocked )
            throws AuthorizationException, InvalidSessionException,
            ComponentNotFoundException, ConfigurationException,
            ModificationException,
            MetaMatrixComponentException {

            return  ModelManager.getConfigurationManager(getConnection()).getConfig(Configuration.NEXT_STARTUP_ID);

    }

    private ConfigurationObjectEditor getObjectEditor()
            throws AuthorizationException, InvalidSessionException,
            ComponentNotFoundException, ConfigurationException,
            ModificationException,
            MetaMatrixComponentException {
        if ( coeConfigEditor == null ) {
            coeConfigEditor =getConfigAPI().createEditor(); 
//                ModelManager.getConfigurationAPI(getConnection()).createEditor();
        }
        return coeConfigEditor;
    }

    private boolean undisplayed(PropertyDefinition def) {
        String displayName = def.getDisplayName();
        boolean match = false;
        int i = 0;
        while ((!match) && (i < UNDISPLAYED_PROPERTIES.length)) {
            if (displayName.equalsIgnoreCase(UNDISPLAYED_PROPERTIES[i])) {
                match = true;
            } else {
                i++;
            }
        }
        return match;
    }
}
