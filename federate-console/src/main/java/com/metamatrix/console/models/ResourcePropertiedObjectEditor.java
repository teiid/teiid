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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ResourceModel;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.jdbc.JDBCReservedWords;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.core.util.Assertion;


/**
 * Created on Jun 11, 2002
 *
 * This propertied object editor is for internal resources.
 * The internal resources is not designed like the configuration
 * framework to used propertied objects.  So this editor
 * is used to present to the client the look of using propertied
 * objects.  Also, internal resources do not have the concept
 * of operational and next startup configurations.
 */
public class ResourcePropertiedObjectEditor extends ConfigurationPropertiedObjectEditor {

    private ConfigurationID configID;
    private Map resources = new HashMap();
    private ConfigurationObjectEditor editor;
    


    /**
     * Package-level constructor, this class may only be instantiated
     * from within this subpackage.  Transactions will be automatically
     * supported if this constructor is used.     
     * @param connectionInfo Helper used to obtain connections to the server.
     * @param configurationID is the configuration from which the allowable resource
     * pools will be derived.
     */
    ResourcePropertiedObjectEditor(ConnectionInfo connectionInfo, ConfigurationID configurationID){
        super(connectionInfo);
        this.configID = configurationID;
    }

    /**
     * Package-level constructor, this class may only be instantiated
     * from within this subpackage.  As a convenience, this constructor
     * takes a {@link com.metamatrix.common.actions.ModificationActionQueue}
     * arguement, which allows the client manual control over any transactions
     * this implementation will do.  Transactions, through the
     * {@link com.metamatrix.common.transaction.UserTransaction UserTransaction}
     * interface, will be <i>disabled</i>; the client will manually have to
     * control their own transactions using their
     * ModificationActionQueue reference and their own
     * ConfigurationAdminAPIFacade reference.
     * @param adminAPI reference to a ConfigurationAdminAPIFacade object
     * @param connectionInfo Helper used to obtain connections to the server.
     * @param destination ModificationActionQueue that is to retain the
     * Actions produced by this PropertiedObjectEditor.
     */
    ResourcePropertiedObjectEditor(ConnectionInfo connectionInfo, ConfigurationID configurationID, ModificationActionQueue destination){
        super(connectionInfo, destination);
        this.configID = configurationID;

    }
    
    protected ConfigurationObjectEditor getEditor() {
        if (this.editor == null){
            this.editor = new BasicConfigurationObjectEditor(false);
        }
        return this.editor;
    }
    
    public void begin() throws TransactionException{
    }
    
    
    
    public void rollback() throws TransactionException{
    }    

    /**
     * Get the allowed values for the property on the specified object.
     * By default, this implementation simply returns the allowed values in the
     * supplied PropertyDefinition instance.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the unmodifiable list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     * @see #hasAllowedValues
     */
    public List getAllowedValues(PropertiedObject obj, PropertyDefinition def) {
        assertComponentObject(obj);
        
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "PropertyDefinition")); //$NON-NLS-1$
        }

        return def.getAllowedValues();
    }



   /**
     * Obtain the list of PropertyDefinitions that apply to the specified object's type.
     * @param obj the propertied object for which the PropertyDefinitions are
     * to be obtained; may not be null
     * @return an unmodifiable list of the PropertyDefinition objects that
     * define the properties for the object; never null but possibly empty
     * @throws AssertionError if <code>obj</code> is null
     */
    public List getPropertyDefinitions(PropertiedObject obj){
        ComponentObject componentObject = assertComponentObject(obj);
        List result = null;

        ComponentType type = ResourceModel.getComponentType(componentObject.getName());
        Iterator defns = type.getComponentTypeDefinitions().iterator();
        result = new ArrayList(type.getComponentTypeDefinitions().size());
        while(defns.hasNext()) {
            ComponentTypeDefn defn = (ComponentTypeDefn) defns.next();
            PropertyDefinition pd = defn.getPropertyDefinition();
 
            
            if (pd.isConstrainedToAllowedValues()) {
                
                    if (defn.getFullName().equalsIgnoreCase("metamatrix.common.pooling.resource.name")) { //$NON-NLS-1$
                        PropertyDefinitionImpl pDefn = new PropertyDefinitionImpl(pd);

                        pDefn.setAllowedValues(Collections.EMPTY_LIST);
                        result.add(pDefn);
                    } else {
                        result.add(pd);
                    }
            } else {
                result.add(pd);
                
            }
            
        }
        return result;
    }


   /**
     * Set on the specified PropertiedObject the value defined by the specified
     * PropertyDefinition.  If <code>null</code> is passed in as
     * the value parameter, this implementation will take that to mean that
     * the value is reverting to its default value.  In this case, the default
     * value will be stored as the value; the value entry in the config
     * database will NOT be deleted, but will be assigned the default value.
     * @param obj the propertied object whose property value is to be set;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition.  If null, this will be interpreted
     * as reverting to the default value.
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public void setValue(PropertiedObject obj, PropertyDefinition def, Object value){
        ComponentObject componentObject = assertComponentObject(obj);
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "PropertyDefinition")); //$NON-NLS-1$
        }

        componentObject = getObject(componentObject);

//System.out.println("setValue(" + obj + ", " + def + ", " + value + ")");
//if (value != null) {
//    System.out.println("value obj type = " + value.getClass().getName());
//}

            if ( value == null ) {
                //If the value is null, this most likely means that the user
                //of the PropertiedObjectPanel has entered in the default
                //value of this property - in that case, the
                //PropertiedObjectPanel passes a null to this method.
                //   And, in the configuration stuff,
                //the default values need to be recorded as values in the
                //database, just like any other value.  So, rather than delete
                //the value (like below, commented out), the default value
                //of the property definition will be saved as the property
                //value
                //   NOTE: The effect of this is that this method can NOT
                //be used to DELETE property values

// SW 4/27/02
// NEW NOTE: If default value == null, property is deleted.
// If we do not delete the property then the code that uses this property will get a value
// and not use the default that is hard coded.
// If the default value != null then we will set the value to the default value.

                if (def.getDefaultValue() == null) {
                    componentObject = this.getEditor().removeProperty(componentObject, def.getName());
                } else {
                    componentObject = this.getEditor().setProperty(componentObject, def.getName(), def.getDefaultValue().toString());
                }

                // Remove property
                //this.getEditor().removeProperty(componentObject, def.getName());
            } else {
                // Set new property value
                String stringValue = null;
                if (value instanceof Boolean) {
                    Boolean bValue = (Boolean) value;
                    stringValue = (bValue.booleanValue()?JDBCReservedWords.TRUE:JDBCReservedWords.FALSE);
                    stringValue = stringValue.toLowerCase();
                } else { // assume string
                    stringValue = (String)value;
                }
                componentObject = this.getEditor().setProperty(componentObject, def.getName(), stringValue);
            }

            storeObject(componentObject);

    }

    /**
     * This method obtains the latest saved version of this object so that it
     * can be used to be passed into the editor when the save is performed
     */
    private ComponentObject getObject(ComponentObject object) {

        if (resources.containsKey(object.getID())) {
            return (ComponentObject) resources.get(object.getID());
        }

        storeObject(object);
        return object;

    }

    /**
     * This method saves this version of the object as the latest changes
     * so that when commit is called, this will be the version of changes
     * that will be saved.
     */
    private void storeObject(ComponentObject object) {
        resources.put(object.getID(), object);
    }

     /**
     * Complete the transaction associated with this object.
     * When this method completes, the thread becomes associated with no transaction.
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    public void commit() throws TransactionException{

    }


    /**
     * Implmented for the purpose of the console to control
     * the commit via the apply button.
     */
    public void apply() throws TransactionException {
        if (this.isAutomaticTransaction()){

            try{
                // have to move the object from the Map to this type of
                // Collection because cannot use map.getValues()
                // due to serialization issues
                Collection r = new ArrayList(resources.size());
                r.addAll(resources.values());

                getConfigurationAPI().saveResources(r);


            } catch (ConfigurationException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0047, AdminPlugin.Util.getString(AdminMessages.ADMIN_0047));
            } catch (InvalidSessionException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            } catch (AuthorizationException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            } catch (MetaMatrixComponentException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            }
        }
    }


}
