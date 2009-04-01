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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.PropertyValidations;
import com.metamatrix.common.object.ConfigurationPropertyObjDisplayComparator;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyAccessPolicy;
import com.metamatrix.common.object.PropertyAccessPolicyImpl;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.object.PropertyType;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionNotSupportedException;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.transaction.manager.SimpleUserTransaction;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;

/**
 * <p>This class serves as both a PropertiedObjectEditor and a
 * UserTransaction for the editing of ComponentObjects and the
 * PropertyDefinitions associated to them through their ComponentType.</p>
 *
 * <p>This class fronts a ConfigurationObjectEditor, and a
 * ConfigurationAdminAPIFacade.  It can be used in one of two ways
 * (corresponding to the two contstructors).</p>
 *
 * <p>The first way is to use the no-arg
 * {@link #ConfigurationPropertiedObjectEditor constructor}.  This
 * implementation will then support transactions through the
 * {@link com.metamatrix.common.transaction.UserTransaction UserTransaction}
 * API, which this object also implements, and which
 * {@link com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel PropertiedObjectPanel}
 * uses automatically.</p>
 *
 * <p>The second way to use this implementation is to use the
 * second {@link #ConfigurationPropertiedObjectEditor(ModificationActionQueue) constructor}
 * which takes a {@link com.metamatrix.common.actions.ModificationActionQueue ModificationActionQueue}
 * parameter.  This allows a client to control the transactional
 * nature of this implementation - the client can manually manage the list
 * of Action objects in the ModificationActionQueue, and execute them
 * as a transaction whenever the client deems it appropriate.  Automatic
 * transactional support will be <i>disabled</i> in this implementation;
 * the methods implementing the
 * {@link com.metamatrix.common.transaction.UserTransaction UserTransaction}
 * interface will be disabled.</p>
 */
public class ConfigurationPropertiedObjectEditor implements PropertiedObjectEditor,
                                                            StringUtil.Constants,
                                                            UserTransaction {

    private ConnectionInfo connectionInfo;
    private ConfigurationObjectEditor editor;
    private ModificationActionQueue queue;
    private PropertyAccessPolicy policy;
    private Object source;

    private boolean automaticTransaction = false;
    private ConfigurationPropertyObjDisplayComparator comp = new com.metamatrix.common.object.ConfigurationPropertyObjDisplayComparator();
    private PropertyValidations validations = new PropertyValidations();
    
    

    /**
     * Package-level constructor, this class may only be instantiated
     * from within this subpackage.  Transactions will be automatically
     * supported if this constructor is used.
     * @param connectionInfo Helper used to obtain connections to the server.
     */
    ConfigurationPropertiedObjectEditor(ConnectionInfo connectionInfo){
        this.connectionInfo = connectionInfo;
        this.policy = new PropertyAccessPolicyImpl();
        automaticTransaction = true;
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
     * @param connectionInfo Helper used to obtain connections to the server.
     * @param destination ModificationActionQueue that is to retain the
     * Actions produced by this PropertiedObjectEditor.
     */
    ConfigurationPropertiedObjectEditor(ConnectionInfo connectionInfo, ModificationActionQueue queue){
        this.connectionInfo = connectionInfo;
        this.policy = new PropertyAccessPolicyImpl();
        this.editor = new BasicConfigurationObjectEditor(true);
        this.editor.setDestination(queue);
        this.queue = queue;
        automaticTransaction = false;
    }

    
    //#######################PropertiedObjectEditor methods##################

    

    ModificationActionQueue getQueue() {
        return queue;
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
        Collection result = null;

        try {
            result = getConfigurationManager().getAllCachedComponentTypeDefinitions(componentObject.getComponentTypeID());

        } catch (ConfigurationException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0022, AdminPlugin.Util.getString(AdminMessages.ADMIN_0022));
        } catch (InvalidSessionException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0023, AdminPlugin.Util.getString(AdminMessages.ADMIN_0023));
        } catch (AuthorizationException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0024, AdminPlugin.Util.getString(AdminMessages.ADMIN_0024));
        } catch (MetaMatrixComponentException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0025, AdminPlugin.Util.getString(AdminMessages.ADMIN_0025));
        }

        if (result!=null) {
            List r = null;
            if (componentObject instanceof ConnectorBinding) {
                r = setIsModifiable(result, Configuration.NEXT_STARTUP_ID);
            } else if (componentObject instanceof ComponentDefn) {
                r = setIsModifiable(result, ((ComponentDefn)componentObject).getConfigurationID());
            }else if(componentObject instanceof Configuration){
                r =  setIsModifiable(result, (ConfigurationID)((Configuration)componentObject).getID());
            }else if(componentObject instanceof DeployedComponent) {
                r = setIsModifiable(result, ((DeployedComponent)componentObject).getConfigurationID());
            } 
            Collections.sort(r, comp);
             return r;

        }
        return new ArrayList(result);
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
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "PropertyDefinition")); //$NON-NLS-1$
        }
        return def.getAllowedValues();
    }

    /**
     * Return whether this editor may be used to set property values on
     * the specified PropertiedObject.
     * @param obj the propertied object; may not be null
     * @return true if the object may not be modified, or false otherwise.
     * @throws AssertionError if <code>obj</code> is null
     */
    public boolean isReadOnly(PropertiedObject obj){
        if(obj == null){
            Assertion.isNotNull(obj,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "PropertiedObject")); //$NON-NLS-1$
        }
        return this.policy.isReadOnly(obj);
    }

    /**
     * Obtain from the specified PropertiedObject the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be an empty collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0", or the NO_VALUE reference if the specified object
     * does not contain the specified PropertyDefinition
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public Object getValue(PropertiedObject obj, PropertyDefinition def){
        ComponentObject componentObject = assertComponentObject(obj);
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "PropertyDefinition")); //$NON-NLS-1$
        }

        Object value = componentObject.getProperties().getProperty(def.getName());

        Object result = value;
        if ( PropertyType.BOOLEAN.equals(def.getPropertyType()) ) {
            if ( value instanceof String ) {
                if ( Boolean.TRUE.toString().equalsIgnoreCase((String) value) ) {
                    result = Boolean.TRUE;
                } else {
                    result = Boolean.FALSE;
                }
            }
        }
        //TODO: handle other types
        // if the value and the default are the same, return null for the
        // property panel so that the value will be displayed in blue
        if (def.getDefaultValue() != null &&
            def.getDefaultValue().equals(result)) {
                return null;
            }

        return result;
        //TODO dependant properties?
    }

    /**
     * Return whether the specified value is considered valid.  The value is not
     * valid if the propertied object does not have the specified property definition,
     * or if it does but the value is inconsistent with the requirements of the
     * property definition.
     * @param obj the propertied object whose property value is to be validated;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be validated; may not be null
     * @param value the proposed value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0"
     * @return true if the value is considered valid, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public boolean isValidValue(PropertiedObject obj, PropertyDefinition def, Object value ){
//        ComponentObject componentObject =
        assertComponentObject(obj);
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "PropertyDefinition")); //$NON-NLS-1$
        }

        // Check for a null value ...
        if ( value == null ) {
            return ( def.getMultiplicity().getMinimum() == 0 ); // only if minimum==0 is value allowed to be null
        }

        boolean isValid = false;
        // From this point forward, the value is never null
        isValid = def.getPropertyType().isValidValue(value);

        //if the value has a carriage return or a new line, it is considered
        //invalid
        if (isValid && value instanceof String){
            
            try {
                validations.isPropertyValid(def.getName(), (String) value);
            } catch (Exception e) {
                return false;
            }
            
            String stringValue = (String)value;
            isValid = (stringValue.lastIndexOf(CARRIAGE_RETURN) == -1);
            isValid = (stringValue.lastIndexOf(NEW_LINE) == -1);
        }

        return isValid;
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

//System.out.println("setValue(" + obj + ", " + def + ", " + value + ")");
//if (value != null) {
//    System.out.println("value obj type = " + value.getClass().getName());
//}
        try{
            ComponentTypeDefn typeDefn = getConfigurationManager().getComponentTypeDefn(def, componentObject);
            if (!typeDefn.getComponentTypeID().equals(componentObject.getComponentTypeID())){
                //then this prop defn is not defined for this type, which means
                //it must be defined for a super type.  It must be set to this
                //type, however
                this.getEditor().createComponentTypeDefn(componentObject.getComponentTypeID(), def, typeDefn.isEffectiveImmediately());
            }

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
                    this.getEditor().removeProperty(componentObject, def.getName());
                } else {
                    this.getEditor().setProperty(componentObject, def.getName(), def.getDefaultValue().toString());
                }

                // Remove property
                //this.getEditor().removeProperty(componentObject, def.getName());
            } else {
                // Set new property value
                String stringValue = null;
                if (value instanceof Boolean) {
                    Boolean bValue = (Boolean) value;
                    stringValue = Boolean.toString(bValue.booleanValue());
                    stringValue = stringValue.toLowerCase();
                } else { // assume string
                    stringValue = (String)value;
                }
                this.getEditor().setProperty(componentObject, def.getName(), stringValue);
            }
        } catch (ConfigurationException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0026, AdminPlugin.Util.getString(AdminMessages.ADMIN_0026));
        } catch (InvalidSessionException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0023, AdminPlugin.Util.getString(AdminMessages.ADMIN_0023));
        } catch (AuthorizationException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0027, AdminPlugin.Util.getString(AdminMessages.ADMIN_0027));
        } catch (MetaMatrixComponentException e){
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0028, AdminPlugin.Util.getString(AdminMessages.ADMIN_0028));
        } catch (ClassCastException e){
//            e.printStackTrace();
            throw new MetaMatrixRuntimeException(e, AdminMessages.ADMIN_0029, AdminPlugin.Util.getString(AdminMessages.ADMIN_0029));
        }
    }

    /**
      */
    public PropertyAccessPolicy getPolicy(){
        return this.policy;
    }

    /**
      */
    public void setPolicy(PropertyAccessPolicy policy){
        this.policy = policy;
    }


    //#######################PropertyAccessPolicy methods##################

    public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def) {
        if(obj == null){
            Assertion.isNotNull(obj,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018,"PropertiedObject")); //$NON-NLS-1$
        }
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018,"PropertyDefinition")); //$NON-NLS-1$
        }
        return this.policy.isReadOnly(obj,def);
    }

    public void setReadOnly(PropertiedObject obj, PropertyDefinition def, boolean readOnly) {
        if(obj == null){
            Assertion.isNotNull(obj,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018,"PropertiedObject")); //$NON-NLS-1$
        }
        if(def == null){
            Assertion.isNotNull(def,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018,"PropertyDefinition")); //$NON-NLS-1$
        }
        this.policy.setReadOnly(obj,def,readOnly);
    }

    public void setReadOnly(PropertiedObject obj, boolean readOnly) {
        if(obj == null){
            Assertion.isNotNull(obj,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018,"PropertiedObject")); //$NON-NLS-1$
        }
        this.policy.setReadOnly(obj,readOnly);
    }

    public void reset(PropertiedObject obj) {
        assertComponentObject(obj);
        this.policy.reset(obj);
    }



    //#######################UserTransactionFactory methods##################

    /**
     * Create a new instance of a UserTransaction that may be used to
     * read information.  Read transactions do not have a source object
     * associated with them (since they never directly modify data).
     * <p>
     * The returned transaction object will not be bound to an underlying
     * system transaction until <code>begin()</code> is called on the returned
     * object.
     * @return the new transaction object
     */
    public UserTransaction createReadTransaction(){
        return new SimpleUserTransaction();
    }

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information.  The transaction will <i>not</i> have a source object
     * associated with it.
     * <p>
     * The returned transaction object will not be bound to an underlying
     * system transaction until <code>begin()</code> is called on the returned
     * object.
     * @return the new transaction object
     */
    public UserTransaction createWriteTransaction(){
        return this;
    }

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information. The source object will be used for all events that are
     * fired as a result of or as a product of this transaction.
     * <p>
     * The returned transaction object will not be bound to an underlying
     * system transaction until <code>begin()</code> is called on the returned
     * object.
     * @param source the object that is considered to be the source of the transaction;
     * may be null
     * @return the new transaction object
     */
    public UserTransaction createWriteTransaction(Object source){
        this.source = source;
        return this;
    }


    //#######################UserTransaction methods#########################

    /**
     * Obtain the status of the transaction represented by this object.
     * Note: this class does not currently support status, and will
     * return either
     * {@link com.metamatrix.common.transaction.TransactionStatus#STATUS_UNKNOWN}
     * for automatic transaction mode (indicating there <i>might</I> be a transaction), or
     * {@link com.metamatrix.common.transaction.TransactionStatus#STATUS_NO_TRANSACTION}
     * if this object is not in automatic transaction mode.
     * @return The transaction status.
     */
    public int getStatus() throws TransactionException{
        if (this.automaticTransaction){
            return TransactionStatus.STATUS_UNKNOWN;
        }
        return TransactionStatus.STATUS_NO_TRANSACTION;
    }

    /**
     * Create a new transaction and associate it with this object.
     * @throws TransactionNotSupportedException if the current thread is already
     * associated with a transaction and the manager does not support
     * nested system transactions.
     */
    public void begin() throws TransactionException{
        if (this.automaticTransaction && this.queue != null && this.queue.hasActions()){
            throw new TransactionNotSupportedException(AdminMessages.ADMIN_0030, AdminPlugin.Util.getString(AdminMessages.ADMIN_0030));
        }
    }

    /**
     * Modify the value of the timeout value that is associated with the
     * transactions represented by this object.
     * Note: this implementation has no timeout, and does not support timeout.
     * @param seconds The value of the timeout in seconds. If the value is
     * zero, the transaction service restores the default value.
     * @throws IllegalStateException Thrown if this object is not associated with a transaction
     */
    public void setTransactionTimeout(int seconds) throws TransactionException{
        //do nothing
    }

    /**
     * Modify the transaction associated with this object such that
     * the only possible outcome of the transaction is to roll back the transaction.
     * Note: this implementation does not support this method, and the
     * method will do nothing
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    public void setRollbackOnly() throws TransactionException{
        //do nothing
    }

    /**
     * Complete the transaction associated with this object.
     * When this method completes, the thread becomes associated with no transaction.
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    public void commit() throws TransactionException{
        if (this.automaticTransaction && this.queue != null){
            try{
                getConfigurationAPI().executeTransaction(this.queue.popActions());
            } catch (ModificationException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            } catch (ConfigurationException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            } catch (InvalidSessionException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            } catch (AuthorizationException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            } catch (MetaMatrixComponentException e){
                throw new TransactionException(e, AdminMessages.ADMIN_0031, AdminPlugin.Util.getString(AdminMessages.ADMIN_0031));
            }
        }
    }

    /**
     * Roll back the transaction associated with this object.
     * When this method completes, the thread becomes associated with no
     * transaction.
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    public void rollback() throws TransactionException{
        if (this.automaticTransaction && this.queue != null){
            this.queue.clear();
        }
    }

    /**
     * Return the (optional) reference to the object that is considered
     * the source of the transaction represented by this object.
     * This is used, for example, to set the source of all events occuring within this
     * transaction.
     * @return the source object, which may be null
     */
    public Object getSource() throws TransactionException{
        return this.source;
    }


    //#######################misc methods#########################

    

    protected boolean isAutomaticTransaction() {
        return automaticTransaction;
    }

    protected ComponentObject assertComponentObject(PropertiedObject obj){
        if(obj == null){
            Assertion.isNotNull(obj,AdminPlugin.Util.getString(AdminMessages.ADMIN_0018,"PropertiedObject")); //$NON-NLS-1$
        }
        
        if(!(obj instanceof ComponentObject) ){
            Assertion.assertTrue( obj instanceof ComponentObject, AdminPlugin.Util.getString(AdminMessages.ADMIN_0032));
        }
        return (ComponentObject) obj;
    }

    protected ConfigurationObjectEditor getEditor() {
        if (this.editor == null){
            this.editor = new BasicConfigurationObjectEditor(true);
        }
        return this.editor;
    }


    /**
    * This method will resolve whether or not the PropertyDefinitions
    * in the list of passed in ComponentTypeDefinitions should be modifiable or
    * not.  In order to determine this it uses the effectiveImmediately parameter,
    * the isModifiable() state of the PropertyDefinition
    * itself, and the Configuration that the currently edited ComponentDefn
    * belongs to.
    *
    * @param componentTypeDefns list of ComponentTypeDefns to be interrogated
    * @param id the ConfigurationID of the object being edited by this editor.
    * @return a list of PropertyDefinition objects with their isModifiable
    * attribute set correctly.
    */
    protected List setIsModifiable(Collection componentTypeDefns, ConfigurationID id) {
        List result = new ArrayList(componentTypeDefns.size());
        Iterator iterator = componentTypeDefns.iterator();

        ConfigurationID nextStartupID = Configuration.NEXT_STARTUP_ID;

        while (iterator.hasNext()) {
            ComponentTypeDefn cDefn = (ComponentTypeDefn)iterator.next();
            PropertyDefinition propDefn = cDefn.getPropertyDefinition();
            if (propDefn.isModifiable()) {
                PropertyDefinitionImpl pDefn = new PropertyDefinitionImpl(propDefn);
                if(id.equals(nextStartupID)) {
                    pDefn.setModifiable(true);
                }else {
                    pDefn.setModifiable(false);
                }
                result.add(pDefn);
            }else {
                result.add(propDefn);
            }
        }
        return result;
    }
    
    
    protected ConfigurationAdminAPI getConfigurationAPI() {
        return ModelManager.getConfigurationAPI(connectionInfo);
    }

    protected ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connectionInfo);
    }


}

