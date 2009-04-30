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

package com.metamatrix.common.actions;

import java.io.Serializable;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

public class AbstractObjectEditor implements ObjectEditor, Serializable {

    protected static final int FIRST_COMMAND = SET;
    protected static final int LAST_COMMAND = REMOVE;

    protected static final String SET_LBL = "Set"; //$NON-NLS-1$
    protected static final String ADD_LBL = "Add"; //$NON-NLS-1$
    protected static final String REMOVE_LBL = "Remove"; //$NON-NLS-1$

    private static final String[] LABELS = { SET_LBL, ADD_LBL, REMOVE_LBL };

    private ModificationActionQueue destination = null;

    private boolean createActions = false;


    /**
     * Create an instance of this editor.
     */
    public AbstractObjectEditor() {
    }

    public AbstractObjectEditor(boolean createActions) {
        this.createActions = createActions;
    }

    /**
     * Get the action destination for this object.  The modification actions,
     * if used, are created by the <code>modifyObject</code> method.
     * @return the action queue into which the modification actions are placed.
     */
    public ModificationActionQueue getDestination() {
        return destination;
    }
    /**
     * Set the destination for this object.
     * @param destination the new destination queue for any modification
     * actions created by this editor.
     */
    public void setDestination(ModificationActionQueue destination) {
        this.destination = destination;
    }

    public boolean doCreateActions() {
      return createActions;
    }

    public void setCreateActions(boolean doCreateActions) {
      this.createActions = doCreateActions;
    }

    protected String getLabel( int command ) {
        return ( command <= LAST_COMMAND ) ? LABELS[command] : "<unknown command>"; //$NON-NLS-1$
    }

    protected void verifyCommand( int command ) {
        if ( command < FIRST_COMMAND || command > LAST_COMMAND ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0001, String.valueOf(command)));
        }
    }

    /**
     * Subclass helper method that simply verifies that the specified target is either an instance of
     * the specified class (or interface).
     * @param target the target or target identifier.
     * @param requiredClass the class/interface that the target must be an instance of.
     * @return the target object (for convenience)
     * @throws IllegalArgumentException if either the target is not an instance of the specified class.
     */
    protected Object verifyTargetClass( Object target, Class requiredClass ) throws IllegalArgumentException {
        if ( ! requiredClass.isInstance(target) ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0002,
            			new Object[] {target.getClass().getName(), requiredClass.getName()}));
        }
        return target;
    }

    protected void createCreationAction(Object targetId, Object value) {
        if ( this.createActions ) {
            ActionDefinition action = new CreateObject(targetId, value );
            this.getDestination().addAction(action);
        }
    }


    protected void createAddNamedAction(Object targetId, AttributeDefinition attrDefn, String name, Object value) {
        if ( this.createActions ) {
            ActionDefinition action = new AddNamedObject(targetId,attrDefn, name, value );
            this.getDestination().addAction(action);
        }
    }

    protected void createAddAction(Object targetId, AttributeDefinition attrDefn, Object value) {
        if ( this.createActions ) {
            ActionDefinition action = new AddObject(targetId, attrDefn, value );
            this.getDestination().addAction(action);
        }
    }

    protected void createExchangeNamedAction(Object targetId, AttributeDefinition attrDefn, String name, Object oldValue, Object newValue ) {
      if ( this.createActions ) {
          ActionDefinition action = new ExchangeNamedObject(targetId, attrDefn, name, oldValue ,newValue);
          this.getDestination().addAction(action);
      }
    }

    protected void createExchangeAction(Object targetId, AttributeDefinition attrDefn, Object oldValue, Object newValue ) {
      if ( this.createActions ) {
          ActionDefinition action = new ExchangeObject(targetId, attrDefn, oldValue ,newValue);
          this.getDestination().addAction(action);
      }
    }

    protected void createRemoveNamedAction(Object targetId, AttributeDefinition attrDefn, String name, Object oldValue) {
        if ( this.createActions ) {
            ActionDefinition action = new RemoveNamedObject(targetId, attrDefn, name, oldValue);
            this.getDestination().addAction(action);
        }
    }

    protected void createRemoveAction(Object targetId, AttributeDefinition attriDefn, Object oldValue) {
        if ( this.createActions ) {
            ActionDefinition action = new RemoveObject(targetId, attriDefn, oldValue);
            this.getDestination().addAction(action);
        }
    }

    protected void createExchangeBoolean(Object targetId, AttributeDefinition attrDefn, boolean oldValue, boolean newValue) {
        if ( this.createActions ) {
          ActionDefinition action = new ExchangeBoolean(targetId, attrDefn, oldValue, newValue );
          this.getDestination().addAction(action);
      }
    }

    protected void createDestroyAction(Object targetId, Object targetObject) {
        if ( this.createActions ) {
            ActionDefinition action = new DestroyObject(targetId,targetObject);
            this.getDestination().addAction(action);
        }
    }


}

