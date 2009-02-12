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

package com.metamatrix.common.callback;

import java.util.List;
import java.util.ListIterator;

import com.metamatrix.common.object.ObjectDefinitionImpl;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinitionGroup;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ToolboxPlugin;

/**
 * Implementations of this interface are passed to a CallbackHandler,
 * allowing underlying services the ability to interact with a calling
 * application to retrieve specific authentication data such as usernames
 * and passwords, or to display certain information, such as error and warning messages.
 * Following the completion of the <code>handle</code> method on CallbackHandler,
 * the component that generated the callbacks can then obtain the information
 * from the callbacks.
 * <p>
 * Callback implementations do not retrieve or display the information requested
 * by underlying security services. Callback implementations simply provide
 * the means to pass such requests to applications, and for applications,
 * if appropriate, to return requested information back to the underlying
 * security services.
 */
public class CallbackImpl extends ObjectDefinitionImpl implements Callback {

    private CallbackChoices choices;
    private int response;
    private PropertiedObject propertiedObject;
    private PropertiedObjectEditor propertiedObjectEditor;
    private List propertyDefinitionGroups;
    private ListIterator groupIterator = null;
    boolean sequential = false;


    public CallbackImpl(String name, String displayName, CallbackChoices choices) {
        this(name, displayName, choices, null, null, null, false);
    }

    public CallbackImpl(String name, String displayName, CallbackChoices choices,
            PropertiedObject propertiedObject, PropertiedObjectEditor propertiedObjectEditor) {
        this(name, displayName, choices, propertiedObject, propertiedObjectEditor, null, false);
    }

    public CallbackImpl(String name, String displayName, CallbackChoices choices,
            PropertiedObject propertiedObject, PropertiedObjectEditor propertiedObjectEditor,
            List propertyDefinitionGroups, boolean isSequential ) {
        super(name, displayName);
        this.choices = choices;
        this.propertiedObject = propertiedObject;
        this.propertiedObjectEditor = propertiedObjectEditor;
        this.propertyDefinitionGroups = propertyDefinitionGroups;

        // set response to default value
        response = choices.getDefaultOption();
    }


    /**
     * return the response that this callback received from the callback handler.
     */
    public int getResponse() {
        return this.response;
    }


    // ****************
    // Callback Methods
    // ****************

    /**
     * get the choice options that are available for providing this callback's setChoice() method.
     * @return the CallbackChoices for this object.
     */
    public CallbackChoices getChoices() {
        return this.choices;
    }

    /**
     * set the response to this callback from the options provide by this callback's
     * getOptions() method.
     * @param response the response value from among the chioce options:
     *     YES, NO, CANCEL, or OK.
     */
    public void setResponse(int responseIndex) {
        this.response = responseIndex;
    }

    /**
     * determine if this Callback contains a PropertiedObject that should be modified to provide
     * values for the resource requesting the callback.
     * @return true if this callback contains a PropertiedObject and a PropertiedObjectEditor.  If
     * false, then the resource is requesting a direct response to this object's getDisplayName().
     */
    public boolean hasPropertiedObject() {
        return this.propertiedObject != null;
    }

    /**
     * determine if this Callback contains one or more PropertyDefinitionGroups.  The groups are
     * used to indicate collections of PropertyDefinitions which the CallbackHandler may process
     * either sequentially (if isSequential() returns true), or all at once.
     * @return true if this callback contains a both PropertiedObject and one or more
     * PropertyDefinitionGroups.  If false, and hasPropertiedObject() returns true, then the resource
     * is requesting that the PropertiedObject be editied directly and all at once by the CallbackHandler.
     */
    public boolean hasPropertyDefinitionGroups() {
        boolean result = false;
        if ( this.propertyDefinitionGroups != null && ! this.propertyDefinitionGroups.isEmpty() ) {
            result = true;
        }
        return result;
    }

    /**
     * determine if the List of PropertyDefinitionGroups contained by this object should be processed
     * one-by-one in order (as in a "wizard"), or all at once (as in a tabbed panel).
     * @return true if this callback contains PropertyDefinitionGroups that should be processed
     * sequentially, as with a wizard.  Returns false if the groups can be processed all at once or
     * if this Callback contains no groups.
     */
    public boolean isSequential() {
        return sequential;
    }

    /**
     * return the PropertiedObjectEditor that the CallbackHandler should user to
     * edit this Callback's PropertiedObject.
     * @return the editor for this Callback's PropertiedObject.  May be null if this
     * Callback does not contain a PropertiedObject.
     */
    public PropertiedObjectEditor getEditor() {
        return this.propertiedObjectEditor;
    }

    /**
     * return a PropertiedObject that the CallbackHandler should use and modify to
     * hold any values in response to this Callback.
     * @return the PropertiedObject which for this Callback's resource is requesting values.  May be
     * null if this Callback does not contain a PropertiedObject.
     */
    public PropertiedObject getPropertiedObject() {
        return this.propertiedObject;
    }

    /**
     * return the list of PropertyDefinitionGroups contained by this Callback.
     * @return the list PropertyDefinitionGroups, null if there are none.
     */
    public List getPropertyDefinitionGroups() {
        return this.propertyDefinitionGroups;
    }

    /**
     * determine if there is a next PropertyDefinitionGroup.
     */
    public boolean hasNextGroup() {
        boolean result = false;
        if ( getGroupIterator() != null ) {
            result = getGroupIterator().hasNext();
        }
        return result;
    }

    /**
     * determine if there is a previous PropertyDefinitionGroup.
     */
    public boolean hasPreviousGroup() {
        boolean result = false;
        if ( getGroupIterator() != null ) {
            result = getGroupIterator().hasPrevious();
        }
        return result;
    }

    /**
     * return the next PropertyDefinitionGroup.
     * @return the next PropertyDefinitionGroup, null if there are no more;
     */
    public PropertyDefinitionGroup getNextGroup() {
        PropertyDefinitionGroup result = null;
        if ( hasNextGroup() ) {
            Object o = getGroupIterator().next();
            Assertion.assertTrue( o instanceof PropertyDefinitionGroup,  ToolboxPlugin.Util.getString("ERR.003.009.0013", o.getClass().getName()));
            result = (PropertyDefinitionGroup) o;
        }
        return result;
    }

    /**
     * return the previous PropertyDefinitionGroup.
     * @return the previous PropertyDefinitionGroup, null if there is no previous Group;
     */
    public PropertyDefinitionGroup getPreviousGroup() {
        PropertyDefinitionGroup result = null;
        if ( hasNextGroup() ) {
            Object o = getGroupIterator().previous();
            Assertion.assertTrue( o instanceof PropertyDefinitionGroup,  ToolboxPlugin.Util.getString("ERR.003.009.0013", o.getClass().getName()));

            result = (PropertyDefinitionGroup) o;
        }
        return result;
    }

    private ListIterator getGroupIterator() {
        if ( groupIterator == null && propertyDefinitionGroups != null ) {
            groupIterator = propertyDefinitionGroups.listIterator();
        }
        return groupIterator;
    }

}

