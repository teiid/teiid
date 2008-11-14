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

package com.metamatrix.common.callback;

import java.util.List;

import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinitionGroup;

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
public interface Callback extends ObjectDefinition {

    /**
     * get the choice options that are available for providing this callback's setChoice() method.
     * @return the options :
     *     UNSPECIFIED_OPTION, YES_NO_OPTION, YES_NO_CANCEL_OPTION, or OK_CANCEL_OPTION.  Note that
     * for UNSPECIFIED_OPTION, any value provided to setChoice() will be acceptable and the value
     * will not be examined buy the resource requesting the callback.
     */
    CallbackChoices getChoices();

    /**
     * set the response to this callback from the options provide by this callback's
     * getOptions() method.
     * @param response the index of available choices from the CallbackChoices object:
     */
    void setResponse(int response);

    /**
     * determine if this Callback contains a PropertiedObject that should be modified to provide
     * values for the resource requesting the callback.
     * @return true if this callback contains a PropertiedObject and a PropertiedObjectEditor.  If
     * false, then the resource is requesting a direct response to this object's getDisplayName().
     */
    boolean hasPropertiedObject();

    /**
     * determine if this Callback contains one or more PropertyDefinitionGroups.  The groups are
     * used to indicate collections of PropertyDefinitions which the CallbackHandler may process
     * either sequentially (if isSequential() returns true), or all at once.
     * @return true if this callback contains a both PropertiedObject and one or more
     * PropertyDefinitionGroups.  If false, and hasPropertiedObject() returns true, then the resource
     * is requesting that the PropertiedObject be editied directly and all at once by the CallbackHandler.
     */
    boolean hasPropertyDefinitionGroups();

    /**
     * determine if the List of PropertyDefinitionGroups contained by this object should be processed
     * one-by-one in order (as in a "wizard"), or all at once (as in a tabbed panel).
     * @return true if this callback contains PropertyDefinitionGroups that should be processed
     * sequentially, as with a wizard.  Returns false if the groups can be processed all at once or
     * if this Callback contains no groups.
     */
    boolean isSequential();

    /**
     * return the PropertiedObjectEditor that the CallbackHandler should user to
     * edit this Callback's PropertiedObject.
     * @return the editor for this Callback's PropertiedObject.  May be null if this
     * Callback does not contain a PropertiedObject.
     */
    PropertiedObjectEditor getEditor();

    /**
     * return a PropertiedObject that the CallbackHandler should use and modify to
     * hold any values in response to this Callback.
     * @return the PropertiedObject which for this Callback's resource is requesting values.  May be
     * null if this Callback does not contain a PropertiedObject.
     */
    PropertiedObject getPropertiedObject();

    /**
     * return the list of PropertyDefinitionGroups contained by this Callback.
     * @return the list PropertyDefinitionGroups, null if there are none.
     */
    List getPropertyDefinitionGroups();

    /**
     * determine if there is a next PropertyDefinitionGroup.
     */
    boolean hasNextGroup();

    /**
     * determine if there is a previous PropertyDefinitionGroup.
     */
    boolean hasPreviousGroup();

    /**
     * return the next PropertyDefinitionGroup.
     * @return the next PropertyDefinitionGroup, null if there are no more;
     */
    PropertyDefinitionGroup getNextGroup();

    /**
     * return the previous PropertyDefinitionGroup.
     * @return the previous PropertyDefinitionGroup, null if there is no previous Group;
     */
    PropertyDefinitionGroup getPreviousGroup();

}

