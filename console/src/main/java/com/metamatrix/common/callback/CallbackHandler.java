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

import java.io.IOException;

/**
 * An application implements a CallbackHandler and passes it to underlying
 * security services so that they may interact with the application to retrieve
 * specific authentication data, such as usernames and passwords, or to display
 * certain information, such as error and warning messages.
 * <p>
 * CallbackHandlers are implemented in an application-dependent fashion.
 * For example, implementations for an application with a graphical user
 * interface (GUI) may pop up windows to prompt for requested information
 * or to display error messages. An implementation may also choose to
 * obtain requested information from an alternate source without asking
 * the end user.
 */
public interface CallbackHandler {

    /**
     * Retrieve or display the requested information in the Callback object.
     * <p>
     * The implementation should process all callback objects before returning, since
     * the caller of this method is free to retrieve the requested information from the
     * callback objects immediately after this method returns.
     * @param callbacks an array of Callback objects provided by the method caller,
     * and which contain the information requested to be retrieved or displayed.
     * @param source the object that is considered the source of the callbacks.
     * @throws IOException if an input or output error occurs
     * @throws UnsupportedCallbackException if the implementation of this method
     * does not support one or more of the Callbacks specified in the callbacks parameter
     */
    void handle(Callback callback, Object source) throws IOException, UnsupportedCallbackException;

}

