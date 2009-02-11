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

package com.metamatrix.common.comm.platform.socket.server;

import java.io.Serializable;

import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.dqp.internal.process.DQPWorkContext;

/**
 * Represents a ClientConnection from the server's point of view.  This interface
 * can be used by a ServerListener implementation to manage the incoming
 * client connections, retrieve information about a particular connection, 
 * and send a message to a particular connection in the asynchronous message scenario.
 */
public interface ClientInstance {

    /**
     * Send a message to this particular client using the asynch message key.
     * @param message The message to send
     * @param messageKey The key sent with the asynch query
     * @throws CommunicationException If an error occurs during the send
     */
    void send(Message message, Serializable messageKey);
        
    /**
     * Shutdown the server's connection to the client.  
     * @throws CommunicationException If an error occurs during the shutdown
     */
    void shutdown() throws CommunicationException;
    
    Cryptor getCryptor();
    
    DQPWorkContext getWorkContext();
}
