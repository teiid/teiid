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

import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.comm.platform.socket.SocketVMController;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListener;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.DhKeyGenerator;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.internal.process.DQPWorkContext;

/**
 * Sockets implementation of the communication framework class representing the server's view of a client connection.
 * Implements the server-side of the sockets messaging protocol.  
 * The client side of the protocol is implemented in SocketServerInstance.
 * Users of this class are expected to provide a WorkerPool for processing incoming messages.  Users must also call read().
 * Users also provide a ServerListener implementation.  The ServerListener is the application level object 
 * processing the application level messages.
 */
public class SocketClientInstance implements ChannelListener, ClientInstance {
	
	private final ObjectChannel objectSocket;
    private final WorkerPool workerPool;
    private final ClientServiceRegistry server;
    private Cryptor cryptor;
    private boolean usingEncryption; 
    private DhKeyGenerator keyGen;
    private DQPWorkContext workContext = new DQPWorkContext();
        
    public SocketClientInstance(ObjectChannel objectSocket, WorkerPool workerPool, ClientServiceRegistry server, boolean isClientEncryptionEnabled) {
        this.objectSocket = objectSocket;
        this.workerPool = workerPool;
        this.server = server;
        this.usingEncryption = isClientEncryptionEnabled;
    }
    
    public void send(Message message, Serializable messageKey) {
    	if (LogManager.isMessageToBeRecorded(SocketVMController.SOCKET_CONTEXT, MessageLevel.DETAIL)) {
            LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, " message: " + message + " for request ID:" + messageKey); //$NON-NLS-1$ //$NON-NLS-2$
        }
    	message.setMessageKey(messageKey);
    	objectSocket.write(message);
    }
    
    /** 
     * @return Returns the cryptor.
     */
    public Cryptor getCryptor() {
        return this.cryptor;
    }

	public void exceptionOccurred(Throwable t) {
		LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, t, "Unhandled exception, closing client instance"); //$NON-NLS-1$
	}

	public void onConnection() throws CommunicationException {
        Handshake handshake = new Handshake();
        handshake.setVersion(SocketListener.getVersionInfo());
        
        if (usingEncryption) {
            keyGen = new DhKeyGenerator();
            byte[] publicKey;
			try {
				publicKey = keyGen.createPublicKey();
			} catch (CryptoException e) {
				throw new CommunicationException(e);
			}
            handshake.setPublicKey(publicKey);
        } 
        this.objectSocket.write(handshake);
	}

	private void receivedHahdshake(Handshake handshake) throws CommunicationException {
		if (usingEncryption) {
            byte[] returnedPublicKey = handshake.getPublicKey();
            
            //ensure the key information
            if (returnedPublicKey == null) {
                throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketClientInstance.invalid_sessionkey")); //$NON-NLS-1$
            }
            
            try {
				this.cryptor = keyGen.getSymmetricCryptor(returnedPublicKey);
			} catch (CryptoException e) {
				throw new CommunicationException(e);
			}
            this.keyGen = null;
        } else {
            this.cryptor = new NullCryptor();
        }
	}

	public void receivedMessage(Object msg) throws CommunicationException {
        if (msg instanceof Message) {
            processMessagePacket((Message)msg);
        } else if (msg instanceof Handshake) {
        	receivedHahdshake((Handshake)msg);
        } 
	}

	private void processMessagePacket(Message packet) {
		if (LogManager.isMessageToBeRecorded(SocketVMController.SOCKET_CONTEXT, SocketLog.DETAIL)) { 
			LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, "processing message:" + packet); //$NON-NLS-1$
        }
		workerPool.execute(new ServerWorkItem(this, packet.getMessageKey(), packet, this.server));
	}

	public void shutdown() throws CommunicationException {
		this.objectSocket.close();
	}

	public DQPWorkContext getWorkContext() {
		return this.workContext;
	}
}
