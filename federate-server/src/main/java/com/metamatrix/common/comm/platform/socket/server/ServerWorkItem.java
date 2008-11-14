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

/**
 * 
 */
package com.metamatrix.common.comm.platform.socket.server;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.platform.socket.SocketVMController;
import com.metamatrix.common.comm.platform.socket.client.ServiceInvocationStruct;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.server.util.LogConstants;

public class ServerWorkItem implements Runnable {
	private final ClientInstance socketClientInstance;
	private final Serializable messageKey;
    private final Message message;
    private final ClientServiceRegistry server;
    
    public ServerWorkItem(ClientInstance socketClientInstance,
			Serializable messageKey, Message message,
			ClientServiceRegistry server) {
		this.socketClientInstance = socketClientInstance;
		this.messageKey = messageKey;
		this.message = message;
		this.server = server;
	}

    public void run() {
    	DQPWorkContext.setWorkContext(this.socketClientInstance.getWorkContext());
    	if (LogManager.isMessageToBeRecorded(SocketVMController.SOCKET_CONTEXT, MessageLevel.DETAIL)) {
            LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, "forwarding message to listener:" + message); //$NON-NLS-1$
        }
		Message result = null;
        try {
        	/* Defect 15211
    		 * If a CNFE occurred while deserializing the packet, the packet message should be a
    		 * MessageHolder containing the exception. Since we can no longer continue processing,
    		 * we should notify the client of the problem immediately.
    		 */
        	if (message.getContents() instanceof CommunicationException) {
        		LogManager.logWarning(SocketVMController.SOCKET_CONTEXT, (Throwable)message.getContents(), 
		                     "Exception while deserializing message packet."); //$NON-NLS-1$
                result = message;
            } else {
	            message.setContents(this.socketClientInstance.getCryptor().unsealObject(message.getContents()));
	            
				if (!(message.getContents() instanceof ServiceInvocationStruct)) {
					throw new AssertionError("unknown message contents"); //$NON-NLS-1$
				}
				ServiceInvocationStruct serviceStruct = (ServiceInvocationStruct)message.getContents();
				Object instance = server.getClientService(serviceStruct.targetClass);
				if (instance == null) {
					throw new ComponentNotFoundException(PlatformPlugin.Util.getString("ServerWorkItem.Component_Not_Found", serviceStruct.targetClass)); //$NON-NLS-1$
				}
				if (!(instance instanceof ILogon)) {
					DQPWorkContext workContext = this.socketClientInstance.getWorkContext();
					server.getSessionService().validateSession(workContext.getSessionId());
				}
				ReflectionHelper helper = new ReflectionHelper(instance.getClass());
				Method m = helper.findBestMethodOnTarget(serviceStruct.methodName, serviceStruct.args);
				Object methodResult;
				try {
					methodResult = m.invoke(instance, serviceStruct.args);
				} catch (InvocationTargetException e) {
					throw e.getCause();
				}
				if (ResultsFuture.class.isAssignableFrom(m.getReturnType()) && methodResult != null) {
					ResultsFuture future = (ResultsFuture) methodResult;
					future.addCompletionListener(new ResultsFuture.CompletionListener() {
	
						public void onCompletion(ResultsFuture completedFuture) {
							Message asynchResult = new Message();
							try {
								asynchResult.setContents((Serializable)completedFuture.get());
							} catch (InterruptedException e) {
								//cannot happen
							} catch (ExecutionException e) {
								asynchResult.setContents(e.getCause());
								logException(e.getCause());
							}
							socketClientInstance.send(asynchResult, messageKey);
						}
						
					});
				} else { //synch call
					Message resultHolder = new Message();
					resultHolder.setContents((Serializable)methodResult);
					result = resultHolder;
				}
            }
        } catch (Throwable t) {
        	logException(t);
            Message holder = new Message();
            holder.setContents(t);
            result = holder;
        } 
        if (result != null) {
            this.socketClientInstance.send(result, messageKey);
        }
    }   
    
    private void logException(Throwable e) {
        //Case 5558: Differentiate between system level errors and
        //processing errors.  Only log system level errors as errors, 
        //log the processing errors as warnings only
        String msg = PlatformPlugin.Util.getString("ServerWorkItem.Received_exception_processing_request"); //$NON-NLS-1$  
        if(e instanceof MetaMatrixProcessingException) {                          
            LogManager.logWarning(LogConstants.CTX_ROUTER, e, msg);
        }else {
            LogManager.logError(LogConstants.CTX_ROUTER, e, msg);
        }
    }
}