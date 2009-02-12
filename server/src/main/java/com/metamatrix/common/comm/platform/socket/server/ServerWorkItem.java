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

/**
 * 
 */
package com.metamatrix.common.comm.platform.socket.server;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import javax.crypto.SealedObject;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.exception.ExceptionHolder;
import com.metamatrix.common.comm.platform.socket.SocketVMController;
import com.metamatrix.common.comm.platform.socket.client.ServiceInvocationStruct;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.server.util.LogConstants;

public class ServerWorkItem implements Runnable {
	private final ClientInstance socketClientInstance;
	private final Serializable messageKey;
    private final Message message;
    private final ClientServiceRegistry<SessionServiceInterface> server;
    
    public ServerWorkItem(ClientInstance socketClientInstance,
			Serializable messageKey, Message message,
			ClientServiceRegistry server) {
		this.socketClientInstance = socketClientInstance;
		this.messageKey = messageKey;
		this.message = message;
		this.server = server;
	}

	/**
	 * main entry point for remote method calls. encryption/decryption is
	 * handled here so that it won't be done by the io thread
	 */
	public void run() {
		DQPWorkContext.setWorkContext(this.socketClientInstance.getWorkContext());
		if (LogManager.isMessageToBeRecorded(SocketVMController.SOCKET_CONTEXT, MessageLevel.DETAIL)) {
			LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, "forwarding message to listener:" + message); //$NON-NLS-1$
		}
		Message result = null;
		String service = null;
		final boolean encrypt = message.getContents() instanceof SealedObject;
        try {
        	/* Defect 15211
    		 * If a CNFE occurred while deserializing the packet, the packet message should be a
    		 * MessageHolder containing the exception. Since we can no longer continue processing,
    		 * we should notify the client of the problem immediately.
    		 */
        	if (message.getContents() instanceof Throwable) {
        		LogManager.logWarning(SocketVMController.SOCKET_CONTEXT, (Throwable)message.getContents(), 
		                     "Exception while deserializing message packet."); //$NON-NLS-1$
                result = message;
            } else {
	            message.setContents(this.socketClientInstance.getCryptor().unsealObject(message.getContents()));
	            
				if (!(message.getContents() instanceof ServiceInvocationStruct)) {
					throw new AssertionError("unknown message contents"); //$NON-NLS-1$
				}
				final ServiceInvocationStruct serviceStruct = (ServiceInvocationStruct)message.getContents();
				Object instance = server.getClientService(serviceStruct.targetClass);
				if (instance == null) {
					throw new ComponentNotFoundException(PlatformPlugin.Util.getString("ServerWorkItem.Component_Not_Found", serviceStruct.targetClass)); //$NON-NLS-1$
				}
				if (!(instance instanceof ILogon)) {
					DQPWorkContext workContext = this.socketClientInstance.getWorkContext();
					server.getSessionService().validateSession(workContext.getSessionId());
				}
				service = serviceStruct.targetClass;
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

								public void onCompletion(
										ResultsFuture completedFuture) {
									Message asynchResult = new Message();
									try {
										asynchResult.setContents((Serializable) completedFuture.get());
									} catch (InterruptedException e) {
										asynchResult.setContents(processException(e, serviceStruct.targetClass));
									} catch (ExecutionException e) {
										asynchResult.setContents(processException(e.getCause(), serviceStruct.targetClass));
									}
									sendResult(asynchResult, encrypt);
								}

							});
				} else { // synch call
					Message resultHolder = new Message();
					resultHolder.setContents((Serializable)methodResult);
					result = resultHolder;
				}
			}
		} catch (Throwable t) {
			Message holder = new Message();
			holder.setContents(processException(t, service));
			result = holder;
		}
		if (result != null) {
			sendResult(result, encrypt);
		}
	}

	void sendResult(Message result, boolean encrypt) {
		if (encrypt) {
			try {
				result.setContents(socketClientInstance.getCryptor().sealObject(result.getContents()));
			} catch (CryptoException e) {
				throw new MetaMatrixRuntimeException(e);
			}
		}
		socketClientInstance.send(result, messageKey);
	}

	private Serializable processException(Throwable e, String service) {
		String context = null;
		if (service != null) {
			context = this.server.getLoggingContextForService(service);
		}
		if (context == null) {
			context = LogConstants.CTX_ROUTER;
		}
		// Case 5558: Differentiate between system level errors and
		// processing errors. Only log system level errors as errors,
		// log the processing errors as warnings only
		String msg = PlatformPlugin.Util.getString("ServerWorkItem.Received_exception_processing_request"); //$NON-NLS-1$
		if (e instanceof MetaMatrixProcessingException) {
			LogManager.logWarning(context, e, msg);
		} else if (e instanceof AdminProcessingException) {
			LogManager.logWarning(context, e, msg);
		} else {
			LogManager.logError(context, e, msg);
		}
		
		if (e instanceof AdminException) {
			return e;
		}

		return new ExceptionHolder(e);
	}

}