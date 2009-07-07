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
package org.teiid.transport;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import javax.crypto.SealedObject;

import org.teiid.adminapi.AdminProcessingException;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.ExceptionHolder;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.platform.socket.client.ServiceInvocationStruct;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;

public class ServerWorkItem implements Runnable {
	private final ClientInstance socketClientInstance;
	private final Serializable messageKey;
    private final Message message;
    private final ClientServiceRegistry server;
    private final SessionServiceInterface sessionService;
    
    public ServerWorkItem(ClientInstance socketClientInstance,
			Serializable messageKey, Message message,
			ClientServiceRegistry server, SessionServiceInterface sessionService) {
		this.socketClientInstance = socketClientInstance;
		this.messageKey = messageKey;
		this.message = message;
		this.server = server;
		this.sessionService = sessionService;
	}

	/**
	 * main entry point for remote method calls. encryption/decryption is
	 * handled here so that it won't be done by the io thread
	 */
	public void run() {
		DQPWorkContext.setWorkContext(this.socketClientInstance.getWorkContext());
		Message result = null;
		String service = null;
		final boolean encrypt = message.getContents() instanceof SealedObject;
        try {
            message.setContents(this.socketClientInstance.getCryptor().unsealObject(message.getContents()));
            
			if (!(message.getContents() instanceof ServiceInvocationStruct)) {
				throw new AssertionError("unknown message contents"); //$NON-NLS-1$
			}
			final ServiceInvocationStruct serviceStruct = (ServiceInvocationStruct)message.getContents();
			Object instance = server.getClientService(serviceStruct.targetClass);
			if (instance == null) {
				throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("ServerWorkItem.Component_Not_Found", serviceStruct.targetClass)); //$NON-NLS-1$
			}
			if (!(instance instanceof ILogon)) {
				DQPWorkContext workContext = this.socketClientInstance.getWorkContext();
				sessionService.validateSession(workContext.getSessionId());
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
				ResultsFuture<Serializable> future = (ResultsFuture<Serializable>) methodResult;
				future.addCompletionListener(new ResultsFuture.CompletionListener<Serializable>() {

							public void onCompletion(
									ResultsFuture<Serializable> completedFuture) {
								Message asynchResult = new Message();
								try {
									asynchResult.setContents(completedFuture.get());
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
			context = LogConstants.CTX_SERVER;
		}
		// Case 5558: Differentiate between system level errors and
		// processing errors. Only log system level errors as errors,
		// log the processing errors as warnings only
		if (e instanceof MetaMatrixProcessingException) {
        	logProcessingException(e, context);
		} else if (e instanceof AdminProcessingException) {
			logProcessingException(e, context);
		} else {
			LogManager.logError(context, e, DQPEmbeddedPlugin.Util.getString("ServerWorkItem.Received_exception_processing_request", this.socketClientInstance.getWorkContext().getConnectionID())); //$NON-NLS-1$
		}

		return new ExceptionHolder(e);
	}
	
	private void logProcessingException(Throwable e, String context) {
		Throwable cause = e;
		while (cause.getCause() != null && cause != cause.getCause()) {
			cause = cause.getCause();
		}
		StackTraceElement elem = cause.getStackTrace()[0];
		LogManager.logDetail(context, e, "Processing exception for session", this.socketClientInstance.getWorkContext().getConnectionID()); //$NON-NLS-1$ 
		LogManager.logWarning(context, DQPEmbeddedPlugin.Util.getString("ServerWorkItem.processing_error", e.getMessage(), this.socketClientInstance.getWorkContext().getConnectionID(), e.getClass().getName(), elem)); //$NON-NLS-1$
	}

}