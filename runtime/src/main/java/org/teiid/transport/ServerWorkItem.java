/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package org.teiid.transport;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import org.teiid.adminapi.AdminProcessingException;
import org.teiid.client.util.ExceptionHolder;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.crypto.CryptoException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.socket.Message;
import org.teiid.net.socket.ServiceInvocationStruct;
import org.teiid.query.QueryPlugin;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ClientServiceRegistryImpl.ClientService;


public class ServerWorkItem implements Runnable {

    private final ClientInstance socketClientInstance;
    private final Serializable messageKey;
    private final Message message;
    private final ClientServiceRegistryImpl csr;

    public ServerWorkItem(ClientInstance socketClientInstance, Serializable messageKey, Message message, ClientServiceRegistryImpl server) {
        this.socketClientInstance = socketClientInstance;
        this.messageKey = messageKey;
        this.message = message;
        this.csr = server;
    }

    /**
     * main entry point for remote method calls.
     */
    public void run() {
        Message result = null;
        String loggingContext = null;
        final boolean encrypt = !(message.getContents() instanceof ServiceInvocationStruct);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            try {
                Thread.currentThread().setContextClassLoader(this.csr.getCallerClassloader());
            } catch(Throwable t) {
                // ignore
            }
            message.setContents(this.socketClientInstance.getCryptor().unsealObject(message.getContents()));
            if (!(message.getContents() instanceof ServiceInvocationStruct)) {
                throw new AssertionError("unknown message contents"); //$NON-NLS-1$
            }
            final ServiceInvocationStruct serviceStruct = (ServiceInvocationStruct)message.getContents();
            final ClientService clientService = this.csr.getClientService(serviceStruct.targetClass.getName());
            loggingContext = clientService.getLoggingContext();
            Method m = clientService.getReflectionHelper().findBestMethodOnTarget(serviceStruct.methodName, serviceStruct.args);
            Object methodResult;
            try {
                methodResult = m.invoke(clientService.getInstance(), serviceStruct.args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (ResultsFuture.class.isAssignableFrom(m.getReturnType()) && methodResult != null) {
                ResultsFuture<Object> future = (ResultsFuture<Object>) methodResult;
                future.addCompletionListener(new ResultsFuture.CompletionListener<Object>() {

                            public void onCompletion(
                                    ResultsFuture<Object> completedFuture) {
                                Message asynchResult = new Message();
                                try {
                                    asynchResult.setContents(completedFuture.get());
                                } catch (InterruptedException e) {
                                    asynchResult.setContents(processException(e, clientService.getLoggingContext()));
                                } catch (ExecutionException e) {
                                    asynchResult.setContents(processException(e.getCause(), clientService.getLoggingContext()));
                                }
                                sendResult(asynchResult, encrypt);
                            }

                        });
            } else { // synch call
                Message resultHolder = new Message();
                resultHolder.setContents(methodResult);
                result = resultHolder;
            }
        } catch (Throwable t) {
            Message holder = new Message();
            holder.setContents(processException(t, loggingContext));
            result = holder;
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
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
                 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40071, e);
            }
        }
        socketClientInstance.send(result, messageKey);
    }

    private Serializable processException(Throwable e, String context) {
        if (context == null) {
            context = LogConstants.CTX_TRANSPORT;
        }
        // Case 5558: Differentiate between system level errors and
        // processing errors. Only log system level errors as errors,
        // log the processing errors as warnings only
        if (e instanceof TeiidProcessingException) {
            logProcessingException(e, context);
        } else if (e instanceof AdminProcessingException) {
            logProcessingException(e, context);
        } else {
            LogManager.logError(context, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40017, this.socketClientInstance.getWorkContext().getSessionId()));
        }

        return new ExceptionHolder(e);
    }

    private void logProcessingException(Throwable e, String context) {
        Throwable cause = e;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }
        StackTraceElement elem = cause.getStackTrace()[0];
        String msg = RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40011, e.getMessage(), this.socketClientInstance.getWorkContext().getSessionId(), e.getClass().getName(), elem);
        if (LogManager.isMessageToBeRecorded(context, MessageLevel.DETAIL)) {
            LogManager.logWarning(context, e, msg);
        } else {
            LogManager.logWarning(context, msg + QueryPlugin.Util.getString("stack_info")); ////$NON-NLS-1$
        }
    }
}