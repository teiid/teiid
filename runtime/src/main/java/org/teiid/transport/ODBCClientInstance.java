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
package org.teiid.transport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.net.ssl.SSLEngine;

import org.teiid.core.util.ReflectionHelper;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.CommunicationException;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.net.socket.ServiceInvocationStruct;
import org.teiid.odbc.ODBCClientRemote;
import org.teiid.odbc.ODBCServerRemote;
import org.teiid.odbc.ODBCServerRemoteImpl;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.PgFrontendProtocol.PGRequest;

public class ODBCClientInstance implements ChannelListener{

    private ODBCClientRemote client;
    private ODBCServerRemoteImpl server;
    private ReflectionHelper serverProxy = new ReflectionHelper(ODBCServerRemote.class);
    private ConcurrentLinkedQueue<PGRequest> messageQueue = new ConcurrentLinkedQueue<PGRequest>();

    public ODBCClientInstance(final ObjectChannel channel, TeiidDriver driver, LogonImpl logonService) {
        this.client = (ODBCClientRemote)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {ODBCClientRemote.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODBC, MessageLevel.TRACE)) {
                    LogManager.logTrace(LogConstants.CTX_ODBC, "invoking client method:", method.getName(), Arrays.deepToString(args)); //$NON-NLS-1$
                }
                ServiceInvocationStruct message = new ServiceInvocationStruct(args, method.getName(),ODBCServerRemote.class);
                channel.write(message);
                return null;
            }
        });
        this.server = new ODBCServerRemoteImpl(this, driver, logonService) {
            @Override
            protected synchronized void doneExecuting() {
                super.doneExecuting();
                while (!server.isExecuting()) {
                    PGRequest request = messageQueue.poll();
                    if (request == null) {
                        break;
                    }
                    if (!server.isErrorOccurred() || request.struct.methodName.equals("sync")) { //$NON-NLS-1$
                        processMessage(request.struct);
                    }
                }
            }
        };
    }

    public ODBCClientRemote getClient() {
        return client;
    }

    @Override
    public void disconnected() {
        server.terminate();
    }

    @Override
    public void exceptionOccurred(Throwable t) {
        int level = SocketClientInstance.getLevel(t);
        LogManager.log(level, LogConstants.CTX_TRANSPORT, LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)||level<MessageLevel.WARNING?t:null, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40114, t.getMessage()));
        server.terminate();
    }

    @Override
    public void onConnection(SSLEngine engine) throws CommunicationException {
        //ssl handling is in ODBCServerRemote.initialize
    }

    @Override
    public void receivedMessage(Object msg) throws CommunicationException {
        if (msg instanceof PGRequest) {
            PGRequest request = (PGRequest)msg;
            synchronized (server) {
                if (server.isExecuting()) {
                    //queue until done
                    messageQueue.add(request);
                    return;
                }
                if (server.isErrorOccurred() && !request.struct.methodName.equals("sync")) { //$NON-NLS-1$
                    //discard until sync
                    return;
                }
            }
            processMessage(request.struct);
        }
    }

    private void processMessage(ServiceInvocationStruct serviceStruct) {
        try {
            Method m = this.serverProxy.findBestMethodOnTarget(serviceStruct.methodName, serviceStruct.args);
            try {
                // since the postgres protocol can produce more than single response
                // objects to a request, all the methods are designed to return void.
                // and relies on client interface to build the responses.
                m.invoke(this.server, serviceStruct.args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        } catch (Throwable e) {
            this.server.errorOccurred(e);
        }
    }

}
