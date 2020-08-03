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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.teiid.client.security.ILogon;
import org.teiid.client.util.ExceptionHolder;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.core.crypto.CryptoException;
import org.teiid.core.crypto.Cryptor;
import org.teiid.core.crypto.DhKeyGenerator;
import org.teiid.core.crypto.NullCryptor;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DQPWorkContext.Version;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.CommunicationException;
import org.teiid.net.socket.Handshake;
import org.teiid.net.socket.Message;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ObjectEncoder.FailedWriteException;


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
    private Cryptor cryptor;
    private ClientServiceRegistryImpl csr;
    private boolean usingEncryption;
    private DhKeyGenerator keyGen;
    private DQPWorkContext workContext = new DQPWorkContext().local(false);

    public SocketClientInstance(ObjectChannel objectSocket, ClientServiceRegistryImpl csr, boolean isClientEncryptionEnabled) {
        this.objectSocket = objectSocket;
        this.csr = csr;
        this.workContext.setSecurityHelper(csr.getSecurityHelper());
        this.usingEncryption = isClientEncryptionEnabled;
        SocketAddress address = this.objectSocket.getRemoteAddress();
        if (address instanceof InetSocketAddress) {
            InetSocketAddress addr = (InetSocketAddress)address;
            this.workContext.setClientAddress(addr.getAddress().getHostAddress());
            this.workContext.setClientHostname(addr.getHostString());
        }
    }

    public void send(Message message, Serializable messageKey) {
        message.setMessageKey(messageKey);
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, "send message: " + message); //$NON-NLS-1$
        }
        objectSocket.write(message);
    }

    /**
     * @return Returns the cryptor.
     */
    public Cryptor getCryptor() {
        return this.cryptor;
    }

    public void exceptionOccurred(Throwable t) {
        //Object encoding may fail, so send a specific type of message to indicate there was a problem
        if (objectSocket.isOpen() && !isClosedException(t)) {
            if (workContext.getClientVersion().compareTo(Version.EIGHT_4) >= 0 && t instanceof FailedWriteException) {
                FailedWriteException fwe = (FailedWriteException)t;
                if (fwe.getObject() instanceof Message) {
                    Message m = (Message)fwe.getObject();
                    if (!(m.getMessageKey() instanceof ExceptionHolder)) {
                        Message exception = new Message();
                        exception.setContents(m.getMessageKey());
                        exception.setMessageKey(new ExceptionHolder(fwe.getCause()));
                        objectSocket.write(exception);
                        LogManager.log(getLevel(t), LogConstants.CTX_TRANSPORT, t, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40113));
                        return;
                    }
                }
            }
            if (workContext.getClientVersion().compareTo(Version.EIGHT_6) >= 0) {
                Message exception = new Message();
                exception.setMessageKey(new ExceptionHolder(t));
                objectSocket.write(exception);
                LogManager.log(getLevel(t), LogConstants.CTX_TRANSPORT, t, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40113));
                return;
            }
        }
        int level = getLevel(t);
        LogManager.log(level, LogConstants.CTX_TRANSPORT, LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)||level<MessageLevel.WARNING?t:null, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40114, t.getMessage()));
        objectSocket.close();
    }

    static int getLevel(Throwable t) {
        if (!(t instanceof IOException)) {
            return MessageLevel.ERROR;
        }
        if (ExceptionUtil.getExceptionOfType(t, ClosedChannelException.class) != null || ExceptionUtil.getExceptionOfType(t, SocketException.class) != null) {
            return MessageLevel.DETAIL;
        }
        if (isClosedException(t)) {
            return MessageLevel.DETAIL;
        }
        return MessageLevel.WARNING;
    }

    //netty notifies listeners before closing, so we try to detect close rather than writing to an invalid connection
    private static boolean isClosedException(Throwable t) {
        if (!(t instanceof IOException)) {
            return false;
        }
        String message = t.getMessage();
        if ((t.getCause() == null || t.getCause() == t) && message != null && (message.equals("Connection reset by peer") || message.equals("Broken pipe") || StringUtil.indexOfIgnoreCase(message, "closed") >= 0)) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            return true;
        }
        return false;
    }

    @Override
    public void onConnection(SSLEngine sslEngine) throws CommunicationException {
        Handshake handshake = new Handshake();
        if (sslEngine != null) {
            SSLSession session = sslEngine.getSession();
            this.workContext.setSSLSession(session);
        }
        handshake.setAuthType(csr.getAuthenticationType());
        if (usingEncryption) {
            keyGen = new DhKeyGenerator();
            byte[] publicKey;
            try {
                handshake.setPublicKeyLarge(keyGen.createPublicKey(true));
            } catch (CryptoException e) {
                //not supported on this platform
            }
            try {
                publicKey = keyGen.createPublicKey(false);
            } catch (CryptoException e) {
                 throw new CommunicationException(RuntimePlugin.Event.TEIID40051, e);
            }
            handshake.setPublicKey(publicKey);
        }
        this.objectSocket.write(handshake);
    }

    @Override
    public void disconnected() {
        if (workContext.getSessionId() != null) {
            workContext.runInContext(new Runnable() {
                @Override
                public void run() {
                    try {
                        csr.getClientService(ILogon.class).logoff();
                    } catch (Exception e) {
                        LogManager.logDetail(LogConstants.CTX_TRANSPORT, e, "Exception closing client instance"); //$NON-NLS-1$
                    }
                }
            });
        }
    }

    private void receivedHahdshake(Handshake handshake) throws CommunicationException {
        String clientVersion = handshake.getVersion();
        this.workContext.setClientVersion(Version.getVersion(clientVersion));
        if (usingEncryption) {
            byte[] returnedPublicKey = handshake.getPublicKey();
            byte[] returnedPublicKeyLarge = handshake.getPublicKeyLarge();

            boolean large = false;
            //ensure the key information
            if (returnedPublicKey == null) {
                if (returnedPublicKeyLarge == null) {
                    throw new CommunicationException(RuntimePlugin.Event.TEIID40052, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40052));
                }
                large = true;
                returnedPublicKey = returnedPublicKeyLarge;
            }
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_TRANSPORT, large?"2048":"1024", "key exchange being used."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            boolean useCbc = handshake.isCbc();
            try {
                this.cryptor = keyGen.getSymmetricCryptor(returnedPublicKey, "08.03".compareTo(clientVersion) > 0, SocketClientInstance.class.getClassLoader(), large, useCbc); //$NON-NLS-1$
            } catch (CryptoException e) {
                 throw new CommunicationException(RuntimePlugin.Event.TEIID40053, e);
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
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, "processing message:" + packet); //$NON-NLS-1$
        }
        workContext.getSession().setLastPingTime(System.currentTimeMillis());
        if (this.workContext.getSecurityHelper() != null) {
            this.workContext.getSecurityHelper().clearSecurityContext();
        }
        final ServerWorkItem work = new ServerWorkItem(this, packet.getMessageKey(), packet, this.csr);
        this.workContext.runInContext(work);
    }

    public void shutdown() throws CommunicationException {
        this.objectSocket.close();
    }

    public DQPWorkContext getWorkContext() {
        return this.workContext;
    }
}
