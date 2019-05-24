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

package org.teiid.net.socket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.client.security.Secure;
import org.teiid.client.util.ExceptionHolder;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.core.crypto.CryptoException;
import org.teiid.core.crypto.Cryptor;
import org.teiid.core.crypto.DhKeyGenerator;
import org.teiid.core.crypto.NullCryptor;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.HostInfo;


/**
 * Client view of a socket server connection that exposes remote services
 * On construction this class will create a channel and exchange a handshake.
 * That handshake will establish a {@link Cryptor} to be used for secure traffic.
 */
public class SocketServerInstanceImpl implements SocketServerInstance {

    private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

    private static AtomicInteger MESSAGE_ID = new AtomicInteger();
    private Map<Serializable, ResultsReceiver<Object>> asynchronousListeners = new ConcurrentHashMap<Serializable, ResultsReceiver<Object>>();

    private long synchTimeout;
    private HostInfo info;

    private ObjectChannel socketChannel;
    private Cryptor cryptor;
    private String serverVersion;
    private HashMap<Class<?>, Object> serviceMap = new HashMap<Class<?>, Object>();

    private boolean hasReader;
    private int soTimeout;

    public SocketServerInstanceImpl(HostInfo info, long synchTimeout, int soTimeout) {
        if (!info.isResolved()) {
            throw new AssertionError("Expected HostInfo to be resolved"); //$NON-NLS-1$
        }
        this.info = info;
        this.synchTimeout = synchTimeout;
        this.soTimeout = soTimeout;
    }

    public synchronized void connect(ObjectChannelFactory channelFactory) throws CommunicationException, IOException {
        this.socketChannel = channelFactory.createObjectChannel(info);
        try {
            doHandshake();
        } catch (CommunicationException e) {
            this.socketChannel.close();
            throw e;
        } catch (IOException e) {
            this.socketChannel.close();
            throw e;
        }
    }

    @Override
    public HostInfo getHostInfo() {
        return info;
    }

    @Override
    public InetAddress getLocalAddress() {
        if (socketChannel != null) {
            return socketChannel.getLocalAddress();
        }
        return null;
    }

    private void doHandshake() throws IOException, CommunicationException {
        Handshake handshake = null;
        boolean sentInit = false;
        long handShakeRetries = 1;
        if (this.soTimeout > 0) {
            handShakeRetries = Math.max(1, synchTimeout/this.soTimeout);
        }
        for (int i = 0; i < handShakeRetries; i++) {
            try {
                Object obj = this.socketChannel.read();

                if (!(obj instanceof Handshake)) {
                     throw new SingleInstanceCommunicationException(JDBCPlugin.Event.TEIID20009, null, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20009));
                }
                handshake = (Handshake)obj;
                break;
            } catch (ClassNotFoundException e1) {
                 throw new SingleInstanceCommunicationException(JDBCPlugin.Event.TEIID20010, e1, e1.getMessage());
            } catch (SocketTimeoutException e) {
                if (!sentInit && !this.info.isSsl()) {
                    //write a dummy initialization value - if the server is actually ssl, this can cause the server side handshake to fail, otherwise it's ignored
                    //TODO: could always do this initialization in the non-ssl case and not wait for a timeout
                    this.socketChannel.write(null);
                    sentInit = true;
                }
                if (i == handShakeRetries - 1) {
                    throw e;
                }
            } catch (IOException e) {
                if (sentInit && !this.info.isSsl()) {
                    throw new SingleInstanceCommunicationException(JDBCPlugin.Event.TEIID20032, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20032));
                }
                throw e;
            }
        }

        try {
            /*if (!getVersionInfo().equals(handshake.getVersion())) {
                 throw new CommunicationException(JDBCPlugin.Event.TEIID20011, NetPlugin.Util.getString(JDBCPlugin.Event.TEIID20011, getVersionInfo(), handshake.getVersion()));
            }*/
            serverVersion = handshake.getVersion();
            handshake.setVersion();

            byte[] serverPublicKey = handshake.getPublicKey();
            byte[] serverPublicKeyLarge = handshake.getPublicKeyLarge();

            if (serverPublicKey != null) {
                DhKeyGenerator keyGen = new DhKeyGenerator();
                boolean large = false;
                if (serverPublicKeyLarge != null) {
                    try {
                        byte[] publicKey = keyGen.createPublicKey(true);
                        handshake.setPublicKey(null);
                        handshake.setPublicKeyLarge(publicKey);
                        serverPublicKey = serverPublicKeyLarge;
                        large = true;
                    } catch (CryptoException e) {
                        //not supported on this platform
                    }
                }
                if (!large) {
                    byte[] publicKey = keyGen.createPublicKey(false);
                    handshake.setPublicKey(publicKey);
                    handshake.setPublicKeyLarge(null);
                }
                boolean useCbc = handshake.isCbc();
                this.cryptor = keyGen.getSymmetricCryptor(serverPublicKey, "08.03".compareTo(serverVersion) > 0, this.getClass().getClassLoader(), large, useCbc);  //$NON-NLS-1$
            } else {
                this.cryptor = new NullCryptor();
            }

            this.socketChannel.write(handshake);
        } catch (CryptoException e) {
             throw new CommunicationException(JDBCPlugin.Event.TEIID20012, e, e.getMessage());
        }
    }

    @Override
    public String getServerVersion() {
        return serverVersion;
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    public void send(Message message, ResultsReceiver<Object> listener, Serializable messageKey)
        throws CommunicationException, InterruptedException {
        if (listener != null) {
            asynchronousListeners.put(messageKey, listener);
        }
        message.setMessageKey(messageKey);
        boolean success = false;
        try {
            Future<?> writeFuture = socketChannel.write(message);
            writeFuture.get(); //client writes are blocking to ensure proper failure handling
            success = true;
        } catch (ExecutionException e) {
             throw new SingleInstanceCommunicationException(JDBCPlugin.Event.TEIID20013, e, e.getMessage());
        } finally {
            if (!success) {
                asynchronousListeners.remove(messageKey);
            }
        }
    }

    /**
     * Send an exception to all clients that are currently waiting for a
     * response.
     */
    private void exceptionOccurred(Throwable e) {
        if (e instanceof CommunicationException) {
            if (e.getCause() instanceof InvalidClassException) {
                log.log(Level.SEVERE, "Unknown class or incorrect class version:", e); //$NON-NLS-1$
            } else {
                log.log(Level.FINE, "Unable to read: socket was already closed.", e); //$NON-NLS-1$
            }
        } else if (e instanceof EOFException) {
            log.log(Level.FINE, "Unable to read: socket was already closed.", e); //$NON-NLS-1$
        } else {
            log.log(Level.WARNING, "Unable to read: unexpected exception", e); //$NON-NLS-1$
        }

        if (!(e instanceof SingleInstanceCommunicationException)) {
            e = new SingleInstanceCommunicationException(e);
        }

        Set<Map.Entry<Serializable, ResultsReceiver<Object>>> entries = this.asynchronousListeners.entrySet();
        for (Iterator<Map.Entry<Serializable, ResultsReceiver<Object>>> iterator = entries.iterator(); iterator.hasNext();) {
            Map.Entry<Serializable, ResultsReceiver<Object>> entry = iterator.next();
            iterator.remove();
            entry.getValue().exceptionOccurred(e);
        }
    }

    private void receivedMessage(Object packet) {
        log.log(Level.FINE, "reading packet"); //$NON-NLS-1$
        if (packet instanceof Message) {
            Message messagePacket = (Message)packet;
            Serializable messageKey = messagePacket.getMessageKey();
            ExceptionHolder holder = null;
            if (messageKey instanceof ExceptionHolder) {
                holder = (ExceptionHolder)messageKey;
                messageKey = (Serializable) messagePacket.getContents();
                if (log.isLoggable(Level.FINE)) {
                    log.log(Level.FINE, "read asynch message:" + messageKey); //$NON-NLS-1$
                }
                if (messageKey == null) {
                    if (log.isLoggable(Level.FINE)) {
                        log.log(Level.FINE, "protocol error aborting all listeners"); //$NON-NLS-1$
                    }
                    for (ResultsReceiver<Object> listener : asynchronousListeners.values()) {
                        listener.exceptionOccurred(holder.getException());
                    }
                    asynchronousListeners.clear();
                    return;
                }
            }
            if (log.isLoggable(Level.FINE)) {
                log.log(Level.FINE, "read asynch message:" + messageKey); //$NON-NLS-1$
            }
            ResultsReceiver<Object> listener = asynchronousListeners.remove(messageKey);
            if (listener != null) {
                if (holder != null) {
                    listener.exceptionOccurred(holder.getException());
                } else {
                    listener.receiveResults(messagePacket.getContents());
                }
            }
        } else {
            //TODO: could ping back
            if (log.isLoggable(Level.FINE)) {
                log.log(Level.FINE, "packet ignored:" + packet); //$NON-NLS-1$
            }
        }
    }

    public void shutdown() {
        socketChannel.close();
    }

    /**
     * @return Returns the cryptor.
     */
    public Cryptor getCryptor() {
        return this.cryptor;
    }

    public void read(long timeout, TimeUnit unit, ResultsFuture<?> future) throws TimeoutException, InterruptedException {
        long timeoutMillis = (int)Math.min(unit.toMillis(timeout), Integer.MAX_VALUE);
        long start = System.currentTimeMillis();
        while (!future.isDone()) {
            boolean reading = false;
            synchronized (this) {
                if (!hasReader) {
                    hasReader = true;
                    reading = true;
                } else if (!future.isDone()) {
                    this.wait(Math.max(1, timeoutMillis));
                }
            }
            if (reading) {
                Object message = null;
                try {
                    if (!future.isDone()) {
                        message = socketChannel.read();
                    }
                } catch (SocketTimeoutException e) {
                } catch (Exception e) {
                    exceptionOccurred(e);
                } finally {
                    synchronized (this) {
                        hasReader = false;
                        this.notifyAll();
                    }
                }
                if (message != null) {
                    //process after reading to prevent recurrent reading with hasReader=true
                    receivedMessage(message);
                }
            }
            if (!future.isDone()) {
                long now = System.currentTimeMillis();
                timeoutMillis -= now - start;
                start = now;
                if (timeoutMillis <= 0) {
                    throw new TimeoutException("Read timeout after " + timeout + " milliseconds."); //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
        }
    }

    @Override
    public synchronized <T> T getService(Class<T> iface) {
        Object service = this.serviceMap.get(iface);
        if (service == null) {
            service = Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new RemoteInvocationHandler(iface, false) {
                @Override
                protected SocketServerInstanceImpl getInstance() {
                    return SocketServerInstanceImpl.this;
                }
            });
            this.serviceMap.put(iface, service);
        }
        return iface.cast(service);
    }

    public long getSynchTimeout() {
        return synchTimeout;
    }

    public static abstract class RemoteInvocationHandler implements InvocationHandler {

        private Class<?> targetClass;
        private boolean secureOptional;

        public RemoteInvocationHandler(Class<?> targetClass, boolean secureOptional) {
            this.targetClass = targetClass;
            this.secureOptional = secureOptional;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            Throwable t = null;
            try {
                final SocketServerInstance instance = getInstance();
                Message message = new Message();
                message.setContents(new ServiceInvocationStruct(args, method.getName(),
                        targetClass));
                Secure secure = method.getAnnotation(Secure.class);
                if (secure != null && (!secure.optional() || secureOptional)) {
                    message.setContents(instance.getCryptor().sealObject(message.getContents()));
                }
                ResultsFuture<Object> results = new ResultsFuture<Object>() {
                    @Override
                    protected Object convertResult() throws ExecutionException {
                        try {
                            Object result = instance.getCryptor().unsealObject(super.convertResult());
                            if (result instanceof ExceptionHolder) {
                                throw new ExecutionException(((ExceptionHolder)result).getException());
                            }
                            if (result instanceof Throwable) {
                                throw new ExecutionException((Throwable)result);
                            }
                            return result;
                        } catch (CryptoException e) {
                            throw new ExecutionException(e);
                        }
                    }

                    @Override
                    public Object get() throws InterruptedException, ExecutionException {
                        try {
                            return this.get(instance.getSynchTimeout(), TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            throw new ExecutionException(e);
                        }
                    }

                    /**
                     * get calls are overridden to provide a thread in which to perform
                     * the actual reads.
                     */
                    @Override
                    public Object get(long timeout, TimeUnit unit)
                            throws InterruptedException, ExecutionException,
                            TimeoutException {
                        instance.read(timeout, unit, this);
                        return super.get(timeout, unit);
                    }
                };
                final ResultsReceiver<Object> receiver = results.getResultsReceiver();

                instance.send(message, receiver, Integer.valueOf(MESSAGE_ID.getAndIncrement()));
                if (ResultsFuture.class.isAssignableFrom(method.getReturnType())) {
                    return results;
                }
                return results.get(instance.getSynchTimeout(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                t = e.getCause();
            } catch (TimeoutException e) {
                t = new SingleInstanceCommunicationException(e);
            } catch (Throwable e) {
                t = e;
            }
            throw ExceptionUtil.convertException(method, t);
        }

        protected abstract SocketServerInstance getInstance() throws CommunicationException;

    }
}