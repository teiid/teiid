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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.AccessibleBufferedInputStream;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.HostInfo;
import org.teiid.net.socket.SocketUtil.SSLSocketFactory;
import org.teiid.netty.handler.codec.serialization.ObjectDecoderInputStream;
import org.teiid.netty.handler.codec.serialization.ObjectEncoderOutputStream;


public final class OioOjbectChannelFactory implements ObjectChannelFactory {

    public static ThreadLocal<Long> TIMEOUTS = new ThreadLocal<Long>();

    private final static int STREAM_BUFFER_SIZE = 1<<15;
    private final static int DEFAULT_MAX_OBJECT_SIZE = 1 << 25;

    private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

    final static class OioObjectChannel implements ObjectChannel {
        private final Socket socket;
        private ObjectOutputStream outputStream;
        private ObjectInputStream inputStream;

        private OioObjectChannel(Socket socket, int maxObjectSize) throws IOException {
            log.fine("creating new OioObjectChannel"); //$NON-NLS-1$
            this.socket = socket;
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            outputStream = new ObjectEncoderOutputStream(out, STREAM_BUFFER_SIZE);
            final ClassLoader cl = this.getClass().getClassLoader();
            inputStream = new ObjectDecoderInputStream(new AccessibleBufferedInputStream(socket.getInputStream(), STREAM_BUFFER_SIZE), cl, maxObjectSize);
        }

        @Override
        public void close() {
            log.finer("closing socket"); //$NON-NLS-1$
            try {
                outputStream.flush();
            } catch (IOException e) {
                // ignore
            }
            try {
                outputStream.close();
            } catch (IOException e) {
                // ignore
            }
            try {
                inputStream.close();
            } catch (IOException e) {
                // ignore
            }
            try {
                socket.close();
            } catch (IOException e) {
                // ignore
            }
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return socket.getRemoteSocketAddress();
        }

        @Override
        public InetAddress getLocalAddress() {
            return socket.getLocalAddress();
        }

        @Override
        public boolean isOpen() {
            return !socket.isClosed();
        }

        @Override
        public Object read() throws IOException, ClassNotFoundException {
            log.finer("reading message from socket"); //$NON-NLS-1$
            try {
                return inputStream.readObject();
            } catch (SocketTimeoutException e) {
                Long timeout = TIMEOUTS.get();
                if (timeout != null && timeout < System.currentTimeMillis()) {
                    TIMEOUTS.set(null);
                    throw new InterruptedIOException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20035));
                }
                throw e;
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        @Override
        public synchronized Future<?> write(Object msg) {
            log.finer("writing message to socket"); //$NON-NLS-1$
            ResultsFuture<Void> result = new ResultsFuture<Void>();
            try {
                outputStream.writeObject(msg);
                outputStream.flush();
                outputStream.reset();
                result.getResultsReceiver().receiveResults(null);
            } catch (IOException e) {
                close();
                result.getResultsReceiver().exceptionOccurred(e);
            }
            return result;
        }
    }

    private Properties props;
    private int receiveBufferSize = 0;
    private int sendBufferSize = 0;
    private boolean conserveBandwidth;
    private int soTimeout = 1000;
    private volatile SSLSocketFactory sslSocketFactory;
    private int maxObjectSize = DEFAULT_MAX_OBJECT_SIZE;

    public OioOjbectChannelFactory(Properties props) {
        this.props = props;
        PropertiesUtils.setBeanProperties(this, props, "org.teiid.sockets", true); //$NON-NLS-1$
    }

    @Override
    public ObjectChannel createObjectChannel(HostInfo info) throws CommunicationException, IOException {
        final Socket socket;
        if (info.isSsl()) {
            if (this.sslSocketFactory == null) {
                try {
                    sslSocketFactory = SocketUtil.getSSLSocketFactory(props);
                } catch (GeneralSecurityException e) {
                     throw new CommunicationException(JDBCPlugin.Event.TEIID20027, e, e.getMessage());
                }
            }
            socket = sslSocketFactory.getSocket(info.getHostName(), info.getPortNumber());
        } else {
            socket = new Socket(info.getInetAddress(), info.getPortNumber());
        }
        if (receiveBufferSize > 0) {
            socket.setReceiveBufferSize(receiveBufferSize);
        }
        if (sendBufferSize > 0) {
            socket.setSendBufferSize(sendBufferSize);
        }
        socket.setTcpNoDelay(!conserveBandwidth); // enable Nagle's algorithm to conserve bandwidth
        socket.setSoTimeout(soTimeout);
        return new OioObjectChannel(socket, maxObjectSize);
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public boolean isConserveBandwidth() {
        return conserveBandwidth;
    }

    public void setConserveBandwidth(boolean conserveBandwidth) {
        this.conserveBandwidth = conserveBandwidth;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public void setMaxObjectSize(int maxObjectSize) {
        this.maxObjectSize = maxObjectSize;
    }

    public int getSoTimeout() {
        return soTimeout;
    }
}