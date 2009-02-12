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

package com.metamatrix.common.comm.platform.socket.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLEngine;

import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.api.MessageListener;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ExceptionHolder;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListener;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListenerFactory;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.DhKeyGenerator;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.ResultsFuture;

/**
 * Client view of a socket server connection that exposes remote services
 * On construction this class will create a channel and exchange a handshake.
 * That handshake will establish a {@link Cryptor} to be used for secure traffic.
 */
public class SocketServerInstanceImpl implements ChannelListener, SocketServerInstance {
	
	private AtomicInteger MESSAGE_ID = new AtomicInteger();

	private HostInfo hostInfo;
	private SSLEngine engine;
    private ObjectChannel socketChannel;
    private SocketLog log;
    private long synchTimeout;

    private Cryptor cryptor;
    
    private Map<Serializable, MessageListener> asynchronousListeners = new ConcurrentHashMap<Serializable, MessageListener>();
    
    private boolean handshakeCompleted;
    private CommunicationException handshakeError;
    
    public SocketServerInstanceImpl() {
    	
    }

    public SocketServerInstanceImpl(final HostInfo host, SSLEngine engine, SocketLog log, long synchTimeout) {
        this.hostInfo = host;
        this.log = log;
        this.engine = engine;
        this.synchTimeout = synchTimeout;
    }
    
    public void connect(ObjectChannelFactory channelFactory, long handShakeTimeout) throws CommunicationException, IOException {
        InetSocketAddress address = null;
        if (hostInfo.getInetAddress() != null) {
            address = new InetSocketAddress(hostInfo.getInetAddress(), hostInfo.getPortNumber());
        } else {
            address = new InetSocketAddress(hostInfo.getHostName(), hostInfo.getPortNumber());
        }
		channelFactory.createObjectChannel(address, engine, new ChannelListenerFactory() {

			public ChannelListener createChannelListener(
					ObjectChannel channel) {
				synchronized (SocketServerInstanceImpl.this) {
					if (SocketServerInstanceImpl.this.handshakeError != null) {
						channel.close();
					}
					SocketServerInstanceImpl.this.socketChannel = channel;
				}
				return SocketServerInstanceImpl.this;
			}
			
		});
		synchronized (this) {
			long endTime = System.currentTimeMillis() + handShakeTimeout;
			while (!this.handshakeCompleted && this.handshakeError == null) {
				long remainingTimeout = endTime - System.currentTimeMillis();
				if (remainingTimeout <= 0) {
					break;
				}
				try {
					this.wait(remainingTimeout);
				} catch (InterruptedException e) {
					break;
				}
			}
			if (!this.handshakeCompleted || this.handshakeError != null) {
				if (this.socketChannel != null) {
					this.socketChannel.close();
				}
				if (this.handshakeError == null) {
					this.handshakeError = new SingleInstanceCommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstanceImpl.handshake_timeout")); //$NON-NLS-1$
				}	
				throw this.handshakeError;
			}
		}
    }
    
    /**
     * Return identifier of the server VM this ServerInstance is associated with. 
     */
    public HostInfo getHostInfo() {
        return this.hostInfo;
    }
    
    static String getVersionInfo() {
        return MetaMatrixProductVersion.VERSION_NUMBER;
    }
    
    private synchronized void receivedHahdshake(Handshake handshake) {
        try {
            if (!getVersionInfo().equals(handshake.getVersion())) {
                this.handshakeError = new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstanceImpl.version_mismatch", getVersionInfo(), handshake.getVersion())); //$NON-NLS-1$
                return;
            }
            
            handshake.setVersion(getVersionInfo());
            
            byte[] serverPublicKey = handshake.getPublicKey();
            
            if (serverPublicKey != null) {
            	DhKeyGenerator keyGen = new DhKeyGenerator();
            	byte[] publicKey = keyGen.createPublicKey();
                this.cryptor = keyGen.getSymmetricCryptor(serverPublicKey);
                handshake.setPublicKey(publicKey);
            } else {
                this.cryptor = new NullCryptor();
            }
            
            this.socketChannel.write(handshake);
            this.handshakeCompleted = true;
        } catch (CryptoException err) {
        	this.handshakeError = new CommunicationException(err);
        } finally {
			this.notify();
        }
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    public void send(Message message, MessageListener listener, Serializable messageKey)
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
        	throw new SingleInstanceCommunicationException(e);
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
	public void exceptionOccurred(Throwable e) {
    	if (e instanceof CommunicationException) {
	        if (e.getCause() instanceof InvalidClassException) {
	            log.logError("SocketServerInstance.read", e, "Unknown class or incorrect class version:"); //$NON-NLS-1$ //$NON-NLS-2$
	        } else {
	            log.logDetail("SocketServerInstance.read", e, "Unable to read: socket was already closed."); //$NON-NLS-1$ //$NON-NLS-2$
	        }
    	} else if (e instanceof EOFException) {
    		log.logDetail("SocketServerInstance.read", e, "Unable to read: socket was already closed."); //$NON-NLS-1$ //$NON-NLS-2$
    	} else {
    		log.logDetail("SocketServerInstance.read", e, "Unable to read: unexpected exception"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
		synchronized (this) {
			if (!handshakeCompleted) {
				this.handshakeError = new SingleInstanceCommunicationException(e, CommPlatformPlugin.Util.getString(engine!=null?"SocketServerInstanceImpl.secure_error_during_handshake":"SocketServerInstanceImpl.error_during_handshake")); //$NON-NLS-1$ //$NON-NLS-2$  
				this.notify();
			}
		}
		
        Message messageHolder = new Message();
        messageHolder.setContents(e instanceof SingleInstanceCommunicationException?e:new SingleInstanceCommunicationException(e));

        Set<Map.Entry<Serializable, MessageListener>> entries = this.asynchronousListeners.entrySet();
        for (Iterator<Map.Entry<Serializable, MessageListener>> iterator = entries.iterator(); iterator.hasNext();) {
			Map.Entry<Serializable, MessageListener> entry = iterator.next();
			iterator.remove();
			entry.getValue().deliverMessage(messageHolder, entry.getKey());
		}
    }

	public void receivedMessage(Object packet) {
        log.logDetail("SocketServerInstance.read", "reading"); //$NON-NLS-1$ //$NON-NLS-2$
        if (log.isLogged("SocketServerInstance.read", SocketLog.DETAIL)) { //$NON-NLS-1$
            log.logDetail("SocketServerInstance.read", "read:" + packet); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (packet instanceof Message) {
        	Message messagePacket = (Message)packet;
            processAsynchronousPacket(messagePacket);
        } else if (packet instanceof Handshake) {
        	receivedHahdshake((Handshake)packet);
        } else {
            if (log.isLogged("SocketServerInstance.read", SocketLog.DETAIL)) { //$NON-NLS-1$
                log.logDetail("SocketServerInstance.read", "packet ignored:" + packet); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
    }

    private void processAsynchronousPacket(Message message) {
        Serializable messageKey = message.getMessageKey();
        if (log.isLogged("SocketServerInstance.read", SocketLog.DETAIL)) { //$NON-NLS-1$
            log.logDetail("SocketServerInstance.read", "read asynch message:" + message); //$NON-NLS-1$ //$NON-NLS-2$
        }
    	MessageListener listener = asynchronousListeners.remove(messageKey);
        if (listener != null) {
            listener.deliverMessage(message, messageKey);
        }
        log.logDetail("SocketServerInstanceImpl.deliverMessage", "message delivered"); //$NON-NLS-1$ //$NON-NLS-2$
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

	public void onConnection() {
		
	}

	@Override
	public <T> T getService(Class<T> iface) {
		return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, new RemoteInvocationHandler(iface));
	}
	
	public class RemoteInvocationHandler implements InvocationHandler {

		private boolean secure;
		private Class<?> targetClass;
		
		public RemoteInvocationHandler(Class<?> targetClass) {
			this.targetClass = targetClass;
			this.secure = !ClientSideDQP.class.isAssignableFrom(targetClass);
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Throwable t = null;
			try {
				Message message = new Message();
				message.setContents(new ServiceInvocationStruct(args, method.getName(),
						targetClass));
				if (secure) {
					message.setContents(getCryptor().sealObject(message.getContents()));
				}
				ResultsFuture results = new ResultsFuture() {
					@Override
					protected Object convertResult() throws ExecutionException {
						try {
							Object result = getCryptor().unsealObject((Serializable) super.convertResult());
							if (result instanceof ExceptionHolder) {
								throw new ExecutionException(((ExceptionHolder)result).convertException());
							}
							if (result instanceof Throwable) {
								throw new ExecutionException((Throwable)result);
							}
							return result;
						} catch (CryptoException e) {
							throw new ExecutionException(e);
						}
					}
				};
				final ResultsReceiver receiver = results.getResultsReceiver();
	
				send(message, new MessageListener() {
	
					public void deliverMessage(Message responseMessage,
							Serializable messageKey) {
						Serializable result = responseMessage.getContents();
						receiver.receiveResults(result);
					}
	
				}, Integer.valueOf(MESSAGE_ID.getAndIncrement()));
				if (ResultsFuture.class.isAssignableFrom(method.getReturnType())) {
					return results;
				}
				return results.get(synchTimeout, TimeUnit.MILLISECONDS);
			} catch (ExecutionException e) {
				t = e.getCause();
			} catch (Throwable e) {
				t = e;
			}
			throw ExceptionUtil.convertException(method, t);
		}

	}

}