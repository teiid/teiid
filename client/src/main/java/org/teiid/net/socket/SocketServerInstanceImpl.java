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

package org.teiid.net.socket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
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
	
	static final int HANDSHAKE_RETRIES = 10;
    private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

	private static AtomicInteger MESSAGE_ID = new AtomicInteger();
    private Map<Serializable, ResultsReceiver<Object>> asynchronousListeners = new ConcurrentHashMap<Serializable, ResultsReceiver<Object>>();

    private long synchTimeout;
    private HostInfo info;

    private ObjectChannel socketChannel;
    private Cryptor cryptor;
    private String serverVersion;
    private AuthenticationType authType = AuthenticationType.CLEARTEXT;
    private HashMap<Class<?>, Object> serviceMap = new HashMap<Class<?>, Object>();
    
    private boolean hasReader;
    
    public SocketServerInstanceImpl(HostInfo info, long synchTimeout) {
    	if (!info.isResolved()) {
    		throw new AssertionError("Expected HostInfo to be resolved"); //$NON-NLS-1$
    	}
        this.info = info;
        this.synchTimeout = synchTimeout;
    }
    
    public synchronized void connect(ObjectChannelFactory channelFactory) throws CommunicationException, IOException {
        this.socketChannel = channelFactory.createObjectChannel(new InetSocketAddress(info.getInetAddress(), info.getPortNumber()), info.isSsl());
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
    
    private void doHandshake() throws IOException, CommunicationException {
    	Handshake handshake = null;
    	for (int i = 0; i < HANDSHAKE_RETRIES; i++) {
	        try {
				Object obj = this.socketChannel.read();
				
				if (!(obj instanceof Handshake)) {
					throw new CommunicationException(JDBCPlugin.Util.getString("SocketServerInstanceImpl.handshake_error"));  //$NON-NLS-1$
				}
				handshake = (Handshake)obj;
				break;
			} catch (ClassNotFoundException e1) {
				throw new CommunicationException(e1);
			} catch (SocketTimeoutException e) {
				if (i == HANDSHAKE_RETRIES - 1) {
					throw e;
				}
			}
    	}

        try {
            /*if (!getVersionInfo().equals(handshake.getVersion())) {
                throw new CommunicationException(NetPlugin.Util.getString("SocketServerInstanceImpl.version_mismatch", getVersionInfo(), handshake.getVersion())); //$NON-NLS-1$
            }*/
            serverVersion = handshake.getVersion();
            authType = handshake.getAuthType();
            handshake.setVersion();
            
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
        } catch (CryptoException err) {
        	throw new CommunicationException(err);
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
            log.log(Level.FINE, "read asynch message:" + messageKey); //$NON-NLS-1$ 
            ResultsReceiver<Object> listener = asynchronousListeners.remove(messageKey);
            if (listener != null) {
                listener.receiveResults(messagePacket.getContents());
            }
        } else {
        	//TODO: could ping back
        	log.log(Level.FINE, "packet ignored:" + packet); //$NON-NLS-1$ 
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
				try {
					if (!future.isDone()) {
						receivedMessage(socketChannel.read());
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

	@Override
	public AuthenticationType getAuthenticationType() {
		return authType;
	}

}