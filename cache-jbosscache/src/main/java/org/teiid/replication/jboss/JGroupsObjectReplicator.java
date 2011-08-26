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

package org.teiid.replication.jboss;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelFactory;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Promise;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.teiid.Replicated;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.ReplicatedObject;

public class JGroupsObjectReplicator implements ObjectReplicator, Serializable {
	
	private static final long serialVersionUID = -6851804958313095166L;
	private static final String CREATE_STATE = "createState"; //$NON-NLS-1$
	private static final String BUILD_STATE = "buildState"; //$NON-NLS-1$
	private static final String FINISH_STATE = "finishState"; //$NON-NLS-1$

	private final class StreamingRunner implements Runnable {
		private final Object object;
		private final String stateId;
		private final JGroupsInputStream is;

		private StreamingRunner(Object object, String stateId, JGroupsInputStream is) {
			this.object = object;
			this.stateId = stateId;
			this.is = is;
		}

		@Override
		public void run() {
			try {
				((ReplicatedObject)object).setState(stateId, is);
				LogManager.logDetail(LogConstants.CTX_RUNTIME, "state set " + stateId); //$NON-NLS-1$
			} catch (Exception e) {
				LogManager.logError(LogConstants.CTX_RUNTIME, e, "error setting state " + stateId); //$NON-NLS-1$
			} finally {
				is.close();
			}
		}
	}

	private final static class ReplicatedInvocationHandler<S> extends ExtendedReceiverAdapter implements
			InvocationHandler, Serializable {
		
		private static final long serialVersionUID = -2943462899945966103L;
		private final S object;
		private RpcDispatcher disp;
		private final HashMap<Method, Short> methodMap;
	    protected Vector<Address> remoteMembers = new Vector<Address>();
	    protected final transient Promise<Boolean> state_promise=new Promise<Boolean>();
	    
		private ReplicatedInvocationHandler(S object,
				HashMap<Method, Short> methodMap) {
			this.object = object;
			this.methodMap = methodMap;
		}
		
		public void setDisp(RpcDispatcher disp) {
			this.disp = disp;
		}
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Short methodNum = methodMap.get(method);
			if (methodNum == null || remoteMembers.isEmpty()) {
				if (methodNum != null) {
			    	Replicated annotation = method.getAnnotation(Replicated.class);
			    	if (annotation != null && annotation.remoteOnly()) {
			    		return null;
			    	}
				}
				try {
					return method.invoke(object, args);
				} catch (InvocationTargetException e) {
					throw e.getCause();
				}
			}
		    try {
		    	Replicated annotation = method.getAnnotation(Replicated.class);
		    	if (annotation.replicateState()) {
		    		Object result = null;
		    		try {
						result = method.invoke(object, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
					Vector<Address> dests = null;
					synchronized (remoteMembers) {
						dests = new Vector<Address>(remoteMembers);
					}
					ReplicatedObject ro = (ReplicatedObject)object;
					String stateId = (String)args[0];
					LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "replicating state", stateId); //$NON-NLS-1$
					JGroupsOutputStream oStream = new JGroupsOutputStream(disp, dests, stateId, (short)(methodMap.size() - 3));
					try {
						ro.getState(stateId, oStream);
					} finally {
						oStream.close();
					}
					LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "sent state", stateId); //$NON-NLS-1$
			        return result;
				}
		        MethodCall call=new MethodCall(methodNum, args);
		        Vector<Address> dests = null;
		        if (annotation.remoteOnly()) {
					synchronized (remoteMembers) {
						dests = new Vector<Address>(remoteMembers);
					}
		        }
		        RspList responses = disp.callRemoteMethods(dests, call, annotation.asynch()?GroupRequest.GET_NONE:GroupRequest.GET_ALL, annotation.timeout());
		        if (annotation.asynch()) {
			        return null;
		        }
		        Vector<Object> results = responses.getResults();
		        if (method.getReturnType() == boolean.class) {
		        	for (Object o : results) {
						if (!Boolean.TRUE.equals(o)) {
							return false;
						}
					}
		        	return true;
		        } else if (method.getReturnType() == Collection.class) {
		        	ArrayList<Object> result = new ArrayList<Object>();
		        	for (Object o : results) {
		        		result.addAll((Collection)o);
					}
		        	return results;
		        }
	        	return null;
		    } catch(Exception e) {
		        throw new RuntimeException(method + " " + args + " failed"); //$NON-NLS-1$ //$NON-NLS-2$
		    }
		}

		@Override
		public void viewAccepted(View newView) {
			if (newView.getMembers() != null) {
				synchronized (remoteMembers) {
					remoteMembers.removeAll(newView.getMembers());
					if (object instanceof ReplicatedObject && !remoteMembers.isEmpty()) {
						((ReplicatedObject)object).droppedMembers(new HashSet<Serializable>(remoteMembers));
					}
					remoteMembers.clear();
					remoteMembers.addAll(newView.getMembers());
					remoteMembers.remove(this.disp.getChannel().getLocalAddress());
				}
			}
		}
		
		@Override
		public void setState(InputStream istream) {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "loading initial state"); //$NON-NLS-1$
			try {
				((ReplicatedObject)object).setState(istream);
				state_promise.setResult(Boolean.TRUE);
			} catch (Exception e) {
				state_promise.setResult(Boolean.FALSE);
				LogManager.logError(LogConstants.CTX_RUNTIME, e, "error loading initial state"); //$NON-NLS-1$
			} finally {
				Util.close(istream);
			}
		}
		
		@Override
		public void getState(OutputStream ostream) {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "getting initial state"); //$NON-NLS-1$
			try {
				((ReplicatedObject)object).getState(ostream);
			} catch (Exception e) {
				LogManager.logError(LogConstants.CTX_RUNTIME, e, "error gettting initial state"); //$NON-NLS-1$
			} finally {
				Util.close(ostream);
			}
		}
	}
	
	private interface Streaming {
		void createState(String id);
		void buildState(String id, byte[] bytes);
		void finishState(String id);
	}

	private transient ChannelFactory channelFactory;
	private String multiplexerStack;
	private String clusterName;
	private String jndiName;
	//TODO: this should be configurable, or use a common executor
	private transient Executor executor = Executors.newCachedThreadPool();

	public ChannelFactory getChannelFactory() {
		return channelFactory;
	}
	
	public void setJndiName(String jndiName) {
		this.jndiName = jndiName;
	}
	
	public String getJndiName() {
		return jndiName;
	}
	
	public String getMultiplexerStack() {
		return multiplexerStack;
	}
	
	public String getClusterName() {
		return clusterName;
	}
	
	public void setChannelFactory(ChannelFactory channelFactory) {
		this.channelFactory = channelFactory;
	}
	
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	public void setMultiplexerStack(String multiplexerStack) {
		this.multiplexerStack = multiplexerStack;
	}
	
	public void start() throws Exception {
		if (this.channelFactory == null) {
			return; //no need to distribute events
		}
    	if (jndiName != null) {
	    	final InitialContext ic = new InitialContext();
    		org.jboss.util.naming.Util.bind(ic, jndiName, this);
    	}
	}

	public void stop() {
    	if (jndiName != null) {
	    	final InitialContext ic ;
	    	try {
	    		ic = new InitialContext() ;
	    		org.jboss.util.naming.Util.unbind(ic, jndiName) ;
	    	} catch (final NamingException ne) {
	    	}
    	}
	}
	
	public void stop(Object object) {
		ReplicatedInvocationHandler<?> handler = (ReplicatedInvocationHandler<?>) Proxy.getInvocationHandler(object);
		Channel c = handler.disp.getChannel();
		handler.disp.stop();
		c.close();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T, S> T replicate(String mux_id,
			Class<T> iface, final S object, long startTimeout) throws Exception {
		Channel channel = this.channelFactory.createMultiplexerChannel(this.multiplexerStack, mux_id);
		Method[] methods = iface.getMethods();
		
		final HashMap<Method, Short> methodMap = new HashMap<Method, Short>();
		final ArrayList<Method> methodList = new ArrayList<Method>();
		
		for (Method method : methods) {
			if (method.getAnnotation(Replicated.class) == null) {
				continue;
			}
			methodList.add(method);
			methodMap.put(method, (short)(methodList.size() - 1));
		}
		
		//add in streaming methods
		Method createState = JGroupsObjectReplicator.Streaming.class.getMethod(CREATE_STATE, new Class<?>[] {String.class});
		methodList.add(createState);
		methodMap.put(createState, (short)(methodList.size() - 1));
		Method buildState = JGroupsObjectReplicator.Streaming.class.getMethod(BUILD_STATE, new Class<?>[] {String.class, byte[].class});
		methodList.add(buildState);
		methodMap.put(buildState, (short)(methodList.size() - 1));
		Method finishState = JGroupsObjectReplicator.Streaming.class.getMethod(FINISH_STATE, new Class<?>[] {String.class});
		methodList.add(finishState);
		methodMap.put(finishState, (short)(methodList.size() - 1));
		
        ReplicatedInvocationHandler<S> proxy = new ReplicatedInvocationHandler<S>(object, methodMap);
        /*
         * TODO: could have an object implement streaming
         * Override the normal handle method to support streaming
         */
		RpcDispatcher disp = new RpcDispatcher(channel, proxy, proxy, object) {
			Map<List<?>, JGroupsInputStream> inputStreams = new ConcurrentHashMap<List<?>, JGroupsInputStream>();
			@Override
			public Object handle(Message req) {
				Object      body=null;

		        if(req == null || req.getLength() == 0) {
		            if(log.isErrorEnabled()) log.error("message or message buffer is null"); //$NON-NLS-1$
		            return null;
		        }

		        try {
		            body=req_marshaller != null?
		                    req_marshaller.objectFromByteBuffer(req.getBuffer(), req.getOffset(), req.getLength())
		                    : req.getObject();
		        }
		        catch(Throwable e) {
		            if(log.isErrorEnabled()) log.error("exception marshalling object", e); //$NON-NLS-1$
		            return e;
		        }

		        if(!(body instanceof MethodCall)) {
		            if(log.isErrorEnabled()) log.error("message does not contain a MethodCall object"); //$NON-NLS-1$

		            // create an exception to represent this and return it
		            return  new IllegalArgumentException("message does not contain a MethodCall object") ; //$NON-NLS-1$
		        }

		        final MethodCall method_call=(MethodCall)body;

		        try {
		            if(log.isTraceEnabled())
		                log.trace("[sender=" + req.getSrc() + "], method_call: " + method_call); //$NON-NLS-1$ //$NON-NLS-2$

	                if(method_lookup == null)
	                    throw new Exception("MethodCall uses ID=" + method_call.getId() + ", but method_lookup has not been set"); //$NON-NLS-1$ //$NON-NLS-2$

		            if (method_call.getId() >= methodList.size() - 3) {
		            	Serializable address = req.getSrc();
		            	String stateId = (String)method_call.getArgs()[0];
		            	List<?> key = Arrays.asList(stateId, address);
		            	JGroupsInputStream is = inputStreams.get(key);
		            	if (method_call.getId() == methodList.size() - 3) {
		            		LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "create state", stateId); //$NON-NLS-1$
		            		if (is != null) {
		            			is.receive(null);
		            		}
		            		is = new JGroupsInputStream();
		            		this.inputStreams.put(key, is);
		            		executor.execute(new StreamingRunner(object, stateId, is));
		            	} else if (method_call.getId() == methodList.size() - 2) {
		            		LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "building state", stateId); //$NON-NLS-1$
		            		if (is != null) {
		            			is.receive((byte[])method_call.getArgs()[1]);
		            		}
		            	} else if (method_call.getId() == methodList.size() - 1) {
		            		LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "finished state", stateId); //$NON-NLS-1$
		            		if (is != null) {
		            			is.receive(null);
		            		}
		            		this.inputStreams.remove(key);
		            	}  
		            	return null;
		            }
		            
	                Method m=method_lookup.findMethod(method_call.getId());
	                if(m == null)
	                    throw new Exception("no method found for " + method_call.getId()); //$NON-NLS-1$
	                method_call.setMethod(m);
		            
	            	return method_call.invoke(server_obj);
		        }
		        catch(Throwable x) {
		            return x;
		        }
			}
		};
		
		proxy.setDisp(disp);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return methodList.get(id);
            }
        });
        
		T replicatedProxy = (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, proxy);

		channel.connect(clusterName);
		if (object instanceof ReplicatedObject) {
			((ReplicatedObject)object).setLocalAddress(channel.getLocalAddress());
			boolean getState = channel.getState(null, startTimeout);
			if (getState) {
				boolean loaded = proxy.state_promise.getResult(startTimeout);
				if (loaded) {
					LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "loaded"); //$NON-NLS-1$
				} else {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, object + " load timeout"); //$NON-NLS-1$
				}
			} else {
				LogManager.logInfo(LogConstants.CTX_RUNTIME, object + " first member or timeout exceeded"); //$NON-NLS-1$
			}
		}

		return replicatedProxy;
	}
	
}
