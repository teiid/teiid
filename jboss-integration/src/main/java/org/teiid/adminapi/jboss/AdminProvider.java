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
package org.teiid.adminapi.jboss;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.profileservice.spi.ProfileService;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.deployers.VDBStatusChecker;

public class AdminProvider {
	
	public static Admin getLocal(final ProfileService profileService, VDBStatusChecker vdbStatusChecker) {
		ProfileConnection pc = new ProfileConnection(profileService);
		return new Admin(pc.getManagementView(), pc.getDeploymentManager(), vdbStatusChecker);
	}

	public static Admin getLocal(VDBStatusChecker vdbStatusChecker) throws AdminComponentException {
		ProfileConnection pc = new ProfileConnection();
		return new Admin(pc.getManagementView(), pc.getDeploymentManager(), vdbStatusChecker);
	}
	
	public static Admin getRemote(String provideURL, String userid, String password, VDBStatusChecker vdbStatusChecker) throws AdminComponentException {
		ProfileConnection pc = new ProfileConnection(provideURL, userid, password);
		return new Admin(pc.getManagementView(), pc.getDeploymentManager(), vdbStatusChecker);		
	}
	
	/**
	 * Connection to profile service from a remote VM or local connection
	 */
	static private class ProfileConnection {
		private static final String PROFILE_SERVICE_JNDI_NAME = "ProfileService"; //$NON-NLS-1$
		private static final String SECURE_PROFILE_SERVICE_JNDI_NAME = "SecureProfileService/remote"; //$NON-NLS-1$
	    private static final String NAMING_CONTEXT_FACTORY = "org.jnp.interfaces.NamingContextFactory"; //$NON-NLS-1$
	    private static final String JNP_TIMEOUT_JNP_INIT_PROP = "jnp.timeout"; //$NON-NLS-1$
	    private static final String JNP_SOTIMEOUT_JNP_INIT_PROP = "jnp.sotimeout"; //$NON-NLS-1$
	    private static final String JNP_DISABLE_DISCOVERY_JNP_INIT_PROP = "jnp.disableDiscovery"; //$NON-NLS-1$
	    
	    /**
	     * This is the timeout (in milliseconds) for the initial attempt to establish the remote connection.
	     */
	    private static final int JNP_TIMEOUT = 60 * 1000; // 60 seconds

	    /**
	     * This is the timeout (in milliseconds) for methods invoked on the remote ProfileService. NOTE: This timeout comes
	     * into play if the JBossAS instance has gone down since the original JNP connection was made.
	     */
	    private static final int JNP_SO_TIMEOUT = 60 * 1000; // 60 seconds

	    /**
	     * A flag indicating that the discovery process should not attempt to automatically discover (via multicast) naming
	     * servers running the JBoss HA-JNDI service if it fails to connect to the specified jnp URL.
	     */
	    private static final boolean JNP_DISABLE_DISCOVERY = true;
	    
	    private ProfileService profileService;
	    private String userName;
	    private String password;
	    
	    public ProfileConnection() throws AdminComponentException {
	    	this.profileService = connect(null, null, null);
	    }	
	    
	    public ProfileConnection(final ProfileService profileService) {
	    	this.profileService = profileService;
	    }	    	        
	    
	    public ProfileConnection(String providerURL, String user, String password) throws AdminComponentException {
	    	this.userName = user;
	    	this.password = password;
	    	this.profileService = connect(providerURL, user, password);
	    }
	    
	    public ManagementView getManagementView() {
    		return this.profileService.getViewManager();
	    }
	    
	    public DeploymentManager getDeploymentManager() {
    		return this.profileService.getDeploymentManager();
	    }
	    
		private ProfileService connect(String providerURL, String user, String password) throws AdminComponentException {
	        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
	        try {
	        	// local connection
	        	if (providerURL == null) {
	                InitialContext ic  = new InitialContext();
                	return (ProfileService)ic.lookup(PROFILE_SERVICE_JNDI_NAME);
	        	}
	        	
	        	Properties env = new Properties();
	        	env.setProperty(Context.PROVIDER_URL, providerURL);
	            
	            // un-authenticated remote login
                env.setProperty(Context.INITIAL_CONTEXT_FACTORY, NAMING_CONTEXT_FACTORY);
                env.setProperty(Context.SECURITY_PRINCIPAL, user);
                env.setProperty(Context.SECURITY_CREDENTIALS, password);         
                env.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces"); //$NON-NLS-1$
                env.setProperty(JNP_DISABLE_DISCOVERY_JNP_INIT_PROP, "true"); //$NON-NLS-1$
                env.setProperty(JNP_TIMEOUT_JNP_INIT_PROP, String.valueOf(JNP_TIMEOUT));
                env.setProperty(JNP_SOTIMEOUT_JNP_INIT_PROP, String.valueOf(JNP_SO_TIMEOUT));
                env.setProperty(JNP_DISABLE_DISCOVERY_JNP_INIT_PROP, String.valueOf(JNP_DISABLE_DISCOVERY));
                env.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces"); //$NON-NLS-1$ //$NON-NLS-2$
                InitialContext ic  = new InitialContext(env);
                
                try {
	                return (ProfileService)ic.lookup(PROFILE_SERVICE_JNDI_NAME);
                } catch(NamingException e) {
                	ProfileService ps =  (ProfileService)ic.lookup(SECURE_PROFILE_SERVICE_JNDI_NAME);
                	return (ProfileService)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {ProfileService.class}, new JaasSecurityHandler(ps, this.userName, this.password));
                }
	        } catch(NamingException e) {
	        	throw new AdminComponentException(e);
	        } finally {
	        	Thread.currentThread().setContextClassLoader(originalContextClassLoader);
	        }
		}
	}
	
	static class JaasSecurityHandler implements InvocationHandler {    
	    private Object target;
	    private LoginContext loginContext;

	    public JaasSecurityHandler(Object target, final String username, final String password) {
	        this.target = target;                
	        Configuration jaasConfig = new JBossConfiguration();
	        try {
	            this.loginContext = new LoginContext(JBossConfiguration.JBOSS_ENTRY_NAME, null, new CallbackHandler() {
					
					@Override
					public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
				        for (Callback callback : callbacks) {
				            if (callback instanceof NameCallback) {
				                NameCallback nameCallback = (NameCallback)callback;
				                nameCallback.setName(username);
				            }
				            else if (callback instanceof PasswordCallback) {
				                PasswordCallback passwordCallback = (PasswordCallback)callback;
				                passwordCallback.setPassword(password.toCharArray());
				            }
				            else {
				                throw new UnsupportedCallbackException(callback, "Unrecognized Callback: " + callback); //$NON-NLS-1$
				            }
				        }						
						
					}
				}, jaasConfig);
	        }
	        catch (LoginException e) {
	            throw new RuntimeException(e);
	        }
	    }

	    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
	        this.loginContext.login();
	        Object returnValue = method.invoke(this.target, args);
	        this.loginContext.logout();
	        return returnValue;
	    }
	}	
}
