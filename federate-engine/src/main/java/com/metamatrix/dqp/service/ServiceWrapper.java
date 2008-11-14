/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.service;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;


/** 
 * This class is wrapper object on any ApplicationService, which overrides the life cycle
 * methods. In some cases we use the same application services such as "VDBService" and 
 * "DataService" in more than one connector. Since they are also installed in the Connector
 * as services and connector is a Application, it tries to manage the life cycle of 
 * these services, which has undesirable effect. we do not want that to happen as 
 * these are shared and not soley owned by the connector. See the invoke method on
 * how we do this logic.
 * @since 4.3
 */
public class ServiceWrapper implements ApplicationService, InvocationHandler {
    ApplicationService sourceSvc = null;
    Method[] availableMethods = null;
    
    public static ApplicationService create(Class[] interfaceTypes, ApplicationService srcSvc) {
       return (ApplicationService)Proxy.newProxyInstance(interfaceTypes[0].getClassLoader(),
                                             interfaceTypes,
                                             new ServiceWrapper(srcSvc));
    }
    
    private ServiceWrapper(ApplicationService svc) {
        this.sourceSvc =svc; 
        this.availableMethods = this.getClass().getDeclaredMethods();
    }
    

    /** 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     * @since 4.3
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
        // snub the call, usally when the service wrapper is used, that application which
        // using the service is not the owner
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     * @since 4.3
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        // snub the call, usally when the service wrapper is used, that application which
        // using the service is not the owner        
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#bind()
     * @since 4.3
     */
    public void bind() throws ApplicationLifecycleException {
        // snub the call, usally when the service wrapper is used, that application which
        // using the service is not the owner        
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     * @since 4.3
     */
    public void unbind() throws ApplicationLifecycleException {
        // snub the call, usally when the service wrapper is used, that application which
        // using the service is not the owner        
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     * @since 4.3
     */
    public void stop() throws ApplicationLifecycleException {
        // snub the call, usally when the service wrapper is used, that application which
        // using the service is not the owner        
    }

    /** 
     * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
     * @since 4.3
     */
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (overridden(method, args)) {
            return method.invoke(this, args);
        }
        return method.invoke(this.sourceSvc, args);
    }
    
    /**
     * Checks if a method needs to be overridden? any methods declared in this class needs to
     * be overridden, so that any of these methods are not actually called. Right now name
     * based checking is enough, parameter based is not necessasary until needed.  
     */
    private boolean overridden(Method check, Object[] args) { 
        for (int i = 0; i < availableMethods.length; i++) {
            if (availableMethods[i].getName().equals(check.getName())) {
                return true;
            }
        }
        return false;
    }

}
