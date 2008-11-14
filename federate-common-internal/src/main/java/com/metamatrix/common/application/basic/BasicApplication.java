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

package com.metamatrix.common.application.basic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.application.Application;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;

/**
 */
public abstract class BasicApplication implements Application {

    protected ApplicationEnvironment environment;
    private ArrayList installedServices = new ArrayList();

    /* 
     * @see com.metamatrix.common.application.Application#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
        BasicEnvironment basicEnv = new BasicEnvironment();
        basicEnv.setApplicationProperties(props);
        this.environment = basicEnv;

    }

    /* 
     * @see com.metamatrix.common.application.Application#installService(com.metamatrix.common.application.ApplicationService)
     */
    public void installService(String type, ApplicationService service) throws ApplicationInitializationException {
        if(service == null) {
            return;
        }
        
        try {
            service.start(this.environment);
            this.environment.bindService(type, service);
            service.bind();
            installedServices.add(type);
        } catch(ApplicationLifecycleException e) {
            throw new ApplicationInitializationException(e, CommonPlugin.Util.getString("BasicApplication.Failed_while_installing_service_of_type__1") + type); //$NON-NLS-1$
        }
    }

    public ApplicationEnvironment getEnvironment() {
        return this.environment;
    }
    /**
     * @see com.metamatrix.common.application.Application#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        for (Iterator i = installedServices.iterator(); i.hasNext();) {
            String type = (String)i.next();
            ApplicationService service = environment.findService(type);
            service.unbind();
            environment.unbindService(type);
            service.stop();
            i.remove();
        }
    }

}
