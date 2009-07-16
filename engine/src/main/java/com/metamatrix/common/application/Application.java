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

package com.metamatrix.common.application;

import java.util.ArrayList;
import java.util.Iterator;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;

/**
 */
public class Application {

    protected ApplicationEnvironment environment = new ApplicationEnvironment();
    private ArrayList<String> installedServices = new ArrayList<String>();

    /* 
     * @see com.metamatrix.common.application.Application#installService(com.metamatrix.common.application.ApplicationService)
     */
    public final void installService(String type, ApplicationService service) throws ApplicationInitializationException {
        if(service == null) {
            return;
        }
        
        try {
            service.start(this.environment);
            this.environment.bindService(type, service);
            installedServices.add(0, type);
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
            environment.unbindService(type);
            service.stop();
            i.remove();
        }
    }

}
