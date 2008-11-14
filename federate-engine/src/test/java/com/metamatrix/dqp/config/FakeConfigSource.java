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

package com.metamatrix.dqp.config;

import java.util.*;
import java.util.HashMap;
import java.util.Properties;

import com.metamatrix.common.application.ApplicationService;

/**
 */
public class FakeConfigSource implements DQPConfigSource {

    private Map services = new HashMap();
    private Properties props = new Properties();
    
    /**
     * 
     */
    public FakeConfigSource() {
        super();
    }

    public void addProperty(String prop, String value) {
        props.setProperty(prop, value);
    }

    /* 
     * @see com.metamatrix.dqp.config.DQPConfigSource#getProperties()
     */
    public Properties getProperties() {
        return new Properties();
    }
    
    public void addService(String svcType, ApplicationService svc) {
        this.services.put(svcType, svc);
    }

    /* 
     * @see com.metamatrix.dqp.config.DQPConfigSource#getService(java.lang.String)
     */
    public ApplicationService getService(String serviceName) {
        return (ApplicationService) services.get(serviceName);
    }

}
