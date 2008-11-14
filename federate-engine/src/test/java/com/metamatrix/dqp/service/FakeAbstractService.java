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

import java.util.Properties;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;

/**
 */
public class FakeAbstractService implements ApplicationService {

    private int initializeCount = 0;
    private int startCount = 0;
    private int bindCount = 0;
    private int unbindCount = 0;
    private int stopCount = 0;
     

    /**
     * 
     */
    public FakeAbstractService() {
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
        this.initializeCount++;
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        this.startCount++;

    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#bind()
     */
    public void bind() throws ApplicationLifecycleException {
        this.bindCount++;

    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     */
    public void unbind() throws ApplicationLifecycleException {
        this.unbindCount++;

    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        this.stopCount++;

    }
    
    public int getInitializeCount() {
        return this.initializeCount;
    }

    public int getStartCount() {
        return this.startCount;
    }

    public int getBindCount() {
        return this.bindCount;
    }

    public int getUnbindCount() {
        return this.unbindCount;
    }

    public int getStopCount() {
        return this.stopCount;
    }

}
