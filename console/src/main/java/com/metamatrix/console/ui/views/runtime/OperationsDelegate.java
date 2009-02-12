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

//#############################################################################
package com.metamatrix.console.ui.views.runtime;

import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
/**
 * The <code>OperationsDelegate</code> interface is implemented by classes
 * that need to start, stop, stop now, suspend, and resume one or more of
 * the following:
 * <ul>
 *     <li> System,
 *     <li> Process,
 *     <li> Host,
 *     <li> Product Service Configuration, or
 *     <li> Service.
 * </ul>
 * @since Golden Gate
 * @version 1.0
 * @author <a href="mailto:dflorian@metamatrix.com">Dan Florian</a>
 */
public interface OperationsDelegate {


    /**
     * @throws ExternalException
     */
    void startOperation() throws ExternalException;

    /**
     * @throws ExternalException
     */
    void stopOperation() throws ExternalException;

    /**
     * @throws ExternalException
     */
    void stopNowOperation() throws ExternalException;
    
    /**
     *  
     * @throws ExternalException
     * @since 4.4
     */
    void showServcieError() throws ExternalException;

    /**
     * @throws ExternalException
     */
    QueueStatisticsFrame startShowQueue(ServiceData sd) throws ExternalException;
    
    VMStatisticsFrame startShowProcess(ProcessData pd);
    boolean isProcessDisplayed(ProcessData pd);
    void refreshProcess(ProcessData pd);
    void refreshService(ServiceData sd);
    boolean isServiceDisplayed (ServiceData sd);
}
