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

package com.metamatrix.admin.objects;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.metamatrix.admin.api.objects.LogConfiguration;


/** 
 * @since 4.3
 */
public class MMLogConfiguration extends MMAdminObject implements
                                                LogConfiguration {

    /**Dummy identifier for the log as a whole.*/
    private final static String Log_IDENTIFIER = "SYSTEM.LOG"; //$NON-NLS-1$
    private int logLevel;
    
    private Set includedContexts;
    
    private Set discardedContexts;
    
    /**
     * xtor for Log Configuration
     *  
     * @since 4.3
     */
    public MMLogConfiguration() {
        super(new String[] {Log_IDENTIFIER});
    }
    /** 
     * @see com.metamatrix.admin.objects.MMAdminObject#toString()
     * @since 4.3
     */
    public String toString() {
       StringBuffer sb = new StringBuffer("LogConfiguration:"); //$NON-NLS-1$
       sb.append("\tLog Level:  "); //$NON-NLS-1$
       sb.append(getLogLevel()); 
       
       Iterator iter = getIncludedContexts().iterator();
       sb.append("\tLog contexts:"); //$NON-NLS-1$
       while ( iter.hasNext() ) {
           String context = (String) iter.next();
           sb.append(context); 
           if( iter.hasNext()) {
               sb.append(","); //$NON-NLS-1$
           }
       }
       return sb.toString();
    }

    /** 
     * @see com.metamatrix.admin.api.objects.LogConfiguration#getLogLevel()
     * @since 4.3
     */
    public int getLogLevel() {
        return logLevel;
    }

    
    /** 
     * Set the Log Level
     * 
     * @param logLevel The logLevel to set.
     * @since 4.3
     */
    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }
    
    /** 
     * @return Returns the discardedContexts.
     * @since 4.3
     */
    public Set getDiscardedContexts() {
        return (this.discardedContexts != null ? this.discardedContexts : new HashSet());
    }
    
    /** 
     * @param discardedContexts The discardedContexts to set.
     * @since 4.3
     */
    public void setDiscardedContexts(Set discardedContexts) {
        this.discardedContexts = discardedContexts;
    }
    
    /** 
     * @return Returns the includedContexts.
     * @since 4.3
     */
    public Set getIncludedContexts() {
        return (this.includedContexts != null ? this.includedContexts : new HashSet());
    }
    
    /** 
     * @param includedContexts The includedContexts to set.
     * @since 4.3
     */
    public void setIncludedContexts(Set includedContexts) {
        this.includedContexts = includedContexts;
    }

}
