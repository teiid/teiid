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

package com.metamatrix.common.config.model;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.core.util.StringUtil;




/** 
 * @since 4.3
 */
public class PropertyValidations  {

    
   
    public static final String UDP_MCAST_ADDR_PROPERTY = "udp.mcast_addr"; //$NON-NLS-1$
    public static final String SYSTEM_NAME = CurrentConfiguration.CLUSTER_NAME; 

    private static final String MULTICAST_PORT_FORMAT = "224.255.255.255"; //$NON-NLS-1$
   

    /** 
     * @see com.metamatrix.common.util.commandline.IPropertyValidation#isPropertyValid(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void isPropertyValid(String key,
                                String value) throws ConfigurationException {
        
        if (key == null) {
            return;
        }
         if (key.equalsIgnoreCase(UDP_MCAST_ADDR_PROPERTY)) {
            validate_multicast_port(key, value);
        }
         
         if (key.equalsIgnoreCase(SYSTEM_NAME)) {
             validate_nonnull_and_contiguous("System Name", key, value);//$NON-NLS-1$
         }         
    }
    
    protected void validate_multicast_port(String key, String value) throws ConfigurationException {
        if (value == null || value.trim().length() == 0) {
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_format", new Object[] {"MulticastPort", "Null", MULTICAST_PORT_FORMAT} )); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        
        List parts = StringUtil.getTokens(value, "."); //$NON-NLS-1$
        // the port must have 4 parts
        if (parts.size() != 4) {
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_format", new Object[] {"MulticastPort", value, MULTICAST_PORT_FORMAT} )); //$NON-NLS-1$ //$NON-NLS-2$                            
        }
        
        validate_nonnull_and_contiguous("MulticastPort", key, value); //$NON-NLS-1$
        
        // verify each part is numeric
        for (Iterator it=parts.iterator(); it.hasNext();) {
            String part = (String) it.next();
            if (isInvalidNumeric(part ))  {
                throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_numeric_value", new Object[] {part, ", must be between 0 and 255"} )); //$NON-NLS-1$ //$NON-NLS-2$ 
            }
            
            validate_port_range(key, part, 0, 255);
        }
        
        
    }
    
    protected void validate_port_range(String key, String value, int start, int end) throws ConfigurationException {
        if (value == null || value.trim().length() == 0) {
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_format", new Object[] {key, "Null", "Range " + start + " and " + end } )); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }
        
        Long nbr = null;
        try {
            nbr = new Long(value);
        } catch (NumberFormatException nfe) {
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_numeric_value", new Object[] {key, ", must be between " + start + " and " + end} )); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$                
        }
        
        if (nbr.longValue() >= start && nbr.longValue() <= end) {
              
        }else {
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_numeric_value", new Object[] {key, ", must be between " + start + " and " + end} )); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$                              
        }
        
        
    }    
    
    protected void validate_nonnull_and_contiguous(String display, String key, String value) throws ConfigurationException {
        if (value == null || value.trim().length() == 0) {
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_contiguous_value", new Object[] {display} )); //$NON-NLS-1$ 
        }
        
        if (value.indexOf(" ") > 0) { //$NON-NLS-1$
            throwException(CommonPlugin.Util.getString("PropertyValidation.Invalid_contiguous_value", new Object[] {display} )); //$NON-NLS-1$ 
        }
               
    }
    
    protected void throwException(String msg) throws ConfigurationException {
        throw new ConfigurationException(msg);
        
    }
    
    protected boolean isInvalidNumeric(String value) {
        try {
          new Long(value);
          return false;
        } catch (NumberFormatException nfe) {
            return true;
        }
    }

}
