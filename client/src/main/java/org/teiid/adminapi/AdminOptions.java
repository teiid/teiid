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

package org.teiid.adminapi;

import java.io.Serializable;

import com.metamatrix.admin.AdminPlugin;


/** 
 * Creates, collects and stores option values for evaluation when
 * executing methods where decisions should be made based on user preferences.
 * <p>
 * Method of use is to create with an option and use the method {@link #addOption(int)}
 * when more than one option is wished or required.</p>
 * <p>
 * <b>Example:</b>
 * <pre>
 *   AdminOptions options = new AdminOptions(AdminOptions.OnConflict.OVERWRITE);
 *   options.addOption(BINDINGS_IGNORE_DECRYPT_ERROR);
 * </pre></p>
 * @since 4.3
 */
public class AdminOptions implements Serializable {

    // *************************************************************************
    // When adding an option to this interface, don't forget to
    // add the corresponding string to the toString() method
    // and to the ALLOWABLE_OPTIONS bitmask below.
    // *************************************************************************
        
    /**
     * In the case when adding resource to the system, if the resource already 
     * exists in the system, these modes define how to handle the situation.  
     */
    public interface OnConflict {
        
        /**
         * Add all bindings in this file and overwrite any
         * bindings that already exist in the system.
         * <p><b>NOTE</b>: This will result in a {@link VDB} with 
         * a status of {@link VDB#INACTIVE} or 
         * {@link VDB#ACTIVE}.</p>  
         */
        public static final int OVERWRITE= 1;
        
        /**
         * Don't add any existing bindings contained in this file
         * (don't update/replace ones that already exist).  This 
         * will <i>not</i> keep any new bindings specified from being 
         * added.
         * <p><b>NOTE</b>: This will result in a {@link VDB} with 
         * a status of {@link VDB#INACTIVE} or 
         * {@link VDB#ACTIVE}.</p>  
         */
        public static final int IGNORE = 2;
        
        /**
         * If there is conflict in the bindings then return with
         * an exception
         * <p><b>NOTE</b>: This will result in a {@link VDB} with 
         * a status of {@link VDB#INCOMPLETE} if all models in
         * the VDB are not bound.</p> 
         */
        public static final int EXCEPTION = 4;        
    }
    
    /**
     * Connector bindings have encrypted passwords as connection
     * properties.  If the password property cannot be decrypted,
     * the connector binding will not start until the connector
     * binding password property is changed.
     * <p>Adding a VDB with this option allows the VDB and its
     * connector bindings to be added and persisted to the system
     * configuration, even if the connector binding properties
     * cannot be decrypted.  Users should set the password property
     * on all connectors added after using this option.</p>
     * <p><b>NOTE</b>: This will result in a {@link VDB} with 
     * a status of {@link VDB#INACTIVE}.</p> 
     */
    public static final int BINDINGS_IGNORE_DECRYPT_ERROR = 8;
    
// =======================================================================================    
//                     End Options Interface
// =======================================================================================    

    private static final int ALLOWABLEOPTIONS = OnConflict.OVERWRITE | 
                                                    OnConflict.IGNORE |
                                                    OnConflict.EXCEPTION |
                                                    BINDINGS_IGNORE_DECRYPT_ERROR;

    // A bitmask for multiple options
    private int optionsMask;
    
    /**
     * Construct with an option.  For available options, see
     * {@link AdminOptions}.
     * <p>
     * <b>Note</b>: A RutimeException is thrown for any option given 
     * that is not found in the interface.</p>
     *  
     * @param option One of the available options in {@link AdminOptions}.
     * @throws RuntimeException for any option given that is not 
     * found in the interface.
     * @since 4.3
     */
    public AdminOptions(int option) throws RuntimeException {
        super();
        
        addOption(option);
    }

    /**
     * Add an option to this object if multiple options are required.
     * <p>
     * <b>Note</b>: A RutimeException is thrown for any option given 
     * that is not found in the interface.</p>
     * 
     * @param anOption the option to add.
     * @throws RuntimeException for any option given that is not 
     * found in the interface.
     * @since 4.3
     */
    public void addOption(int anOption) {
        if (anOption != 0 && (ALLOWABLEOPTIONS & anOption) == anOption) {
            this.optionsMask |= anOption;
        } else {
            throw new RuntimeException(AdminPlugin.Util.getString("AdminOptions.Unknown_option", new Object[] {"" + anOption})); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    /**
     * Check if the given option was specified.
     * 
     * @param anOption the option to check.
     * @return <code>true</true> iff this opject contains the
     * geven option.
     * @since 4.3
     */
    public boolean containsOption(int anOption) {
        return (this.optionsMask & anOption) == anOption;
    }

    /** 
     * @see java.lang.Object#toString()
     * @since 4.3
     */
    public String toString() {
        StringBuffer optionString = new StringBuffer("["); //$NON-NLS-1$
        
        if ( (optionsMask & OnConflict.OVERWRITE) == OnConflict.OVERWRITE ) {
            optionString.append("OnConflict_OVERWRITE, "); //$NON-NLS-1$
        }
        if ( (optionsMask & OnConflict.IGNORE) == OnConflict.IGNORE ) {
            optionString.append("OnConflict_IGNORE, "); //$NON-NLS-1$
        }
        if ( (optionsMask & OnConflict.EXCEPTION) == OnConflict.EXCEPTION ) {
            optionString.append("OnConflict_EXCEPTION, "); //$NON-NLS-1$
        }
        if ( (optionsMask & BINDINGS_IGNORE_DECRYPT_ERROR) == BINDINGS_IGNORE_DECRYPT_ERROR ) {
            optionString.append("BINDINGS_IGNORE_DECRYPT_ERROR, "); //$NON-NLS-1$
        } 

        if ( optionString.length() == 1 ) {
            optionString.append("UNKNOWN"); //$NON-NLS-1$
        } else if (optionString.length() > 2 && optionString.charAt(optionString.length() - 2) == ',' ) {
            optionString.setLength(optionString.length() - 2);
        }
        
        optionString.append(']');
        return optionString.toString();
    }
    
}
