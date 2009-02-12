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

package com.metamatrix.toolbox.event;

import java.util.EventObject;
import java.util.Properties;
import java.util.Iterator;

/**
 * This event is fired whenever a user Preferences are changed.
 * Listeners can obtain the current preference values from from
 * CurrentConfiguration(ApplicationConstants.SOME_FILTER_CONSTANT)
 */
public class UserPreferencesEvent extends EventObject {

    private Properties newProps;
    private Properties oldProps;
    private Properties differences;

    public UserPreferencesEvent(Object source, Properties newPreferences, Properties oldPreferences) {
        super(source);
        newProps = newPreferences;
        oldProps = oldPreferences;
    }

    /**
     * Return a Properties object containing the new set of preferences.
     */
    public Properties getNewPreferences() {
        return newProps;
    }

    /**
     * Return a Properties object containing the preferences BEFORE this event occurred.
     */
    public Properties getOldPreferences() {
        return oldProps;
    }

    /**
     * Return a Properties object containing only the set of properties that have either
     * changed or have been newly created.
     */
    public Properties getChanges() {
        if ( differences == null ) {
            differences = new Properties();

            Iterator iter = newProps.keySet().iterator();
            while ( iter.hasNext() ) {
                String key = (String) iter.next();
                if ( oldProps.containsKey(key) ) {
                    if ( !(oldProps.getProperty(key).equals(newProps.getProperty(key))) ) {
                        differences.put(key, newProps.getProperty(key));
                    }
                } else {
                    differences.put(key, newProps.getProperty(key));
                }
            }
        }
        return differences;
    }

}

