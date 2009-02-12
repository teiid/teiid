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

package com.metamatrix.core.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.util.Assertion;

abstract public class AbstractEventSource implements EventSource {

    private static final String LISTENER_MAY_NOT_BE_NULL = CorePlugin.Util.getString("AbstractEventSource.The_event_listener_may_not_be_null"); //$NON-NLS-1$
    private static final String EVENT_CLASS_MAY_NOT_BE_NULL = CorePlugin.Util.getString("AbstractEventSource.The_event_class_may_not_be_null"); //$NON-NLS-1$

    Map eventClassListeners = new HashMap(5);
    List eventListeners     = new ArrayList(5);

    public synchronized void addListener(Class eventClass, EventObjectListener listener) 
        throws EventSourceException {
        Assertion.isNotNull(listener,LISTENER_MAY_NOT_BE_NULL);
        Assertion.isNotNull(eventClass,EVENT_CLASS_MAY_NOT_BE_NULL);

        if (! eventClassListeners.containsKey(eventClass)) {
            List listenerList = new ArrayList(1);
            listenerList.add(listener);
            eventClassListeners.put(eventClass, listenerList);
        } else {
            List listenerList = (List)eventClassListeners.get(eventClass);
            if (! listenerList.contains(listener)) {
                listenerList.add(listener);
            }
        }
    }
    public synchronized void addListener(EventObjectListener listener)
        throws EventSourceException {
        Assertion.isNotNull(listener,LISTENER_MAY_NOT_BE_NULL);
        if (! eventListeners.contains(listener)) {
            eventListeners.add(listener);
        }
    }
    public synchronized void removeListener(Class eventClass, EventObjectListener listener)
        throws EventSourceException {
        Assertion.isNotNull(listener,LISTENER_MAY_NOT_BE_NULL);
        Assertion.isNotNull(eventClass,EVENT_CLASS_MAY_NOT_BE_NULL);

        if (eventClassListeners.containsKey(eventClass)) {
            List listenerList = (List)eventClassListeners.get(eventClass);
            listenerList.remove(listener);
        }
    }
    public synchronized void removeListener(EventObjectListener listener)
        throws EventSourceException {
        Assertion.isNotNull(listener,LISTENER_MAY_NOT_BE_NULL);

        eventListeners.remove(listener);

        // Remove the listener from all of the event class listeners ...
        Iterator iter = eventClassListeners.values().iterator();
        while ( iter.hasNext() ) {
            List listenerList = (List) iter.next();
            listenerList.remove(listener);
        }
    }
    public synchronized void removeAllListeners()
        throws EventSourceException {
        eventClassListeners.clear();
        eventListeners.clear();
    }
    public synchronized List getListeners() {

        // Return those listeners that listen to all events ...
        return new ArrayList(eventListeners);
    }

    public synchronized List getAllListeners() {
        Set result = new HashSet();
        result.addAll(eventListeners);
        Iterator iter = eventClassListeners.values().iterator();
        while ( iter.hasNext() ) {
            List listenerList = (List) iter.next();
            result.addAll(listenerList);
        }
        return new ArrayList(result);
    }

    public synchronized List getListeners(Class eventClass) {
        Assertion.isNotNull(eventClass,EVENT_CLASS_MAY_NOT_BE_NULL);

        // Always include those listeners that listen to all events ...
        List returnList = new ArrayList(eventListeners);

        // Add any those that listen to the specific event class ...
        List listeners = (List)eventClassListeners.get(eventClass);
        if ( listeners != null ) {

            // Add them one-by-one, so there are no duplicates ...
            Object listener = null;
            Iterator itr = listeners.iterator();
            while (itr.hasNext()) {
                listener = itr.next();
                if (! returnList.contains(listener)) {
                    returnList.add(listener);
                }
            }
        }

        return returnList;
    }

    /**
     * Add to this event source all the listeners of the specified AbstractEventSource.
     * This implementation is careful <i>not</i> to get a lock on both objects at once.
     */
    public void addListeners(AbstractEventSource eventSource) throws EventSourceException {
        // This makes a copy within the synchronized method ...
        final List anyListeners = eventSource.getListeners();

        // Copy the contents of the map into a temporary map ...
        final Map listenersByClass = new HashMap();
        synchronized(eventSource) {
            final Iterator classIter = eventSource.eventClassListeners.entrySet().iterator();
            while (classIter.hasNext()) {
                final Map.Entry entry = (Map.Entry) classIter.next();
                final Object eventClass = entry.getKey();
                final List listeners = (List) entry.getValue();
                // Add a copy of the array to the temp map ...
                listenersByClass.put(eventClass,new ArrayList(listeners));
            }
            
        }
    
        // Now add all the listeners (each of these will be synchronized individually) ...
        // Add the 'any' listeners ...
        this.eventListeners.addAll(anyListeners);

        // Add the listeners to specific event classes ...
        final Iterator classIter = listenersByClass.entrySet().iterator();
        while (classIter.hasNext()) {
            final Map.Entry entry = (Map.Entry) classIter.next();
            final Class eventClass = (Class) entry.getKey();
            final List listeners = (List) entry.getValue();
            // Add a copy of the array to the temp map ...
            final Iterator listenerIter = listeners.iterator();
            while (listenerIter.hasNext()) {
                final EventObjectListener listener = (EventObjectListener) listenerIter.next();
                this.addListener(eventClass,listener);
            }
        }

    }

}

