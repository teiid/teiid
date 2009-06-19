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

import java.util.EventObject;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.util.Assertion;

abstract public class AbstractEventBroker extends AbstractEventSource implements EventBroker {
    private static final String CANNOT_ADD_SELF_AS_LISTENER_MESSAGE = CorePlugin.Util.getString("AbstractEventBroker.CannotAddSelfAsListener"); //$NON-NLS-1$
    private boolean shutdownRequested = false;
    private boolean shutdownComplete  = false;
    private String name = ""; //$NON-NLS-1$
    
    // variables used when event performance monitoring
    private static final boolean EVENT_PERF;
    private static final double EVENT_FLOOR;
    private StringBuffer eventPerfMsg = new StringBuffer();

	static {
		EVENT_PERF = (System.getProperty("eventPerf") != null); //$NON-NLS-1$
        
        if (EVENT_PERF) {
            final double DEFAULT = 10D;
            double temp = DEFAULT;
    		String txt = System.getProperty("eventFloor"); //$NON-NLS-1$

    		if (txt != null) {
    			try {
    				temp = Double.parseDouble(txt);
                    temp = (temp < 0) ? 0 : temp;
    			}
    			catch (NumberFormatException theException) {
    				temp = DEFAULT;
    			}
    		}
    		EVENT_FLOOR = temp;
        }
        else {
            EVENT_FLOOR = 0;
        }
	}
	
    protected AbstractEventBroker() {
    }

    protected void setName( String name ) {
        if(name == null){
            Assertion.isNotNull(name,CorePlugin.Util.getString("AbstractEventBroker.The_name_of_the_event_broker_may_not_be_null")); //$NON-NLS-1$
        }
        if(name.length() == 0){
            Assertion.isNotZeroLength(name,CorePlugin.Util.getString("AbstractEventBroker.The_name_of_the_event_broker_may_not_be_zero-length")); //$NON-NLS-1$
        }
        this.name = name;
    }
    
    public String getName() {
        return this.name;    
    }

    public String toString() {
        return this.name;    
    }

    public void addListener(Class eventClass, EventObjectListener listener)
        throws EventSourceException {

        //Do not allow add EventBroker to add itself to the list of listeners
        Assertion.isNotNull(listener,CANNOT_ADD_SELF_AS_LISTENER_MESSAGE);

        //Do not allow a listener to be added if the event broker is shutdown
        assertReady();
        super.addListener(eventClass, listener);
    }
    public void addListener(EventObjectListener listener)
        throws EventSourceException {

        //Do not allow add EventBroker to add itself to the list of listeners
        Assertion.isNotNull(listener,CANNOT_ADD_SELF_AS_LISTENER_MESSAGE);

        //Do not allow a listener to be added if the event broker is shutdown
        assertReady();
        super.addListener(listener);
    }

    public void processEvent(EventObject obj) {
        //Do not allow an event to be processed if the event broker is shutdown
        assertReady();
        if (obj != null) {
            this.process(obj);
        }
    }

    public boolean shutdown() throws EventBrokerException {
        this.shutdownRequested = true;
        this.waitToCompleteShutdown();        // block until subclass ensures all events are processed

        try {
            super.removeAllListeners();
        } catch (EventSourceException e) {

        }
        return this.shutdownComplete;
    }

    public boolean isShutdown() {
        return isShutdownComplete();
    }

    protected void notifyListeners(EventObject obj) {
        if (obj != null) {
            
            if (EVENT_PERF && (eventPerfMsg.length() == 0)) {
                eventPerfMsg.append('\n');
            }
            
            // variables used for event performance monitoring
        	double eventTime = 0;
        	int count = 0;

//             System.out.println("\nProcessing event event ... \n" +
//                "   Event type : " + obj.getClass().getName() + "\n" +
//                "   Source     : " + obj.getSource().getClass().getName());

            List listeners = super.getListeners(obj.getClass());
//            System.out.println("#listeners = " + listeners.size() );
            Iterator itr = listeners.iterator();
            while (itr.hasNext()) {
                EventObjectListener listener = (EventObjectListener)itr.next();
// RMH: Don't want to check this, 'cause most of the time the source & the listener are NOT the same object
// RMH:           if (listener != obj.getSource()) {
                    if ( listener != null ) {
                        try {
                        	if (EVENT_PERF) {
                        		double start = System.currentTimeMillis();
                            	listener.processEvent(obj);
                            	double totalTime = System.currentTimeMillis() - start;
                            	eventTime += totalTime;
                            	count++;

                            	if (totalTime >= EVENT_FLOOR) {
                                    final Object[] params = new Object[]{new Double(totalTime),
                                                                         getShortClassName(listener)};
                                    eventPerfMsg.append(CorePlugin.Util.getString("AbstractEventBroker.eventFloorExceeded",params)); //$NON-NLS-1$
                            	}
                        	}
                        	else {
                        		listener.processEvent(obj);
                        	}
                        } catch ( Throwable t ) {
                            LogManager.logError(LogConstants.CTX_COMMUNICATION, t,CorePlugin.Util.getString("AbstractEventBroker.Error_during_event_processing",this.getName())); //$NON-NLS-1$
                        }
                    }
// RMH:           }
            }

            if (EVENT_PERF && (eventTime >= EVENT_FLOOR)) {
                final Object[] params = new Object[]{new Double(eventTime),
                                                     new Integer(count),
                                                     getShortClassName(obj),
                                                     getShortClassName(obj.getSource())};
                eventPerfMsg.append(CorePlugin.Util.getString("AbstractEventBroker.basePerformanceMessage",params)); //$NON-NLS-1$
                            
                if (obj instanceof TargetableEvent) {
                    eventPerfMsg.append(CorePlugin.Util.getString("AbstractEventBroker.targetedPerformanceMessage",((TargetableEvent)obj).getTarget())); //$NON-NLS-1$
                }
                
                eventPerfMsg.append('\n');
                
                LogManager.logInfo(LogConstants.CTX_COMMUNICATION, eventPerfMsg.toString());
                eventPerfMsg.setLength(0);
            }
        }
    }
    
    protected void assertReady() {
        if (this.shutdownRequested || this.shutdownComplete) {
            throw new IllegalStateException(CorePlugin.Util.getString("AbstractEventBroker.No_events_can_be_processed_EventBroker_is_shutdown",this.getName())); //$NON-NLS-1$
        }
    }

    private static String getShortClassName(Object theObject) {
    	String className = theObject.getClass().getName();
    	int index = className.lastIndexOf('.'); 
    	return (index == -1) ? className : className.substring(index + 1);
    }

    protected abstract void process(EventObject obj);
    protected abstract void waitToCompleteShutdown();

    protected boolean isShutdownRequested() {
        return this.shutdownRequested;
    }

    protected boolean isShutdownComplete() {
        return this.shutdownComplete;
    }

    protected void setShutdownComplete(boolean shutdownComplete) {
        this.shutdownComplete = shutdownComplete;
    }
}

