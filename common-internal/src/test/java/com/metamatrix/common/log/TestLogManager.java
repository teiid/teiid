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

package com.metamatrix.common.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.MessageLevel;

/**
 * This test case tests the LogManager.
 */
public class TestLogManager extends TestCase {

	String context = "SomeContext"; //$NON-NLS-1$
	
    /**
     * Constructor for TestLogManager.
     * @param name
     */
    public TestLogManager(String name) {
        super(name);
    }
    
	@Override
	protected void setUp() throws Exception {
    	Map<String, Integer> contextMap = new HashMap<String, Integer>();
    	contextMap.put(context, MessageLevel.DETAIL);
    	LogManager.configuration = new LogConfigurationImpl(contextMap);
	}    
    
    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /*
     * Test for boolean isMessageToBeRecorded(String, int)
     */
    public void testIsMessageToBeRecordedString() {
    	assertTrue(LogManager.isMessageToBeRecorded(context, MessageLevel.CRITICAL) ); 
    	
    	LogConfiguration cfg = LogManager.getLogConfigurationCopy();
        cfg.setLogLevel(context, MessageLevel.NONE);
        LogManager.setLogConfiguration(cfg);
        assertFalse(LogManager.isMessageToBeRecorded(context, MessageLevel.CRITICAL) );
    }

    /**
     * Test that all msgs logged are equal and output in same order.
     */
    public void testLogMessage() throws Exception {
    	
    	
        LogConfiguration cfg = LogManager.getLogConfigurationCopy();
        cfg.setLogLevel(context, MessageLevel.INFO );
        LogManager.setLogConfiguration(cfg);
         
        ListLogger listener = new ListLogger(6);
        LogManager.logListener = listener;

        List<String> sentMsgList = new ArrayList<String>();
        sentMsgList.add("A message 1"); //$NON-NLS-1$
        sentMsgList.add("A message 2"); //$NON-NLS-1$
        sentMsgList.add("A message 3"); //$NON-NLS-1$
        sentMsgList.add("A message 4"); //$NON-NLS-1$
        sentMsgList.add("A message 5"); //$NON-NLS-1$
        sentMsgList.add("A message 6"); //$NON-NLS-1$

        for (Iterator iter = sentMsgList.iterator(); iter.hasNext();) {
            String msg = (String) iter.next();
            LogManager.logInfo(context, msg); 
        }
        
        List recevedMsgList = listener.getLoggedMessages();
        assertEquals(sentMsgList, recevedMsgList);
    }

    /**
     *
     * A log listener that saves messages (IStatus)s in a
     * List for later comparison.
     */
    class ListLogger implements LogListener {
        private List messages = new ArrayList();
        private int expectedMessages;

        public ListLogger(int expectedMessages) {
        	this.expectedMessages = expectedMessages;
        }

        /* (non-Javadoc)
         * @see com.metamatrix.core.log.LogListener#logMessage(org.eclipse.core.runtime.IStatus, long, java.lang.String, java.lang.String)
         */
        public synchronized void log(int level, String context, Object msg){
            this.messages.add(msg);
            if (this.messages.size() == expectedMessages) {
            	this.notifyAll();
            }
        }
        
		public void log(int level, String context, Throwable t, Object msg) {
            this.messages.add(msg);
            if (this.messages.size() == expectedMessages) {
            	this.notifyAll();
            }			
		}        

        /* (non-Javadoc)
         * @see com.metamatrix.core.log.LogListener#shutdown()
         */
        public void shutdown() {
            messages.clear();
            messages = null;

        }

        public int size() {
            return this.messages.size();
        }

        public synchronized List getLoggedMessages() throws InterruptedException {
        	if (this.messages.size() < expectedMessages) {
        		this.wait(1000);
        	}
            return this.messages;
        }

    }



}
