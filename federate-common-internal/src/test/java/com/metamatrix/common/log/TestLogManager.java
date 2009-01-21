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

package com.metamatrix.common.log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.internal.core.log.PlatformLog;

/**
 * This test case tests the LogManager.
 */
public class TestLogManager extends TestCase {

    /**
     * Constructor for TestLogManager.
     * @param name
     */
    public TestLogManager(String name) {
        super(name);
    }

	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestLogManager.class);
		//return suite;
        return new TestSetup(suite){
            protected void setUp() throws Exception{
                setUpOnce();
            }
            protected void tearDown() throws Exception{
            }
        };
	}
    
	private static void setUpOnce() throws Exception {
		System.setProperty(CoreConstants.NO_CONFIGURATION, "");//$NON-NLS-1$
		CurrentConfiguration.reset();
		LogManager.stop();
		
	}
    
    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /*
     * Test for boolean isMessageToBeRecorded(String, int)
     */
    public void testIsMessageToBeRecordedStringI() {
        assertFalse(LogManager.isMessageToBeRecorded("SomeContext", MessageLevel.CRITICAL) ); //$NON-NLS-1$
    }

    /**
     * Test that all msgs logged are equal and output in same order.
     */
    public void testLogMessage() throws Exception {
        // init the LogManager (do not rely on later calls to do it)
        LogConfiguration cfg = LogManager.getLogConfiguration();
        cfg.setMessageLevel( MessageLevel.INFO );

        ListLogger listener = new ListLogger(6);
        PlatformLog logger = PlatformLog.getInstance();
        logger.addListener(listener);

        List sentMsgList = new ArrayList();
        sentMsgList.add("A message 1"); //$NON-NLS-1$
        sentMsgList.add("A message 2"); //$NON-NLS-1$
        sentMsgList.add("A message 3"); //$NON-NLS-1$
        sentMsgList.add("A message 4"); //$NON-NLS-1$
        sentMsgList.add("A message 5"); //$NON-NLS-1$
        sentMsgList.add("A message 6"); //$NON-NLS-1$

        for (Iterator iter = sentMsgList.iterator(); iter.hasNext();) {
            String msg = (String) iter.next();
            LogManager.logInfo("SomeContext", msg); //$NON-NLS-1$
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
        public synchronized void logMessage(LogMessage msg){
            this.messages.add(msg.getText());
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
