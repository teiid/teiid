/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.logging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;


/**
 * This test case tests the LogManager.
 */
public class TestLogManager extends TestCase {

    private static final String CONTEXT = "SomeContext"; //$NON-NLS-1$

    /**
     * Constructor for TestLogManager.
     * @param name
     */
    public TestLogManager(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        ListLogger logger = new ListLogger();
        logger.setLogLevel(CONTEXT, MessageLevel.DETAIL);
        LogManager.logListener = logger;
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /*
     * Test for boolean isMessageToBeRecorded(String, int)
     */
    public void testIsMessageToBeRecordedString() {
        assertTrue(LogManager.isMessageToBeRecorded(CONTEXT, MessageLevel.CRITICAL) );

        ListLogger cfg = (ListLogger)LogManager.logListener;
        cfg.setLogLevel(CONTEXT, MessageLevel.NONE);
        assertFalse(LogManager.isMessageToBeRecorded(CONTEXT, MessageLevel.CRITICAL) );
    }

    /**
     * Test that all msgs logged are equal and output in same order.
     */
    public void testLogMessage() throws Exception {
        ListLogger cfg = (ListLogger)LogManager.logListener;
        cfg.setLogLevel(CONTEXT, MessageLevel.INFO );

        List<String> sentMsgList = new ArrayList<String>();
        sentMsgList.add("A message 1"); //$NON-NLS-1$
        sentMsgList.add("A message 2"); //$NON-NLS-1$
        sentMsgList.add("A message 3"); //$NON-NLS-1$
        sentMsgList.add("A message 4"); //$NON-NLS-1$
        sentMsgList.add("A message 5"); //$NON-NLS-1$
        sentMsgList.add("A message 6"); //$NON-NLS-1$

        for (Iterator<String> iter = sentMsgList.iterator(); iter.hasNext();) {
            String msg = iter.next();
            LogManager.logInfo(CONTEXT, msg);
        }

        List<String> recevedMsgList = cfg.getLoggedMessages();
        assertEquals(sentMsgList.size(), recevedMsgList.size());
        assertEquals(sentMsgList, recevedMsgList);
    }

    /**
     *
     * A log listener that saves messages (IStatus)s in a
     * List for later comparison.
     */
    class ListLogger implements Logger {
        private List<String> messages = new ArrayList<String>();
        private Map<String, Integer> contextMap = new HashMap<String, Integer>();
        private int defaultLevel;

        public ListLogger() {
        }

        public void log(int level, String context, Object... msg){
            this.messages.add(msg[0].toString());
        }

        public void log(int level, String context, Throwable t, Object... msg) {
            this.messages.add(msg[0].toString());
        }

        public void shutdown() {
            messages.clear();
            messages = null;

        }

        public int size() {
            return this.messages.size();
        }

        public List<String> getLoggedMessages() {
            return this.messages;
        }

        public Set<String> getContexts() {
            return this.contextMap.keySet();
        }

        public int getLogLevel(String context) {
            Integer level = this.contextMap.get(context);
            if (level != null) {
                return level;
            }
            return defaultLevel;
        }

        public void setLogLevel(String context, int logLevel) {
            this.contextMap.put(context, logLevel);
        }

        @Override
        public boolean isEnabled(String context, int msgLevel) {
            int level = getLogLevel(context);
            return level >= msgLevel;
        }

        @Override
        public void putMdc(String key, String val) {

        }

        @Override
        public void removeMdc(String key) {

        }
    }

}
