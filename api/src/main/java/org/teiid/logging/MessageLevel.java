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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constants that define the level of the messages that are to be recorded
 * by the LogManager.
 */
public final class MessageLevel {

    /**
     * Message level value that specifies that no messages are to be recorded.
     */
    public static final int NONE            = 0;

    /**
     * Message level value that specifies that critical messages are to be recorded.
     */
    public static final int CRITICAL        = 1;

    /**
     * Message level value that specifies that error messages and critical
     * messages are to be recorded.
     */
    public static final int ERROR           = 2;

    /**
     * Message level value that specifies that warning, error and critical
     * messages are to be recorded.
     */
    public static final int WARNING         = 3;

    /**
     * Message level value that specifies that information, warning, error and critical
     * messages are to be recorded.
     */
    public static final int INFO            = 4;

    /**
     * Message level value that specifies that detailed, information, warning, error and critical
     * messages are to be recorded.
     */
    public static final int DETAIL          = 5;

    /**
     * Message level value that specifies that all messages are to be recorded.
     */
    public static final int TRACE           = 6;


    /**
     * The default message level is WARNING.
     */
    public static final int DEFAULT_MESSAGE_LEVEL = WARNING;

    private static final int MINIMUM = NONE;
    private static final int MAXIMUM = TRACE;

    /**
     * Constants that define the types of the messages that are to be recorded
     * by the LogManager.
     */
    public static class Labels {
        public static final String CRITICAL     = "CRITICAL"; //$NON-NLS-1$
        public static final String ERROR        = "ERROR"; //$NON-NLS-1$
        public static final String WARNING      = "WARNING"; //$NON-NLS-1$
        public static final String INFO         = "INFO"; //$NON-NLS-1$
        public static final String DETAIL       = "DETAIL"; //$NON-NLS-1$
        public static final String TRACE        = "TRACE"; //$NON-NLS-1$
        public static final String NONE         = "NONE"; //$NON-NLS-1$
        static final String UNKNOWN   = "UNKNOWN"; //$NON-NLS-1$
    }

    /**
     * Constants that define the types of the messages that are to be recorded
     * by the LogManager.
     */
    public static class DisplayNames {
        public static final String CRITICAL     = "Critical"; //$NON-NLS-1$
        public static final String ERROR        = "Error"; //$NON-NLS-1$
        public static final String WARNING      = "Warning"; //$NON-NLS-1$
        public static final String INFO         = "Information"; //$NON-NLS-1$
        public static final String DETAIL       = "Detail"; //$NON-NLS-1$
        public static final String TRACE        = "Trace"; //$NON-NLS-1$
        public static final String NONE         = "None"; //$NON-NLS-1$
    }

    private static Map LABEL_TO_LEVEL_MAP = new HashMap();
    private static Map DISPLAY_TO_LEVEL_MAP = new HashMap();
    private static List LABELS = new ArrayList(MAXIMUM - MINIMUM + 1);
    private static List DISPLAYS = new ArrayList(MAXIMUM - MINIMUM + 1);

    static {
        LABEL_TO_LEVEL_MAP.put(Labels.CRITICAL, new Integer(CRITICAL) );
        LABEL_TO_LEVEL_MAP.put(Labels.ERROR, new Integer(ERROR) );
        LABEL_TO_LEVEL_MAP.put(Labels.WARNING, new Integer(WARNING) );
        LABEL_TO_LEVEL_MAP.put(Labels.INFO, new Integer(INFO) );
        LABEL_TO_LEVEL_MAP.put(Labels.DETAIL, new Integer(DETAIL) );
        LABEL_TO_LEVEL_MAP.put(Labels.TRACE, new Integer(TRACE) );
        LABEL_TO_LEVEL_MAP.put(Labels.NONE, new Integer(NONE) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.CRITICAL, new Integer(CRITICAL) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.ERROR, new Integer(ERROR) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.WARNING, new Integer(WARNING) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.INFO, new Integer(INFO) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.DETAIL, new Integer(DETAIL) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.TRACE, new Integer(TRACE) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.NONE, new Integer(NONE) );

        // Do in the correct order so that the indexes match the levels
        LABELS.add(Labels.NONE);
        LABELS.add(Labels.CRITICAL);
        LABELS.add(Labels.ERROR);
        LABELS.add(Labels.WARNING);
        LABELS.add(Labels.INFO);
        LABELS.add(Labels.DETAIL);
        LABELS.add(Labels.TRACE);
        DISPLAYS.add(DisplayNames.NONE);
        DISPLAYS.add(DisplayNames.CRITICAL);
        DISPLAYS.add(DisplayNames.ERROR);
        DISPLAYS.add(DisplayNames.WARNING);
        DISPLAYS.add(DisplayNames.INFO);
        DISPLAYS.add(DisplayNames.DETAIL);
        DISPLAYS.add(DisplayNames.TRACE);
    }

    /**
     * Utility method to set the level of messages that are recorded for this VM.
     * @param newMessageLevel the new level; must be either
     *    <code>MessageLevel.NONE</code>,
     *    <code>MessageLevel.CRITICAL</code>,
     *    <code>MessageLevel.ERROR</code>,
     *    <code>MessageLevel.WARNING</code>,
     *    <code>MessageLevel.INFO</code>,
     *    <code>MessageLevel.DETAIL</code>, or
     *    <code>MessageLevel.TRACE</code>.
     * @throws IllegalArgumentException if the level is out of range.
     */
    public static boolean isMessageLevelValid( int newMessageLevel ) {
        return !( newMessageLevel < MessageLevel.NONE || newMessageLevel > MessageLevel.TRACE );
    }

    /**
     * Utility method for knowing what is the lower boundary for
     * a valid message level.
     * @return int message level
     * @see #NONE
     */
    public static int getValidLowerMessageLevel() {
        return MessageLevel.NONE;
    }

    /**
     * Utility method for knowing what is the upper boundary for
     * a valid message level.
     * @return int message level
     * @see #TRACE
     */
    public static int getValidUpperMessageLevel() {
        return MessageLevel.TRACE;
    }

    public static String getLabelForLevel( int level ) {
        switch ( level ) {
            case MessageLevel.NONE:
                return Labels.NONE;
            case MessageLevel.CRITICAL:
                return Labels.CRITICAL;
            case MessageLevel.ERROR:
                return Labels.ERROR;
            case MessageLevel.WARNING:
                return Labels.WARNING;
            case MessageLevel.INFO:
                return Labels.INFO;
            case MessageLevel.DETAIL:
                return Labels.DETAIL;
            case MessageLevel.TRACE:
                return Labels.TRACE;
        }
        return Labels.UNKNOWN;
        //throw new IllegalArgumentException("The specified message level \"" + level + "\" is invalid");
    }

    public static int getMinimumLevel() {
        return MINIMUM;
    }

    public static int getMaximumLevel() {
        return MAXIMUM;
    }

    public static Collection getDisplayNames() {
        return DISPLAYS;
    }


    /**
     * Utility method to get the labels for the levels, starting with the lowest
     * level and ending with the highest level.
     * @return an ordered list of String labels
     */
    public static List getLabels() {
        return LABELS;
    }



}

