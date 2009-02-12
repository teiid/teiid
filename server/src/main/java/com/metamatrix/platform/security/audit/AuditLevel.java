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

package com.metamatrix.platform.security.audit;

import java.util.*;

import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.util.ErrorMessageKeys;

/**
 * Constants that define the level of the messages that are to be recorded
 * by the LogManager.
 */
public final class AuditLevel {

    /**
     * Message level value that specifies that no messages are to be recorded.
     */
    public static final int NONE            = 0;

    /**
     * Message level value that specifies that critical messages are to be recorded.
     */
    public static final int FULL            = 1;

    /**
     * The default message level is NONE.
     */
    public static final int DEFAULT_AUDIT_LEVEL = NONE;

    private static final int MINIMUM = NONE;
    private static final int MAXIMUM = FULL;

    /**
     * Constants that define the types of the messages that are to be recorded
     * by the LogManager.
     */
    public static class Labels {
        public static final String FULL         = "FULL"; //$NON-NLS-1$
        public static final String NONE         = "NONE"; //$NON-NLS-1$
        static final String UNKNOWN             = "UNKNOWN"; //$NON-NLS-1$
    }

    /**
     * Constants that define the types of the messages that are to be recorded
     * by the LogManager.
     */
    public static class DisplayNames {
        public static final String FULL         = "Full"; //$NON-NLS-1$
        public static final String NONE         = "None"; //$NON-NLS-1$
        static final String UNKNOWN             = "Unknown"; //$NON-NLS-1$
    }

    private static Map LABEL_TO_LEVEL_MAP = new HashMap();
    private static Map DISPLAY_TO_LEVEL_MAP = new HashMap();
    private static List LABELS = new ArrayList(MAXIMUM - MINIMUM + 1);
    private static List DISPLAYS = new ArrayList(MAXIMUM - MINIMUM + 1);

    static {
        LABEL_TO_LEVEL_MAP.put(Labels.FULL, new Integer(FULL) );
        LABEL_TO_LEVEL_MAP.put(Labels.NONE, new Integer(NONE) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.FULL, new Integer(FULL) );
        DISPLAY_TO_LEVEL_MAP.put(DisplayNames.NONE, new Integer(NONE) );

        // Do in the correct order so that the indexes match the levels
        LABELS.add(Labels.NONE);
        LABELS.add(Labels.FULL);
        DISPLAYS.add(DisplayNames.NONE);
        DISPLAYS.add(DisplayNames.FULL);
    }

    /**
     * Utility method to set the level of messages that are recorded for this VM.
     * @param newAuditLevel the new level; must be either
     *    <code>AuditLevel.NONE</code>,
     *    <code>AuditLevel.FULL</code>
     * @throws IllegalArgumentException if the level is out of range.
     */
    public static boolean isAuditLevelValid( int newAuditLevel ) {
        return !( newAuditLevel < AuditLevel.MINIMUM || newAuditLevel > AuditLevel.MAXIMUM );
    }

    /**
     * Utility method to get the labels for the levels, starting with the lowest
     * level and ending with the highest level.
     * @return an ordered list of String labels
     */
    public static List getLabels() {
        return LABELS;
    }

    /**
     * Utility method to get the display names for the levels, starting with the lowest
     * level and ending with the highest level.
     * @return an ordered list of String display names
     */
    public static List getDisplayNames() {
        return DISPLAYS;
    }

    public static String getLabelForLevel( int level ) {
        switch ( level ) {
            case AuditLevel.NONE:
                return Labels.NONE;
            case AuditLevel.FULL:
                return Labels.FULL;
        }
        return Labels.UNKNOWN;
    }

    public static String getDisplayNameForLevel( int level ) {
        switch ( level ) {
            case AuditLevel.NONE:
                return DisplayNames.NONE;
            case AuditLevel.FULL:
                return DisplayNames.FULL;
        }
        return DisplayNames.UNKNOWN;
    }

    public static int getLevelForLabel(String messageLabel) {
        Integer result = (Integer) LABEL_TO_LEVEL_MAP.get(messageLabel);
        if ( result == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0001, messageLabel));
        }
        return result.intValue();
    }

    public static int getLevelForDisplayName(String messageDisplayName) {
        Integer result = (Integer) DISPLAY_TO_LEVEL_MAP.get(messageDisplayName);
        if ( result == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0002, messageDisplayName));
        }
        return result.intValue();
    }

    public static int getMinimumLevel() {
        return MINIMUM;
    }

    public static int getMaximumLevel() {
        return MAXIMUM;
    }

}

