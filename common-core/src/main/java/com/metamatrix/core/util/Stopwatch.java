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

package com.metamatrix.core.util;

import java.io.Serializable;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.log.Logger;
import com.metamatrix.core.log.MessageLevel;

public class Stopwatch implements Serializable {
    private long start = 0;
    private long stop = 0;
    private Statistics stats = new Statistics();
    private boolean active = true;

    private static final String SECONDS      = CorePlugin.Util.getString("Stopwatch.seconds"); //$NON-NLS-1$
    private static final String MILLISECONDS = CorePlugin.Util.getString("Stopwatch.milliseconds"); //$NON-NLS-1$
    private static final int VALUE_LENGTH = 10;

    /**
     * Return whether the stopwatch is active.  When the stopwatch is active,
     * it is recording the time durations (via <code>start</code> and <code>stop</code>)
     * and will print duration statistics (via <code>printDuration</code>).
     * When the stopwatch is inactive, invoking these methods does nothing
     * but return immediately.
     * @return true if the stopwatch is active, or false if it is inactive.
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Set the stopwatch as inactive.
     * @see isActive
     */
    public void setInactive() {
        active = false;
    }
    /**
     * Set the stopwatch as active.
     * @see isActive
     */
    public void setActive() {
        active = true;
    }

    /**
     * If the stopwatch is active, record the starting time for a time segment.
     * If the stopwatch is inactive, the method returns immediately.
     * @see isActive
     */
    public void start() {
        if ( active ) {
            start = System.currentTimeMillis();
        }
    }
    
    /**
     * If the stopwatch is active, record the starting time for a time segment.
     * If the stopwatch is inactive, the method returns immediately.
     * @see isActive
     */
    public void start(boolean reset) {
        if( reset )
            reset();
        start();
    }
    
    /**
     * If the stopwatch is active, record the ending time for a time segment.
     * If the stopwatch is inactive, the method returns immediately.
     * @see isActive
     */
    public void stop() {
        if ( active ) {
            stop = System.currentTimeMillis();
            stats.add( stop - start );
        }
    }
    /**
     * Reset the statistics for this stopwatch, regardless of the active state.
     */
    public void reset() {
        start    = 0;
        stop     = 0;
        stats.reset();
    }
    /**
     * Return the total duration recorded, in milliseconds.
     * @return the total number of milliseconds that have been recorded
     */
    public long getTotalDuration() {
        return stats.getTotal();
    }
    /**
     * Return the average duration recorded as a date.
     * @return the number of milliseconds that have been recorded averaged
     * over the number of segments
     */
    public float getAverageDuration() {
        return stats.getAverage();
    }
    /**
     * Return the number of segments that have been recorded.
     * @return the number of segments
     */
    public int getSegmentCount() {
        return stats.getCount();
    }

    public String toString() {
        String units = MILLISECONDS;
        StringBuffer valueString = null;
        long value = getTotalDuration();
        if ( value >= 1000 ) {
            float fvalue = value / 1000.0f;
            units = SECONDS;
            valueString = new StringBuffer(Float.toString(fvalue));
        } else {
            valueString = new StringBuffer(Long.toString(value));
        }
        valueString.append(units);
        return valueString.toString();
    }

    public String getTimeValueAsString( long value ) {
        String units = MILLISECONDS;
        StringBuffer valueString = null;
        if ( value >= 1000 ) {
            float fvalue = value / 1000.0f;
            units = SECONDS;
            valueString = new StringBuffer(Float.toString(fvalue));
        } else {
            valueString = new StringBuffer(Long.toString(value));
        }
        while ( valueString.length() < VALUE_LENGTH ) valueString.insert(0,' ');
        return "" + valueString + units; //$NON-NLS-1$
    }
    public String getTimeValueAsString( float value ) {
        String units = MILLISECONDS;
        if ( value >= 1000.0f ) {
            value = value / 1000.0f;
            units = SECONDS;
        }
        StringBuffer valueString = new StringBuffer(Float.toString(value));
        while ( valueString.length() < VALUE_LENGTH ) valueString.insert(0,' ');
        return "" + valueString + units; //$NON-NLS-1$
    }
    public String getValueAsString( int value ) {
        StringBuffer valueString = new StringBuffer(Integer.toString(value));
        while ( valueString.length() < VALUE_LENGTH ) valueString.insert(0,' ');
        return "" + valueString; //$NON-NLS-1$
    }

    /**
     * Print the current statistics
     * @param stream the stream to which the statistics should be printed
     */
    public void printStatistics(java.io.PrintStream stream) {
        if ( active ) {
            stream.println(CorePlugin.Util.getString("Stopwatch.Stopwatch_Statistics")); //$NON-NLS-1$
            stream.println(CorePlugin.Util.getString("Stopwatch.Statistics_Total",this.getTimeValueAsString( stats.getTotal() ) ) ); //$NON-NLS-1$
            stream.println(CorePlugin.Util.getString("Stopwatch.Statistics_Previous",this.getTimeValueAsString( stats.getLast() ) ) ); //$NON-NLS-1$
            if ( stats.getCount() > 1 ) {
                stream.println(CorePlugin.Util.getString("Stopwatch.Statistics_Count",this.getValueAsString( stats.getCount() ) ) ); //$NON-NLS-1$
                stream.println(CorePlugin.Util.getString("Stopwatch.Statistics_Average",this.getTimeValueAsString( stats.getAverage() ) ) ); //$NON-NLS-1$
                stream.println(CorePlugin.Util.getString("Stopwatch.Statistics_Minimum",this.getTimeValueAsString( stats.getMinimum() ) ) ); //$NON-NLS-1$
                stream.println(CorePlugin.Util.getString("Stopwatch.Statistics_Maximum",this.getTimeValueAsString( stats.getMaximum() ) ) ); //$NON-NLS-1$
            }
        }
    }

    /**
     * Print the current statistics to System.out
     */
    public void printStatistics() {
        printStatistics(System.out);
    }


    public class Statistics implements Serializable {
        private long minimum = 0;
        private long maximum = 0;
        private long last = 0;
        private long total = 0;
        private int count = 0;
        private boolean minimumInitialized = false;

        public long getMinimum() { return minimum; }
        public long getMaximum() { return maximum; }
        public long getLast() { return last; }
        public float getAverage() { return ( (float)total / (float)count ); }
        public long getTotal() { return total; }
        public int getCount() { return count; }
        public void add( long duration ) {
            ++count;
            total += duration;
            last = duration;
            if ( duration > maximum ) {
                maximum = duration;
            }
            else if ( !minimumInitialized || duration < minimum ) {
                minimum = duration;
                minimumInitialized = true;
            }
        }
        public void reset() {
            minimum = 0;
            maximum = 0;
            last = 0;
            total = 0;
            count = 0;
            minimumInitialized = false;
        }
    }
    
    /**
     * Logs a message containing a time increment in milliseconds and a messages describing the operation or context that
     * the time relates to.
     * @param message
     * @param time
     * @since 4.3
     */
    public static void logTimedMessage(String message, long time, Logger log) {
    	log.log(MessageLevel.INFO, getTimeString(time) + message); 
    }
    
    /**
     * This convience method stops the current stopwatch, logs a message containing the resulting time increment/duration
     * and restarts the stopwatch. 
     * @param message
     * @since 4.3
     */
    public void stopLogIncrementAndRestart(String message, Logger log) {
        stop();
        logTimedMessage(message, getTotalDuration(), log);
        // Restart by reset = true
        start(true);
    }
    
    /**
     * This convience method stops the current stopwatch, prints a message to System.out containing the resulting time increment/duration
     * and restarts the stopwatch.
     * @param message
     * @since 4.3
     */
    public void stopPrintIncrementAndRestart(String message) {
        stop();
        System.out.println(message + getTotalDuration());
        // Restart by reset = true
        start(true);
    }
    
    private static String getTimeString(long time) {
        String timeString = StringUtil.Constants.EMPTY_STRING + time; 
        int nSpaces = 8 - timeString.length();
        StringBuffer buff = new StringBuffer();
        
        buff.append("Time = ["); //$NON-NLS-1$
        for(int i=0; i<nSpaces; i++ ) {
            buff.append(StringUtil.Constants.SPACE);
        }
        buff.append(timeString + "] ms : "); //$NON-NLS-1$
        
        return buff.toString();
    }
}

