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

package com.metamatrix.internal.core.id;

import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Random;

import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.UUID;

/**
 * <p>This class is a factory for generating universally unique identifiers
 * (UUID's). </p>
 *
 * <h3>Output format for UUIDs</h3>
 * <p>UUIDs are output in the following 36-character format:
 * <pre>
 *    xxxxxxxx-yyyy-zzzz-cccc-nnnnnnnnnnnn
 * </pre>
 * where x=least significant time component, y=middle significant time component,
 * z=most significant time component multiplexed with version, c=clock sequence
 * multiplexed with variant, and n=node component (random number).
 * </p>
 *
 * <p>The generated ID's conform somewhat to the (now expired) IETF internet
 * draft standard, "UUIDs and GUIDs", DCE spec on UUIDs. </p>
 *
 * <ul>
 *   <li>
 *   <a href="http://hegel.ittc.ukans.edu/topics/internet/internet-drafts/draft-l/draft-leach-uuids-guids-01.txt">
 *      UUIDs and GUIDs, P. Leach, R. Salz, 02/05/1998</a>
 *   </li>
 *   <li>
 *   <a href="http://www.opengroup.org/onlinepubs/009629399/apdxa.htm">
 *      DCE Universal Unique Identifier</a>.
 *   </li>
 * </ul></p>
 *
 * <p>All references in this code to bit positions as "least significant" and
 * "most significant" refer to the bits moving from right to left, respectively.
 * </p>
 */
public class EmbeddedUUIDFactory {

    // UUID spec version 
    private static final int UUID_VERSION = UUID.Version.TIME_BASED;      // = 1;  
    
    // UUID variant
    private static final int UUID_VARIANT = UUID.Variant.STANDARD;        // = 0x8000;
    
    // Random number generation algorithm
    private static final String RNG_ALGORITHM = "SHA1PRNG"; //$NON-NLS-1$
    
    // Number of bytes in the node portion of the UUID
    private static final int N_NODE_BYTES = 6;

    // Maximum number for clock sequence number
    private static final int CLOCK_SEQ_LAST = 0x4000;   // 16384
    
    // Conversion to go from milliseconds to nanoseconds
    private static final int MILLI_TO_NANO = 1000000;

    // Calendar used to determine timezone and daylight savings time offsets
    // This cannot be static as it is locale-specific 
    private final Calendar calendar = Calendar.getInstance();
    
    /**
     * Random number generator.
     */
    private Random rng = null;

    /**
     * Keeps track of how many ID's were created by this instance of this
     * factory.
     */
    private long counter = 0;

    /**
     * The "node" part of the UUID, which is meant to represent the spatial
     * component of the ID.  This will be "simulated" using a pseudorandom
     * number from a cryptographically strong random number generator.  Only
     * one of these will be generated per factory instance.
     */
    long nodeComponent;

    /**
     * The clock sequence number.  This is initialized at the time of
     * construction of an instance of this factory.  It must be updated
     * whenever the system time is not monotonically increasing.
     */
    int clockSeq;

    /**
     * The last timestamp that was generated.  Each new timestamp is compared
     * with the last, and if it is not greater than the last one, then the
     * clock sequence number is incremented.
     */
    long lastTime = 0L;

    /**
     * <p>The maximum number of UUID's that will be generated per system clock
     * tick.  This is based on the estimated system clock resolution.  This
     * essentially allows multiple UUID's to be "squeezed in" in the nano-
     * seconds between system clock events. </p>
     *
     * <p>Since the clock resolution is in terms of milliseconds, whereas the
     * time component used for UUID generation is in terms of nanoseconds, the
     * resolution in nanoseconds is obtained by multiplying the system
     * resolution by 1e6.  The max # UUIDs per tick is then taken to be
     * 1/10 of that value, to be conservative. </p>
     */
    private long maxUuidsPerTick;

    /**
     * The number of UUIDs generated for the current time tick.
     */
    private int uuidsThisTick = 0;

    // -------------------------------------------------------------------------
    //                           C O N S T R U C T O R
    // -------------------------------------------------------------------------
    
    /**
     * <p>Construct an instance of this factory.  This constructor attempts 
     * to get an instance of a cryptographically strong pseudo-random number
     * generator {@link java.security.SecureRandom}, using the "SH1" algorithm.
     * If this fails, a message is logged and the standard random number
     * generator {@link java.util.Random} is used. </p>
     *
     * <p>This constructor also loads up a single random number to be used
     * for all ID's generated by this factory.  This random number is meant to
     * represent the "node" part of ID, which is supposed to cover the
     * spatial component of the generated ID's. </p>
     */
    public EmbeddedUUIDFactory() {
        try {
            rng = SecureRandom.getInstance( RNG_ALGORITHM );
        } catch( Exception everything ) {
            // Log the error...
            // TBD
            System.out.println("Couldn't initialize random number generation algorithm " //$NON-NLS-1$
                + RNG_ALGORITHM );
            rng = new Random();
        }

        // Initialize the node component...
        initNodeComponent();

        // Initialize the clock sequence...
        initClockSequence();

        // Initialize the system time for the locale
        lastTime = System.currentTimeMillis();

        // Get an estimate of the system's clock resolution; this will be used
        // as the maximum number of UUID's that can be generated per clock tick
        long clockResolution = getClockResolution(); // In milliseconds

        // Use 10% of resolution in nanoseconds ...
        maxUuidsPerTick = (clockResolution * MILLI_TO_NANO) / 10;
    }
    
    // -------------------------------------------------------------------------
    //                       P U B L I C     M E T H O D S
    // -------------------------------------------------------------------------
    
    /** Return the number of ID's that this factory instance has generated. */
    public long getCount() {
        return counter;
    }
    
    /**
     * <p>Return whether the specified ObjectID instance is valid.  Only ObjectID
     * instances that are for this protocol will be passed in. </p>
     * 
     * <p>This implementation defers the validation to the UUID class. </p>
     *
     * @param id the ID that is to be validated, and which is never null
     * @return true if the instance is valid for this protocol, or false if
     * it is not valid.
     */
    public boolean validate(ObjectID id) {
        if ( id instanceof UUID ) {
            return UUID.validate(id);
        }
        return false;
    }    

    // -------------------------------------------------------------------------
    //                   G E N E R A T I O N    M E T H O D S
    // -------------------------------------------------------------------------
    
    /**
     * <p>Create a new ObjectID instance using this protocol. </p>
     *
     * <p>This member must be synchronized because it makes use of shared
     * internal state. </p>
     *
     * <p>This algorithm follows the one specified in the IETF spec.  It uses
     * a pseudo-random number generated by a cryptographically string random
     * number generator for the "node" component, as recommended in Section 4
     * of that paper. </p>
     *
     * <h3>Algorithm from IETF spec</h3>
     * <p>
     * <ol>
     *    <li> Determine the values of the timestamp and clock sequence.  For
     *         the purposes of this algorithm, consider the timestamp to be a
     *         60-bit unsigned integer and the clock sequence to be a 14-bit
     *         unsigned integer. </li>
     *    <li> Adjust the clock sequence if time has "moved backward". </li>
     *    <li> Adjust the local system time to convert from millseconds
     *         to nanoseconds, and to account for multiple UUIDs per tick. </li>
     *    <li> Adjust the clock sequence if time has "moved backward". </li>
     *    <li> Put the time component (adjusted for locale and sclae) into
     *         the most significant part of the UUID, with the least significant
     *         part of time in the most significant part of the UUID. </li>
     *    <li> Multiplex the spec version into the most significant 4 bits of
     *         of the "high time" field </li>
     *    <li> Put the clock sequence number into the least significant 14 bits
     *         of the most significant 2 bytes (16 bits) of the least significant
     *         part of the UUID. </li>
     *    <li> Multiplex the variant into the most significant 2 bits of the
     *         clock sequence number bytes. </li>
     *    <li> Set the least significant 6 bytes of the least significant part
     *         of the UUID to the random number representing the "node".
     *         The highest bit of the node component should be '1'. </li>
     * </ol>
     * </p>
     *
     * <i>NOTE that we are *not* adjusting the time to conform exactly to the
     * IETF spec, where timestamp is relative to 15 Oct 1582 00:00:00.00 </i>
     * 
     * @return Universally unique ID (UUID)
     */
    public synchronized ObjectID create() {

        long currentTime = getSystemTime();
        ////System.out.println( "Current time: " + currentTime );

        // Adjust clock sequence if time has "moved backward"
        if ( currentTime < lastTime ) {
            adjustClockSequence();
        }

        // Adjust time to account for multiple generations per tick
        long timeComponent = adjustTimeComponent(currentTime);

        // ---------------------
        // Most Significant Part
        // ---------------------

        // Time bits, least to most significant moving right to left 
        // v = version, z = time high, y = time mid, x = time low
        // Bits:
        // xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx yyyyyyyy yyyyyyyy vvvvzzzz zzzzzzzz 
        // Bytes:
        //    7        6        5        4        3        2        1        0

        // Use only the rightmost 32 bits (bytes 0-3) for low order time
        long timeLow  = timeComponent & 0x00000000ffffffffL;

        // Shift leftmost 32 bits  to lowest order part of long, then mask
        // off the last 16 bits (bytes 4-5), leaving 4 bits for version
        long timeMid  = (timeComponent >> 32) & 0x000000000000ffffL;

        // Shift "high" bytes (6-7) to lowest order part of long, then mask
        // off 12 bits
        long timeHigh = (timeComponent >> 48) & 0x0000000000003fffL;

        // Multiplex the version number into the high time field
        long timeHighAndVersion = timeHigh |= (UUID_VERSION << 12);

        // Place parts into the appropriate places
        long mostSig = (timeLow << 32) | (timeMid << 16) | timeHighAndVersion;

        // ----------------------
        // Least Significant Part
        // ----------------------

        // Time bits, least to most significant moving right to left 
        // v = variant ('10'), c = clock sequence, n = node (random number)
        // Bits:
        // vvcccccc cccccccc nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn 
        // Bytes:
        //    7        6        5        4        3        2        1        0

        // 'Or' the least significant 14 bits of the clock sequence number into
        // the long, and set the variant bits to '10' in the 2 highest bits
        long clockSeqAndReserved = (clockSeq & 0x3fff) | UUID_VARIANT;

        // Place parts into the appropriate places
        long leastSig = (clockSeqAndReserved << 48) | nodeComponent;

        // Increment number of ID's generated by this factory instance so far
        ++counter;

        return new UUID(mostSig, leastSig);
    }

    /**
     * <p>This method updates the "node" component of the UUID.  This is called
     * at construction time.  It may also be called if it is determined that
     * the generated number may no longer be valid for some reason. </p>
     *
     * <p>This method sets the <code>nodeComponent</code> field. </p>
     */
    private synchronized void initNodeComponent() {
        //  48-bit buffer for use with rng
        byte[] randBuf = new byte[N_NODE_BYTES];

        // Load up the 48-bit (6-byte) buffer with a random number
        rng.nextBytes(randBuf);

        nodeComponent = 0L;
        for (int i = 0; i < N_NODE_BYTES; i++) {
            // Shift current state of this part by 1 byte, then 'or' in the
            // lower 8 bits of the random number...
            nodeComponent = (nodeComponent << 8) | (randBuf[i] & 0xff);
        }

        // Set the highest order bit to '1'; this is the multicast bit for IEEE
        // node addresses, and so the randomly generated field won't conflict
        nodeComponent |= 0x800000000000L;
    }

    /**
     * <p>This method initializes the clock sequence.  This uses the same random
     * number generator as used to set the "node" bytes. </p>
     *
     * <p>This method sets the <code>clockSeq</code> field. </p>
     */
    private synchronized void initClockSequence() {
    
        byte[] clockBuf = new byte[2];   // 2 bytes, 16 bits -- we only need 14 bits

        // Load up the 16-bit (2-byte) buffer with a random number
        rng.nextBytes(clockBuf);

        // 8 bits + 6 bits
        clockSeq = (clockBuf[0] & 0xff) << 8 | (clockBuf[1] & 0x3f);
    }

    /**
     * <p>Adjusts the clock sequence if the current time is not greater than 
     * or equal to the last time. </p>
     *
     * <p>This method updates the <code>clockSeq</code> field. </p>
     */
    private synchronized void adjustClockSequence() {
        clockSeq = (clockSeq + 1) % (CLOCK_SEQ_LAST + 1);
        if ( clockSeq == 0 ) {
            clockSeq = 1;
        }
    }

    /**
     * <p>Adjusts system time to yield the "time component" for generating a UUID.
     * The time component is supposed to be the universal coordinated time (UTC),
     * in nanoseconds.  So, the Java system time, adjusted to GMT, is multipled
     * by 1e6 to convert milliseconds to nanoseconds. </p>
     *
     * <p>This method updates the <code>lastTime</code> field if the local
     * system time is different than the last system time. </p>
     *
     * <p>This method may also update the system time in the case where the 
     * maximum number of UUID's have been generated for the current time tick,
     * and we have to "spin" until the next tick.  The last time is the standard
     * Java system time obtained from the last call to <code>System.currentTimeMillis
     * </code>.  Given nanosecond resolution, this is extremely unlikely. <p>
     *
     * <p>This time should be adjusted to be relative to Oct 15, 1582 00:00:00.00
     * to adhere to the IETF spec, but the raw Java time (relative to Jan 1, 1970)
     * is used instead. </p>
     *
     * <h3>References</h3>
     * <p>
     * <ul>
     *    <li><a href=http://tycho.usno.navy.mil>U.S. Naval Observatory Time</a>
     *    has infomration about UTC and daylight savings time.</li>
     * </ul>
     * </p>
     *
     * @param systemTime The value of the system clock most recently retrieved from
     *        the system
     * @return The system time in nanoseconds, adjusted to account for time zone
     *         and daylight savings time, incremented if multiple UUIDs are
     *         being generated for the current tick; the actual system time is
     *         updated from the system if the maximum number of UUID's were
     *         already generated for the original time (very unlikely given
     *         the nanosecond clock resolution).
     */
    private synchronized long adjustTimeComponent( long systemTime ) {

        while ( true ) {
            if ( systemTime != lastTime ) {
                uuidsThisTick = 0;
                lastTime = systemTime;
                break;  // Break out of loop
            }
            if ( uuidsThisTick < maxUuidsPerTick ) {
                ++uuidsThisTick;
                break;  // break out of loop
            }
            // Trying to generate too many IDs for clock speed; spin til next tick
            systemTime = System.currentTimeMillis();
        }

        // Adjust system time to be in nanoseconds, and to account for # UUIDs
        // this tick
        return (systemTime*MILLI_TO_NANO) + uuidsThisTick;

        // This is also where we'd adjust to Oct 15, 1582 if we wanted to...
        // return (systemTime*MILLI_TO_NANO) + 12219292800000L ??? (*1000?) + uuidsThisTick;
    }

    /**
     * <p>Returns the system time, in terms of universal coordinated time (UTC).
     * This requires adjustments to the local time to account for the time zone
     * and for the possibility of a daylight savings time adjustment. </p>
     *
     * <p> This method is provided to encapsulate any adjustments to the time, and 
     * also to allow for a single place to make changes for testing. </p>
     *
     * @return The current system time
     */
    private long getSystemTime() {
        //return 1L;

        long time = System.currentTimeMillis();

        // Adjustment for time zone (not accounting for daylight savings)
        // This is value to *add* to UTC to get local, so subtract from local
        // for UTC
        int timeZoneOffset = calendar.get( Calendar.ZONE_OFFSET );

        // Adjustment in milliseconds for daylight savings time
        int dstOffset = calendar.get( Calendar.DST_OFFSET );


        // IS THIS VALUE *ALWAYS* NONZERO, OR ONLY DURING DST???

       
        // Have to add in daylight savings offset in everytime, in case it 
        // changes while we're running
        time -= (timeZoneOffset + dstOffset);

        return time;
    }

    /**
     * Returns an estimate of the system's clock resolution.
     * @return Clock resolution in milliseconds
     */
    private static long getClockResolution() {
        long t1 = System.currentTimeMillis();
        long t2 = System.currentTimeMillis();
        long delta = t2 - t1;
        long resolution = Long.MAX_VALUE;
        t1 = t2;
        int k = 0;
        do {
            t2 = System.currentTimeMillis();
            delta = t2 - t1;
            if ( (delta > 0) & (delta < resolution) ) { resolution = delta; }
            t1 = t2;
            ++k;
        } while ( resolution == Long.MAX_VALUE || k < 1000 );
        return resolution;
    }

}


