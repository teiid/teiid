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

package com.metamatrix.common.log.config;

import java.io.Serializable;
import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.log.MessageLevel;

/**
 * Basic, default implementation of the LogConfiguration interface.  This
 * class contains the <code>int</code> logging level, as well as a Collection
 * of String discarded contexts (logging contexts which will <i>not</i> be
 * logged.)  This class also contains static final String constants
 * specifying the two property names whose values completely describe a
 * LogConfiguration, plus static utility methods for converting a
 * LogConfiguration object to and from a Properties object representation.
 */
public class BasicLogConfiguration implements LogConfiguration, Serializable {

    //************************************************************
    // static constants, members, methods
    //************************************************************

    /**
     * The name of the System property that contains the message level for the LogManager.
     * This is an optional property that defaults to '3'.
     */
    public static final String LOG_LEVEL_PROPERTY_NAME   = "metamatrix.log"; //$NON-NLS-1$

    /**
     * <p>The name of the System property that contains the set of comma-separated
     * context names for messages <i>not</i> to be recorded.  A message context is simply
     * some string that identifies something about the component that generates
     * the message.  The value for the contexts is application specific.
     * </p><p>
     * This is an optional property that defaults to no contexts (i.e., messages
     * with any context are recorded).</p>
     */
    public static final String LOG_CONTEXT_PROPERTY_NAME = "metamatrix.log.contexts"; //$NON-NLS-1$

    /**
     * This String should separate each of the contexts in the String value
     * for the property {@link #LOG_CONTEXT_PROPERTY_NAME}.  For example,
     * if this delimiter were a comma, the value for the property might
     * be something like "CONFIG,QUERY,CONFIGURATION_ADAPTER".
     */
    public static final String CONTEXT_DELIMETER = ","; //$NON-NLS-1$

    /**
     * <p>
     * Creates a non-null LogConfiguration from the given properties.  A
     * LogConfiguration with default behavior (using default, no-arg
     * {@link #BasicLogConfiguration() constructor}) is created if one or more
     * of the two necessary property values are missing, or if the
     * Properties parameter is null.</p>
     *
     * <p>The two properties are
     * {@link #LOG_LEVEL_PROPERTY_NAME} and
     * {@link #LOG_CONTEXT_PROPERTY_NAME}</p>
     *
     * @param props Properties which should have values for the properties
     * {@link #LOG_LEVEL_PROPERTY_NAME} and
     * {@link #LOG_CONTEXT_PROPERTY_NAME}
     * @return LogConfiguration object representing the property values
     * @throws LogConfigurationException this currently <i>is not</i> being
     * thrown, but remains in the method signature because it is very
     * possible the implementation of this method may change.  It should
     * be noted that if any unexpected error happens (such as an invalid
     * property value) a default LogConfiguration is created instead of
     * what may be expected.
     */
    public static LogConfiguration createLogConfiguration( Properties props ) throws LogConfigurationException {
        if ( props == null ) {
            return new BasicLogConfiguration();
        }
        LogConfiguration result = null;

        // Create a configuration with the specified level ...
        String logValue = props.getProperty(LOG_LEVEL_PROPERTY_NAME);
        if ( logValue != null && logValue.trim().length() > 0 ) {
            try {
                int logLevel = Integer.parseInt(logValue);
                result = new BasicLogConfiguration(logLevel);
            } catch ( NumberFormatException e ) {
            }
        }
        if ( result == null ) {
            result = new BasicLogConfiguration();
        }

        // Get the contexts ...
        String contextValues = props.getProperty(LOG_CONTEXT_PROPERTY_NAME);
        if ( contextValues != null ) {
            StringTokenizer tokenizer = new StringTokenizer(contextValues,CONTEXT_DELIMETER);
            while ( tokenizer.hasMoreElements() ) {
                result.discardContext(tokenizer.nextElement().toString());
            }
        }        	
        return result;
    }

    /**
     * <p>Creates a Properties object containing the two name/value pairs which
     * define a LogConfiguration, parsed from the given LogConfiguration
     * object.</p>
     *
     * <p>The two properties are
     * {@link #LOG_LEVEL_PROPERTY_NAME} and
     * {@link #LOG_CONTEXT_PROPERTY_NAME}</p>
     *
     * @param logConfiguration LogConfiguration which you want a Properties
     * object representation of.  This parameter may be <code>null</code>,
     * in which case an empty Properties object will be returned.
     * @return Properties object representing LogConfiguration parameter.
     * An empty Properties object is return if the parameter is <code>null</code>.
     * Otherwise, the Properties object will have a value for the property
     * {@link #LOG_LEVEL_PROPERTY_NAME}, and <i>may</i> have a value for
     * the property {@link #LOG_CONTEXT_PROPERTY_NAME}, if the parameter
     * logConfiguration had any discarded log contexts in it.
     * @throws LogConfigurationException this currently <i>is not</i> being
     * thrown, but remains in the method signature because it is very
     * possible the implementation of this method may change.
     */
    public static Properties getLogConfigurationProperties(LogConfiguration logConfiguration)
    throws LogConfigurationException{
        if (logConfiguration == null){
            return new Properties();
        }

        String logLevel = String.valueOf(logConfiguration.getMessageLevel());

        String contextString = null;
        Collection discards = logConfiguration.getDiscardedContexts();
        if (discards.size() != 0){
            StringBuffer contextValues = new StringBuffer();
            Iterator discardedContexts = logConfiguration.getDiscardedContexts().iterator();
            while (discardedContexts.hasNext()){
                if (contextValues.length()!=0){
                    contextValues.append(CONTEXT_DELIMETER);
                }
                contextValues.append((String)discardedContexts.next());

            }
            contextString = contextValues.toString();
        }

        Properties props = new Properties();
        if (logLevel != null){
            props.setProperty(LOG_LEVEL_PROPERTY_NAME, logLevel);
        }
        if (contextString != null){
            props.setProperty(LOG_CONTEXT_PROPERTY_NAME, contextString);
        }
        return props;
    }

    //************************************************************
    // instance members, methods
    //************************************************************

    private Set discardedContexts = null;
    private Set unmodifiableContexts = null;
    private int msgLevel;

	public BasicLogConfiguration( Collection contexts, int msgLevel ) {
        if ( ! MessageLevel.isMessageLevelValid(msgLevel) ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0014,
            		new Object[] {String.valueOf(msgLevel), String.valueOf(MessageLevel.getValidLowerMessageLevel()), String.valueOf(MessageLevel.getValidUpperMessageLevel())}));
        }
        this.msgLevel = msgLevel;
        if ( contexts != null ) {
            this.discardedContexts = new HashSet(contexts);
        } else {
            this.discardedContexts = new HashSet();
        }
        this.unmodifiableContexts = Collections.unmodifiableSet(this.discardedContexts);
	}

    public BasicLogConfiguration() {
        this.msgLevel = MessageLevel.DEFAULT_MESSAGE_LEVEL;
        this.discardedContexts = new HashSet();
        this.unmodifiableContexts = Collections.unmodifiableSet(this.discardedContexts);
	}

    public BasicLogConfiguration( int msgLevel ) {
        this(null,msgLevel);
	}

    public BasicLogConfiguration(LogConfiguration currentLogConfig) {
        this.msgLevel = currentLogConfig.getMessageLevel();
        this.discardedContexts = new HashSet(currentLogConfig.getDiscardedContexts());
        this.unmodifiableContexts = Collections.unmodifiableSet(this.discardedContexts);
    }

    public boolean isContextDiscarded( String context ) {
		boolean discarded = ((context != null) && 
    			this.discardedContexts.contains(context));
    	return discarded;
    }

    public boolean isLevelDiscarded( int level ) {
        return ( level > msgLevel );
    }

    /**
     * Obtain the set of contexts for messages that are to be discarded.
     * If this method returns an empty set, then messages in all contexts
     * are being recorded; if not empty, then messages with a context in the
     * returned set are discarded and messages for all other contexts recorded.
     * @return the set of contexts for messages that are to be discarded
     */
    public Set getDiscardedContexts() {
        return this.unmodifiableContexts;
    }

    /**
     * Specify that messages with the input context should be discarded
     * and not recorded.
     * @param context the context to add to the set; this method does nothing
     * if the reference is null
     */
    public void discardContext( String context ) {
        if ( context != null ) {
            this.discardedContexts.add(context);
        }
        this.unmodifiableContexts = Collections.unmodifiableSet(
        		this.discardedContexts);
    }

    /**
	 * Get the level of detail of messages that are currently being recorded.
	 * @return the level of detail
	 */
    public int getMessageLevel() {
        return msgLevel;
    }

    /**
     * Compares this object to another. If the specified object is an instance of
     * the MetadataID class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).
     * Note:  this method <i>is</i> consistent with <code>equals()</code>, meaning
     * that <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        if ( obj == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.MISC_ERR_0001, "LogConfiguration")); //$NON-NLS-1$
        }
        LogConfiguration that = (LogConfiguration) obj;     // May throw ClassCastException

        // Check the message level first ...
        int diff = this.getMessageLevel() - that.getMessageLevel();
        if ( diff != 0 ) {
            return diff;
        }

        // Check the contexts ...
        boolean sizesMatch = this.getDiscardedContexts().size() == that.getDiscardedContexts().size();
        boolean thisContainsThat = this.getDiscardedContexts().containsAll( that.getDiscardedContexts() );
        if ( thisContainsThat ) {
            if ( sizesMatch ) {
                return 0;   // they are equal
            }
            return 1;   // this has all of that plus more
        }
        return -1;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof LogConfiguration ) {
            LogConfiguration that = (LogConfiguration) obj;

            // Check the message level first ...
            int diff = this.getMessageLevel() - that.getMessageLevel();
            if ( diff != 0 ) {
                return false;
            }

            // Check the contexts ...
            boolean sizesMatch = this.getDiscardedContexts().size() == that.getDiscardedContexts().size();
            boolean thisContainsThat = this.getDiscardedContexts().containsAll( that.getDiscardedContexts() );
            if ( thisContainsThat && sizesMatch ) {
                return true;    // they are equal
            }
        }

        // Otherwise not equal ...
        return false;
    }

	/**
	 * String representation of logging configuration.
	 * @return String representation
	 */
	public String toString() {
		StringBuffer str = new StringBuffer("LogConfiguration: {"); //$NON-NLS-1$
        str.append("Log Level: " + MessageLevel.getLabelForLevel(msgLevel) ); //$NON-NLS-1$
		str.append("; DiscardedContexts["); //$NON-NLS-1$
        Iterator iter = this.discardedContexts.iterator();
        if ( iter.hasNext() ) {
			str.append(iter.next().toString());
        }
        while ( iter.hasNext() ) {
			str.append(',');
			str.append(iter.next().toString());
        }
		str.append("]}"); //$NON-NLS-1$
		return str.toString();
	}

    public Object clone() {
        return new BasicLogConfiguration(this.discardedContexts,this.msgLevel);
    }

    /**
     * Direct the log configuration to record all known logging contexts.
     */
    public void recordAllContexts() {
        // TODO: Map contexts to PlatformLog somehow
        //throw new UnsupportedOperationException("This method is not implemented."); //$NON-NLS-1$
    }

    /**
     * Direct the log configuration to discard the given contexts and
     * not record them.
     * @param contexts the collection of contexts that should be discarded.
     */
    public void discardContexts(Collection contexts) {
		for (Iterator it=contexts.iterator(); it.hasNext(); ) {
            String c = (String) it.next();
            if (!discardedContexts.contains(c)) {
                discardedContexts.add(c);
            }
        }
        this.unmodifiableContexts = Collections.unmodifiableSet(
        		this.discardedContexts);
    }

    /**
     * Direct the log configuration to record only these contexts.
     * @param contexts the contexts that should be recorded.
     */
    public void recordContexts(Collection contexts) {
    	for (Iterator it=contexts.iterator(); it.hasNext(); ) {
            String c = (String) it.next();
            if (discardedContexts.contains(c)) {
                discardedContexts.remove(c);
            }
        }
        this.unmodifiableContexts = Collections.unmodifiableSet(
        		this.discardedContexts);
	}

    /**
     * Direct the log configuration to record messages of the given level
     * or above.
     * @param level the lowest level to record.
     */
    public void setMessageLevel(int level) {
		this.msgLevel = level;
    }
}
