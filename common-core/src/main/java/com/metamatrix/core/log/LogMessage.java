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

package com.metamatrix.core.log;

import java.io.Serializable;


public class LogMessage implements Serializable{

	private String context;
    private int level;
	private Object[] msgParts;
	private Throwable exception;
	private long timestamp;
    private String threadName;
    private int errorCode;
    
    public LogMessage() {
    }

	public LogMessage(String context, int level, Object[] msgParts ) {
	    this.context = context;
	    this.level = level;
	    this.msgParts = convertMsgParts(msgParts);
	    this.timestamp = System.currentTimeMillis();
        this.threadName = Thread.currentThread().getName();
	}

	public LogMessage(String context, int level, Throwable e, Object[] msgParts) {
        this(context, level, msgParts);
	    this.exception = e;
	}
    
    public LogMessage(String context, int level, Throwable e, Object[] msgParts, int errorCode) {
        this(context, level, msgParts);
        this.exception = e;
        this.errorCode = errorCode;
    }

	
    public LogMessage(String context, int level, Throwable e, Object[] msgParts, String threadname ) {
        this(context, level, msgParts);
        this.exception = e;
        this.threadName = threadname;
    }    

    public String getContext() {
        return this.context;
    }

    public int getLevel() {
        return this.level;
    }

    public String getThreadName() {
        return this.threadName;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

	public Throwable getException() {
		return this.exception;
	}



    //do a toString() to the object array before adding to the worker
	//to make sure the current state is recorded
	private Object[] convertMsgParts(Object[] oriMsgParts){
		if(oriMsgParts == null){
			return oriMsgParts;
		}
		for(int i=0; i<oriMsgParts.length; i++){
			if(oriMsgParts[i] != null){
				oriMsgParts[i] = oriMsgParts[i].toString();
			}
		}
		return oriMsgParts;
	}

	public String getText() {
		StringBuffer text = null;
		if(msgParts != null) {
			text = new StringBuffer();
		    for(int i=0; i<msgParts.length; i++) {
		        if (i>0) text.append(" "); //$NON-NLS-1$
                Object omsg = msgParts[i];
                if ( omsg != null ) {
		            text.append(omsg.toString());
                }
		    }
		}

        if (text == null) {
        	return "NULL"; //$NON-NLS-1$
        } else {
        	return text.toString();
        }			
	}
    
    /** 
     * @return Returns the errorCode.
     */
    public int getErrorCode() {
        return this.errorCode;
    }
}
