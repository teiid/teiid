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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.core.CorePlugin;

public class LogMessage implements Externalizable {

  public static final String VM_NAME = "VMName"; //$NON-NLS-1$
  public static final String HOST_NAME = "HostName"; //$NON-NLS-1$
    
//    public static String VM_NAME = VMNaming.getVMName();
//    public static String HOST_NAME = VMNaming.getLogicalHostName();
//
//    private static final String DEFAULT_VM_NAME = ""; //$NON-NLS-1$

    private static final String NULL_MSG_TEXT = "Null"; //$NON-NLS-1$
//
//    static {
//        if ( VM_NAME == null || VM_NAME.trim().length() == 0 ) {
//            VM_NAME = VMNaming.getVMIDString();
//        }
//        if ( VM_NAME == null || VM_NAME.trim().length() == 0 ) {
//            VM_NAME = DEFAULT_VM_NAME;
//        }
//    }

	private String msgID;

	private String context;
    private int level;
	private Object[] msgParts;
	private Throwable exception;
	private long timestamp;
    private String threadName;
    private String hostName;
    private String vmName;
    private int errorCode;
    
    public LogMessage() {
    }

    public LogMessage(String msgID, String context, int level) {
		this.msgID = msgID;
	    this.context = context;
	    this.level = level;
	    this.msgParts = null;
	    this.timestamp = System.currentTimeMillis();
        this.threadName = Thread.currentThread().getName();
        this.hostName = HOST_NAME;
        this.vmName = VM_NAME;
	}

	public LogMessage(String msgID, String context, int level, Throwable e) {
        this(msgID, context, level);
	    this.exception = e;
	}


	public LogMessage(String msgID, String context, int level, Object[] msgParts ) {
		this.msgID = msgID;
	    this.context = context;
	    this.level = level;
	    this.msgParts = convertMsgParts(msgParts);
	    this.timestamp = System.currentTimeMillis();
        this.threadName = Thread.currentThread().getName();
        this.hostName = HOST_NAME;
        this.vmName = VM_NAME;
	}

	public LogMessage(String msgID, String context, int level, Throwable e, Object[] msgParts ) {
        this(msgID, context, level, msgParts);
	    this.exception = e;
	}

	public LogMessage(String context, int level, Object[] msgParts ) {
		this.msgID= null;
	    this.context = context;
	    this.level = level;
	    this.msgParts = convertMsgParts(msgParts);
	    this.timestamp = System.currentTimeMillis();
        this.threadName = Thread.currentThread().getName();
        this.hostName = HOST_NAME;
        this.vmName = VM_NAME;
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

    public String getVMName() {
        return this.vmName;
    }

    public String getHostName() {
        return this.hostName;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

	public Throwable getException() {
		return this.exception;
	}

	public String getText() {
		String msg = null;

		if (msgID != null) {
			if (msgParts == null) {
				msg = CorePlugin.Util.getString(msgID);
			} else {

				msg = CorePlugin.Util.getString(msgID, msgParts);

			}

		} else {

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
	        	msg = NULL_MSG_TEXT;
	        } else {
	        	msg = text.toString();
	        }


		}

        if (msg == null || msg.length() == 0) {
            msg = NULL_MSG_TEXT;
        }


		return msg;
	}

	// implements Externalizable
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(context);
		out.writeInt(level);
		out.writeObject(msgParts);
		out.writeObject(exception);
		out.writeLong(timestamp);
		out.writeObject(threadName);
		out.writeObject(vmName);
		out.writeInt(this.errorCode);
	}

	// implements Externalizable
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.context = (String) in.readObject();
		this.level = in.readInt();
		this.msgParts = (Object[]) in.readObject();
		this.exception = (Throwable) in.readObject();
		this.timestamp = in.readLong();
		this.threadName = (String) in.readObject();
		this.vmName = (String) in.readObject();
		this.errorCode = in.readInt();
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

    
    /** 
     * @return Returns the errorCode.
     */
    public int getErrorCode() {
        return this.errorCode;
    }
}
