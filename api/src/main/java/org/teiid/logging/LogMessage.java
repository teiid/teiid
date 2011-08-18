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

package org.teiid.logging;

import java.io.Serializable;


public class LogMessage implements Serializable{

	private static final long serialVersionUID = -134503344442009940L;
	
	private Object[] msgParts;

	public LogMessage(Object[] msgParts) {
        this.msgParts = msgParts;
	}

	public Object[] getMessageParts() {
		return this.msgParts;
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
        } 
        return text.toString();
	}
	
	public String toString() {
		return getText();
	}
}
