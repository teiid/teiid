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

package com.metamatrix.common.config;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.api.exception.MetaMatrixException;

/**
 * <p>This exception is thrown by {@link StartupStateController}, to
 * indicate to a calling MetaMatrixController that the system is
 * not in a state in which initialization can proceed.</p>
 */
public final class StartupStateException extends MetaMatrixException {

    private int startupState;

    private static final String DEFAULT_MESSAGE_BEGIN = "Current startup state "; //$NON-NLS-1$
    private static final String DEFAULT_MESSAGE_MIDDLE = " could not be changed to "; //$NON-NLS-1$

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public StartupStateException() {
        super();
    }
    
    /**
     * Construct this exception with a message, and the system state.
     */
    public StartupStateException( String message, int startupState ) {
        super( message );
        this.startupState = startupState;
    }

    /**
     * Construct this exception with a default message, which will
     * include the current startup state and the desired startup state
     * parameters, explaining that the desired state cannot be reached.
     */
    public StartupStateException( int desiredStartupState, int startupState ) {
        super( generateDefaultMessage(desiredStartupState, startupState) );
        this.startupState = startupState;
    }


    /**
     * Use the constants in {@link StartupStateController} to interpret the
     * system state from the returned int.
     * @return int indicating system startup state
     */
    public int getStartupState(){
        return this.startupState;
    }

    private static final String generateDefaultMessage(int desiredStartupState, int startupState ) {
        StringBuffer s = new StringBuffer(DEFAULT_MESSAGE_BEGIN);
        switch (startupState){
            case StartupStateController.STATE_STOPPED:
                s.append(StartupStateController.STATE_STOPPED_LABEL);
                break;
            case StartupStateController.STATE_STARTING:
                s.append(StartupStateController.STATE_STARTING_LABEL);
                break;
            case StartupStateController.STATE_STARTED:
                s.append(StartupStateController.STATE_STARTED_LABEL);
        }
        s.append(DEFAULT_MESSAGE_MIDDLE);
        switch (desiredStartupState){
            case StartupStateController.STATE_STOPPED:
                s.append(StartupStateController.STATE_STOPPED_LABEL);
                break;
            case StartupStateController.STATE_STARTING:
                s.append(StartupStateController.STATE_STARTING_LABEL);
                break;
            case StartupStateController.STATE_STARTED:
                s.append(StartupStateController.STATE_STARTED_LABEL);
        }
        s.append("."); //$NON-NLS-1$
        return s.toString();
    }

    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        startupState = in.readInt();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(startupState);
    }

}

