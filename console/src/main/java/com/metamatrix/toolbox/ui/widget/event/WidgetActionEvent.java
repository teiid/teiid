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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.event;

// System imports
import java.awt.event.ActionEvent;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class WidgetActionEvent extends ActionEvent {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final int TIMEOUT = 5000;    // ms

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    private boolean isProcessing = false;
    private boolean isDestroyed = false;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public WidgetActionEvent(final Object source, final String command) {
        this(source, command, 0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public WidgetActionEvent(final Object source, final String command, final int modifiers) {
        super(source, ACTION_PERFORMED, command, modifiers);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void destroy() {
        this.isDestroyed = true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public boolean isDestroyed() {
        return isDestroyed;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public boolean isProcessing() {
        return isProcessing;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public synchronized void setProcessing(final boolean isProcessing) {
        this.isProcessing = isProcessing;
        notifyAll();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public synchronized void waitWhileProcessing() {
        while (isProcessing) {
            try {
                wait(TIMEOUT);
            } catch (final InterruptedException ignored) {}
        }
    }
}
