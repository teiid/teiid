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

package com.metamatrix.console.models;

import java.util.EventObject;

public class ModelChangedEvent extends EventObject{

    public final static String NO_ARG = "No arg"; //$NON-NLS-1$

    public final static String NEW_OP           = "New"; //$NON-NLS-1$
    public final static String CHANGE_OP        = "Change"; //$NON-NLS-1$
    public final static String DELETE_OP        = "Delete"; //$NON-NLS-1$
    public final static String ADD_TO_OP        = "Add To"; //$NON-NLS-1$
    public final static String REMOVE_FROM_OP   = "Remove From"; //$NON-NLS-1$
    public final static String TERMINATE_OP     = "Terminate"; //$NON-NLS-1$
    public final static String STALE            = "Stale"; //$NON-NLS-1$


    private String message;
    private Object arg;

    public ModelChangedEvent(Object source, String message){
        this(source, message, NO_ARG);
    }

    public ModelChangedEvent(Object source, String message, Object arg){
        super(source);
        setMessage(message);
        setArg(arg);
    }

    public String getMessage(){
        return message;
    }

    private void setMessage(String message){
        this.message = message;
    }

    public Object getArg(){
        return arg;
    }

    private void setArg(Object arg){
        this.arg = arg;
    }

    public String toString(){
        StringBuffer buffy = new StringBuffer("ModelChangedEvent\n"); //$NON-NLS-1$
        buffy.append("Source: " + super.getSource().toString()); //$NON-NLS-1$
        buffy.append("\nMessage: " + getMessage()); //$NON-NLS-1$
        return buffy.toString();
    }


    public static boolean isChangeAction( String sAction )
    {
        // Test for change action.  Currently this means everything but STALE.
	  	if(     sAction.equals( NEW_OP )
	  		||	sAction.equals( CHANGE_OP )
	  	 	||	sAction.equals( DELETE_OP )
	  	 	||	sAction.equals( ADD_TO_OP )
	  	 	||	sAction.equals( REMOVE_FROM_OP )
	  	 	||	sAction.equals( TERMINATE_OP )
	  	  ) {
			return true;
        }
	  	return false;

    }
}
