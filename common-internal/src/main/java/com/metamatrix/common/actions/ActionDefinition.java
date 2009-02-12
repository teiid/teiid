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

package com.metamatrix.common.actions;

import java.io.Serializable;
import java.util.Date;

/**
 * This abstract class is the foundation for the set of classes which define the atomic actions for the
 * ModificationAction class.
 * @see ModificationAction
 */
abstract public class ActionDefinition implements Cloneable, Serializable {

    /**
     * The object (or identifier for an object) that is the target of this action.  May not be null and must
     * be serializable.
     */
    private Object target;
    private long creationTime;

    private Object[] arguments;

    private Integer attributeCode;
    private String attributeDescription;

    /**
     * Create a new instance of an action with no target, no attribute definition, and no arguments.
     */
    public ActionDefinition() {
        creationTime = System.currentTimeMillis();
    }

    /**
     * Create a new instance of an action.
     * @param arguments the objects that define the arguments for this action.
     */
    public ActionDefinition( Object[] arguments ) {
        creationTime = System.currentTimeMillis();
        this.arguments = arguments;
    }

    /**
     * Create a new instance of an action definition by specifying the target.
     * @param target the object (or identifier for the object) that is the target of this action.
     * @param arguments the objects that define the arguments for this action.
     */
    public ActionDefinition(Object target, AttributeDefinition attribute, Object[] arguments ) {
        creationTime = System.currentTimeMillis();
        this.target = target;
        if ( attribute != null ) {
            this.attributeCode = new Integer( attribute.getCode() );
            this.attributeDescription = attribute.getLabel();
        }
        this.arguments = arguments;
    }

    /**
     * Create a new instance of an action definition by specifying the target.
     * @param target the object (or identifier for the object) that is the target of this action.
     * @param arguments the objects that define the arguments for this action.
     */
    public ActionDefinition(Object target, AttributeDefinition attribute ) {
        this(target,attribute,null);
    }

    protected ActionDefinition(Object target, Integer code ) {
        this(target,code,null,System.currentTimeMillis());
    }

    protected ActionDefinition(Object target, Integer code, Object[] arguments, long creationTime ) {
        this.creationTime = creationTime;
        this.target = target;
        this.attributeCode = code;
        this.arguments = arguments;
    }

    protected ActionDefinition(Object target, Integer code, Object[] arguments ) {
        this.creationTime = System.currentTimeMillis();
        this.target = target;
        this.attributeCode = code;
        this.arguments = arguments;
    }

    protected ActionDefinition( ActionDefinition rhs ) {
        this.creationTime = rhs.creationTime;
        this.target = rhs.target;
        this.attributeCode = rhs.attributeCode;
        this.arguments = rhs.arguments;
    }

    /**
     * Get the time that this action was created.
     * @return the creation time.
     */
    public Date getCreationTime() {
        return new Date(creationTime);
    }

    /**
     * Get the time as milliseconds that this action was created.
     * @return the creation time as milliseconds.
     */
    public long getCreationTimeMillis() {
        return creationTime;
    }

    /**
     * Get the description (i.e., verb) for this type of action.
     * @return the string description for this type of action.
     */
    public abstract String getActionDescription();

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     */
    public abstract Object clone();

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        return getActionDescription();
    }

    /**
     * Obtain the definition of the action that undoes this action definition.  If a modification action with the
     * returned action definition is applied to the same target (when the state is such as that left by
     * the original modification action), the resulting target will be left in a state that is identical to the target
     * before either action were applied.
     * @return the action definition that undoes this action definition.
     */
    public abstract ActionDefinition getUndoActionDefinition();

	/**
	 * Return whether this definition has a target.
	 * @return true if the target of this object is not null.
	 */
    public final boolean hasTarget(){
    	return ( target != null );
    }
	/**
	 * Return whether this definition has a target.
	 * @return true if the target of this object is not null.
	 */
    public final boolean hasArguments(){
    	return ( arguments != null && arguments.length != 0 );
    }
	/**
	 * Return whether this definition has an attribute definition.
	 * @return true if the target of this object is not null.
	 */
    public final boolean hasAttributeCode(){
        return ( attributeCode != null );
    }
	/**
	 * Return whether this definition has an attribute definition.
	 * @return true if the target of this object is not null.
	 */
    public final String getAttributeDescription(){
        return ( attributeDescription != null ) ? attributeDescription : ""; //$NON-NLS-1$
    }
    /**
     * Get the target object for this action.
     * @return the target object, or null if there is no target defined
     */
    public final Object getTarget() {
        return target;
    }
    /**
     * Get the an attribute definition code for this action.
     * @return the attribute definition code as an Integer, or null if there is no attribute defined.
     */
    public final Integer getAttributeCode() {
        return attributeCode;
    }
    public Object[] getArguments() {
        return arguments;
    }
}





