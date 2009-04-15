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

package com.metamatrix.query.optimizer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.lang.Command;

/**
 * A tree node object used to hold state during command planning and optimizing.
 */
public class CommandTreeNode {
	
	/** The command type is a relational command */
	public static final int TYPE_RELATIONAL_COMMAND = 2;
	
	/** The command type is an XML query */
	public static final int TYPE_XML_COMMAND = 3;
	
	/** The command type is a procedural command */
	public static final int TYPE_PROCEDURAL_COMMAND = 4;

    /** The command type is an XQuery command */
    public static final int TYPE_XQUERY_COMMAND = 5;
    
    /** The command type is a batched update command. */
    public static final int TYPE_BATCHED_UPDATE_COMMAND = 6;

    /** The command type is a dynamic command. */
    public static final int TYPE_DYNAMIC_COMMAND = 7;
    
    /** The command type is a prepared batched update command. */
    public static final int TYPE_PREPARED_BATCH_UPDATE_COMMAND = 8;
    
	/** The command, or subcommand, stored at this node */
	private Command command;

	/** The type of node, as defined by class constants */
	private int commandType;
	
	/** The Planner-specific canonical plan object */
	private Object canonicalPlan;
	
	/** Planner-specific node properties, as defined by each CommandPlanner implementation */
	private Map<Integer, Object> properties = new HashMap<Integer, Object>();
	
	/** The parent of this node, null if root. */
	private CommandTreeNode parent;

	/** Child nodes */
	private LinkedList<CommandTreeNode> children = new LinkedList<CommandTreeNode>();	

	// ====================================================
	// API
	// ====================================================
	
	/**
	 * Get the type of this command
	 * @return int one of three type constants defined in this class
	 * @see #TYPE_RELATIONAL_COMMAND
	 * @see #TYPE_XML_COMMAND
	 * @see #TYPE_PROCEDURAL_COMMAND
	 */
	public int getCommandType() {
		return commandType;
	}	

	/**
	 * Set the type of this command
	 * @param commandType one of three type constants defined in this class
	 * @see #TYPE_RELATIONAL_COMMAND
	 * @see #TYPE_XML_COMMAND
	 * @see #TYPE_PROCEDURAL_COMMAND
	 */
	public void setCommandType(int commandType) {
		this.commandType = commandType;
	}	
	
	/**
	 * Returns the Command object.
	 * @return Command
	 */
	public Command getCommand() {
		return command;
	}

	/**
	 * Sets the Command object.
	 * @param command The command to set
	 */
	public void setCommand(Command command) {
		this.command = command;
	}
	
	/**
	 * Retrieve the planner-specific canonical plan
	 * @return Object planner-specific canonical plan
	 */
	public Object getCanonicalPlan(){
		return this.canonicalPlan;
	}
	
	/**
	 * Set the planner-specific canonical plan
	 * @param canonicalPlan Object
	 */
	public void setCanonicalPlan(Object canonicalPlan){
		this.canonicalPlan = canonicalPlan;
	}
	
	/**
	 * Returns the planner-specific ProcessorPlan implementation.
	 * This ProcessorPlan may be needed by the planner of this
	 * node's parent node - in other words a planner may need 
	 * access to the ProcessorPlans of subcommands or other nested
	 * commands.
	 * @return ProcessorPlan at this node, or null if none
	 */
	public ProcessorPlan getProcessorPlan() {
		return this.command.getProcessorPlan();
	}

	/**
	 * Sets the planner-specific ProcessorPlan implementation for the
	 * Command represented by this node.
	 * @param processorPlan The processorPlan to set
	 */
	public void setProcessorPlan(ProcessorPlan processorPlan) {
		this.command.setProcessorPlan(processorPlan);
	}	

	// ====================================================
	// Tree Stuff
	// ====================================================

	public CommandTreeNode getParent() {
		return parent;
	}

	public void setParent(CommandTreeNode parent) {
		this.parent = parent;
	}

	public List<CommandTreeNode> getChildren() {
		return this.children;
	}
	
	public int getChildCount() {
		return this.children.size();
	}
		
	public CommandTreeNode getFirstChild() {
		return this.children.getFirst();
	}
	
	public CommandTreeNode getLastChild() {
		return this.children.getLast();
	}
		
	public void addFirstChild(CommandTreeNode child) {
		this.children.addFirst(child);
	}
	
	public void addLastChild(CommandTreeNode child) {
		this.children.addLast(child);
	}
	
	public void addChildren(List<CommandTreeNode> otherChildren) {
		this.children.addAll(otherChildren);
	}
	
	public boolean hasChild(CommandTreeNode child) {
		return this.children.contains(child);
	}
		
	public boolean removeChild(CommandTreeNode child) {
		return this.children.remove(child);
	}		

	// ====================================================
	// Properties
	// ====================================================

	/**
	 * Retrieve one of the {@link CommandPlanner}-specific properties 
	 * stored at this node, or null if no property of the given key
	 * exists.
	 * @param propertyID key of the property
	 * @return Object property value
	 */
	public Object getProperty(Integer propertyID) {
		return properties.get(propertyID);
	}
	
	/**
	 * Set a {@link CommandPlanner}-specific property.  Each planner may have
	 * conflicting property keys, so an object of this Class should only be
	 * used for one CommandPlanner at a time.
	 * @param propertyID planner-specific property key
	 * @param value property value
	 */
	public void setProperty(Integer propertyID, Object value) {
		properties.put(propertyID, value);
	}

	// ====================================================
	// Overriden Object Methods
	// ====================================================

	/**
	 * Print CommandTreeNode structure starting at this node
	 * @return String representing this node and all children under this node
	 */
	public String toString() {
		StringBuffer str = new StringBuffer();
		getRecursiveString(str, 0);
		return str.toString();
	}

	// ====================================================
	// Utility
	// ====================================================

	/**
	 * Just print single node to string instead of node+recursive plan.
	 * @return String representing just this node
	 */
	public String nodeToString() {
		StringBuffer str = new StringBuffer();
		getNodeString(str);
		return str.toString();
	}
	
	// Define a single tab
	private static final String TAB = "  "; //$NON-NLS-1$

	
	private void setTab(StringBuffer str, int tabStop) {
		for(int i=0; i<tabStop; i++) {
			str.append(TAB);
		}			
	}
	
	private void getRecursiveString(StringBuffer str, int tabLevel) {
		setTab(str, tabLevel);
		getNodeString(str);
		str.append("\n");  //$NON-NLS-1$
		getCanonicalPlanString(str);
		
		// Recursively add children at one greater tab level
		for (CommandTreeNode child : this.children) {
			child.getRecursiveString(str, tabLevel+1);
		}		
	}

	private void getNodeString(StringBuffer str) {
		str.append("(type="); //$NON-NLS-1$
		str.append(CommandTreeNode.getNodeTypeString(this.commandType));
		str.append(", command="); //$NON-NLS-1$
		str.append(this.command);
		if(this.properties != null) {
			str.append(", props="); //$NON-NLS-1$
			str.append(this.properties);
		}
		str.append(")");  //$NON-NLS-1$
	}	

	private void getCanonicalPlanString(StringBuffer str) {
		if(this.canonicalPlan != null) {
			str.append("canonical plan:\n"); //$NON-NLS-1$
			str.append(this.canonicalPlan);
			str.append("\n");  //$NON-NLS-1$
		}		
	}
	
	/** 
	 * Convert a type code into a type string. 
	 * @param type Type code, as defined in class constants
	 * @return String representation for code
	 */
	private static final String getNodeTypeString(int type) {
		switch(type) {
			case TYPE_PROCEDURAL_COMMAND:		return "Procedural"; //$NON-NLS-1$
			case TYPE_RELATIONAL_COMMAND:		return "Relational"; //$NON-NLS-1$
			case TYPE_XML_COMMAND:				return "XML"; //$NON-NLS-1$
            case TYPE_XQUERY_COMMAND:           return "XQuery"; //$NON-NLS-1$
            case TYPE_BATCHED_UPDATE_COMMAND:   return "BatchedUpdate"; //$NON-NLS-1$
			default:							return "Unknown: " + type; //$NON-NLS-1$
		}
	}	

}
