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

package org.teiid.dqp.internal.process;

import java.util.List;

import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Reference;


class PreparedPlan{
	private ProcessorPlan plan;
	private Command command;
	private List<Reference> refs;
	private AnalysisRecord analysisRecord;
	
	/**
	 * Return the ProcessorPlan.
	 */
	public ProcessorPlan getPlan(){
		return plan;
	}
	
	/**
	 * Return the plan description.
	 */
	public AnalysisRecord getAnalysisRecord(){
		return this.analysisRecord;
	}
	
	/**
	 * Return the Command .
	 */
	public Command getCommand(){
		return command;
	}
	
	/**
	 * Return the list of Reference.
	 */
	public List<Reference> getReferences(){
		return refs;
	}
	
	/**
	 * Set the ProcessorPlan.
	 */
	public void setPlan(ProcessorPlan planValue){
		plan = planValue;
	}
	
	/**
	 * Set the plan description.
	 */
	public void setAnalysisRecord(AnalysisRecord analysisRecord){
        this.analysisRecord = analysisRecord;
	}
	
	/**
	 * Set the Command.
	 */
	public void setCommand(Command commandValue){
		command = commandValue;
	}
	
	/**
	 * Set the list of Reference.
	 */
	public void setReferences(List<Reference> refsValue){
		refs = refsValue;
	}
	
}