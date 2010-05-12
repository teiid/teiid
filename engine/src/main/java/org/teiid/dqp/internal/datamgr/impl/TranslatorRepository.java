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

package org.teiid.dqp.internal.datamgr.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.translator.ExecutionFactory;


public class TranslatorRepository implements Serializable{
	
	private static final long serialVersionUID = -1212280886010974273L;
	private Map<String, ExecutionFactory> translatorRepo = new ConcurrentHashMap<String, ExecutionFactory>();

	public void addTranslator(String name, ExecutionFactory t) {
//		try {
//			InitialContext ic = new InitialContext();
//			ic.bind(name, t);
//		} catch (NamingException e) {
//			LogManager.logError(LogConstants.CTX_RUNTIME, DQPPlugin.Util.getString("failed_to_bind_translator", name)); //$NON-NLS-1$
//		}
		this.translatorRepo.put(name, t);
	}
	
	public ExecutionFactory getTranslator(String name) {
		return this.translatorRepo.get(name);
	}
	
	public ExecutionFactory removeTranslator(String name) {
//		try {
//			InitialContext ic = new InitialContext();
//			ic.unbind(name);
//		} catch (NamingException e) {
//			LogManager.logError(LogConstants.CTX_RUNTIME, DQPPlugin.Util.getString("failed_to_unbind_translator", name)); //$NON-NLS-1$
//		}		
		return this.translatorRepo.remove(name);
	}	
}