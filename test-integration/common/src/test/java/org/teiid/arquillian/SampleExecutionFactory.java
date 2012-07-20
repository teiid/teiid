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

package org.teiid.arquillian;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.loopback.LoopbackExecution;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@org.teiid.translator.Translator(name = "loopy")
@SuppressWarnings("nls")
public class SampleExecutionFactory extends LoopbackExecutionFactory {
	static int metadataloaded = 0; // use of static is bad, but we instantiate a separate translator for each vdb load
	
	public SampleExecutionFactory() {
		setSupportsSelectDistinct(true);
		setWaitTime(10);
		setRowCount(200);
		setSourceRequiredForMetadata(false);
		setSourceRequired(false);
	}
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, Object conn) throws TranslatorException {
		super.getMetadata(metadataFactory, conn);
		metadataloaded++;
		
		Table t = metadataFactory.addTable("Matadata");
		metadataFactory.addColumn("execCount", "integer", t);
	}
	
	@Override
	public boolean isSourceRequired() {
		return false;
	}	
	
    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
    		throws TranslatorException {
    	if (command.toString().equals("SELECT g_0.execCount FROM Matadata AS g_0")) { //$NON-NLS-1$
    		return new ResultSetExecution() {
				boolean served = false;
				@Override
				public void execute() throws TranslatorException {
					
				}
				
				@Override
				public void close() {
					
				}
				
				@Override
				public void cancel() throws TranslatorException {
				}
				
				@Override
				public List<?> next() throws TranslatorException, DataNotAvailableException {
					if (!served) {
						served = true;
						return Arrays.asList(metadataloaded);
					}
					return null;
				}
			};
    	}
        return new LoopbackExecution(command, this);
    }   
	
}