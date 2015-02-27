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

package org.teiid.translator;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.RuntimeMetadata;

/**
 * Demonstrates the delegating translator pattern to add low level logging.
 * 
 * @param <F>
 * @param <C>
 */
@Translator(name="record-logging", description="A translator logging all results returned by translators.")
public class RecordLoggingExecutionFactory<F, C> extends BaseDelegatingExecutionFactory<F, C> {

	private static final class LoggingHandler implements InvocationHandler {
		private final Execution execution;
		private final ExecutionContext executionContext;

		private LoggingHandler(Execution execution, ExecutionContext executionContext) {
			this.execution = execution;
			this.executionContext = executionContext;
		}

		@Override
		public Object invoke(Object arg0, Method arg1, Object[] arg2)
				throws Throwable {
			try {
				Object result = arg1.invoke(execution, arg2);
				if (arg1.getName().equals("next")) { //$NON-NLS-1$
					LogManager.log(MessageLevel.TRACE, LogConstants.CTX_COMMANDLOGGING, executionContext.getRequestId() + " " + result); //$NON-NLS-1$
				} else if (arg1.getName().equals("getUpdateCounts")) { //$NON-NLS-1$
					LogManager.log(MessageLevel.TRACE, LogConstants.CTX_COMMANDLOGGING, executionContext.getRequestId() + " " + Arrays.toString((int[])result)); //$NON-NLS-1$
				} else if (arg1.getName().equals("getOutputParameterValues")) { //$NON-NLS-1$
					LogManager.log(MessageLevel.TRACE, LogConstants.CTX_COMMANDLOGGING, executionContext.getRequestId() + " " + Arrays.toString((Object[])result)); //$NON-NLS-1$
				}
				return result;
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
		}
	}

	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments,
			Command command, ExecutionContext executionContext,
			RuntimeMetadata metadata, C connection) throws TranslatorException {
		final ProcedureExecution execution = getDelegate().createDirectExecution(arguments, command, executionContext,
				metadata, connection);
		return (ProcedureExecution)Proxy.newProxyInstance(RecordLoggingExecutionFactory.class.getClassLoader(), new Class[] {ProcedureExecution.class}, new LoggingHandler(execution, executionContext));
	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		final ProcedureExecution execution = getDelegate().createProcedureExecution(command, executionContext,
				metadata, connection);
		return (ProcedureExecution)Proxy.newProxyInstance(RecordLoggingExecutionFactory.class.getClassLoader(), new Class[] {ProcedureExecution.class}, new LoggingHandler(execution, executionContext));
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		ResultSetExecution execution = getDelegate().createResultSetExecution(command, executionContext, metadata,
				connection);
		return (ResultSetExecution)Proxy.newProxyInstance(RecordLoggingExecutionFactory.class.getClassLoader(), new Class[] {ResultSetExecution.class}, new LoggingHandler(execution, executionContext));
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		UpdateExecution execution = getDelegate().createUpdateExecution(command, executionContext, metadata,
				connection);
		return (UpdateExecution)Proxy.newProxyInstance(RecordLoggingExecutionFactory.class.getClassLoader(), new Class[] {UpdateExecution.class}, new LoggingHandler(execution, executionContext));
	}
	
}
