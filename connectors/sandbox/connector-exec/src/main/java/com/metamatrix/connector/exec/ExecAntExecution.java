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

package com.metamatrix.connector.exec;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Location;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.ExecTask;
import org.apache.tools.ant.taskdefs.ExecuteStreamHandler;
import org.apache.tools.ant.taskdefs.PumpStreamHandler;
import org.apache.tools.ant.types.Commandline.Argument;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.util.CommonPropertyNames;
import com.metamatrix.common.util.OSPlatformUtil;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.TempDirectory;

/**
 * Represents the execution of a command.
 */
public class ExecAntExecution extends BasicExecution implements ResultSetExecution {

	private static final Random random = new Random(System.currentTimeMillis());

	private static String INSTALL_DIR = ".";//$NON-NLS-1$

	private static final String WIN_EXEC = "win.executable"; //$NON-NLS-1$
	private static final String UNIX_EXEC = "unix.executable"; //$NON-NLS-1$

	private static final String DEFAUTL_WIN_EXEC = "cmd.exe"; //$NON-NLS-1$
	private static final String DEFAUTL_UNIX_EXEC = "/bin/sh"; //$NON-NLS-1$

	// Connector resources
	private ConnectorEnvironment env;
	private List responses = new ArrayList();
	private String execcommand;
	private boolean isWin = false;

	// Execution state
	int[] neededColumns;
	int returnIndex = 0;

	private Process p = null;

	private List exclusionList;
	private IQuery query;

	public ExecAntExecution(IQuery query, ConnectorEnvironment env, RuntimeMetadata metadata, ConnectorLogger logger, List exclusionThese) {
		this.env = env;
		this.query = query;
		if (exclusionThese != null)
			exclusionList = exclusionThese;
		else
			exclusionList = Collections.EMPTY_LIST;
		Properties props = env.getProperties();

		if (OSPlatformUtil.isWindows()) {
			execcommand = props.getProperty(WIN_EXEC, DEFAUTL_WIN_EXEC);
			Assertion.isNotNull(execcommand, WIN_EXEC+ " property was not defined for os type"); //$NON-NLS-1$
			isWin = true;

		} else {
			execcommand = props.getProperty(UNIX_EXEC, DEFAUTL_UNIX_EXEC);
			Assertion.isNotNull(execcommand, UNIX_EXEC + " property was not defined for os type"); //$NON-NLS-1$
		}
	}

	/*
	 * @see com.metamatrix.data.SynchQueryExecution#execute(com.metamatrix.data.language.IQuery,
	 *      int)
	 */
	public void execute()
			throws ConnectorException {

		env.getLogger().logTrace("Exec executing command: " + query); //$NON-NLS-1$
		org.teiid.connector.language.ICriteria crit = query.getWhere();
		if (crit == null)
			throw new ConnectorException(ExecPlugin.Util
					.getString("ExecExecution.Must_have_criteria")); //$NON-NLS-1$
		Map whereVariables = ExecVisitor.getWhereClauseMap(crit);
		if (whereVariables.isEmpty())
			throw new ConnectorException(ExecPlugin.Util
					.getString("ExecExecution.Must_have_criteria")); //$NON-NLS-1$

		String command = ""; //$NON-NLS-1$
		int i = 2;
		for (Iterator it = whereVariables.keySet().iterator(); it.hasNext();) {
			String whereKey = (String) it.next();
			String v = ((String) whereVariables.get(whereKey)).trim();
			isValid(v);
			command += v;
			i++;
		}
		try {
			execute(command);
		} catch (Exception e) {
			env.getLogger().logError("Execution Error", e); //$NON-NLS-1$
			throw new ConnectorException(e);
		}
	}

	private boolean isValid(String command) throws ConnectorException {
		boolean isvalid = true;
		List tokens = StringUtil.getTokens(command.toLowerCase(), " "); //$NON-NLS-1$
		for (Iterator it = tokens.iterator(); it.hasNext();) {
			String v = (String) it.next();
			if (exclusionList.contains(v))
				throw new ConnectorException(ExecPlugin.Util.getString(
						"ExecExecution.Execution_of_statement_not_allowed", v));//$NON-NLS-1$
		}

		return isvalid;
	}

	protected void execute(String command) throws Exception {

		ExecuteMMTask task = createClass();

		env.getLogger().logTrace(
				"Exec: " + execcommand + " command: " + command); //$NON-NLS-1$ //$NON-NLS-2$

		Project p = new Project();
		p.init();
		p.setBasedir(INSTALL_DIR);
		p.setCoreLoader(this.getClass().getClassLoader());

		task.setProject(p);

		task.setDir(new File((INSTALL_DIR)));

		task.setLogError(true);

		task.setFailonerror(false);

		task.setTaskType("MMExecutionTask"); //$NON-NLS-1$
		task.setTaskName("ExecAntExecution"); //$NON-NLS-1$
		task.setExecutable(execcommand);

		Argument a = task.createArg();

		if (isWin) {
			// do not set for unix
			task.setVMLauncher(true);

			a.setValue("/c"); //$NON-NLS-1$
			a = task.createArg();
		}
		a.setLine(command);
		// a.setValue(command);
		a.setProject(p);

		task.setAppend(true);
		task.setLocation(Location.UNKNOWN_LOCATION);

		task.execute();
		task.addToRows(responses);

	}

	private ExecuteMMTask createClass() {

		ExecuteMMTask rep = new ExecuteMMTask();

		return rep;
	}

	@Override
	public List next() throws ConnectorException, DataNotAvailableException {
		if (returnIndex < responses.size()) {
			return (List)responses.get(returnIndex++);
		} 

		return null;
	}

	/**
	 * @param row
	 * @param neededColumns
	 */
	static List projectRow(List row, int[] neededColumns) {
		List output = new ArrayList(neededColumns.length);

		for (int i = 0; i < neededColumns.length; i++) {
			output.add(row.get(neededColumns[i]));
		}

		return output;
	}

	/*
	 * @see com.metamatrix.data.Execution#close()
	 */
	public void close() throws ConnectorException {
		// nothing to do

		exclusionList = null;
		responses = null;

		if (p != null) {
			try {
				p.destroy();
			} catch (Exception e) {

			} finally {
				p = null;

			}
		}

	}

	/*
	 * @see com.metamatrix.data.Execution#cancel()
	 */
	public void cancel() throws ConnectorException {
		this.close();

	}

	final class ExecuteMMTask extends ExecTask {
		protected ByteArrayOutputStream os = null;
		protected ByteArrayOutputStream erros = null;
		protected ByteArrayInputStream in = null;
		protected BufferedOutputStream bos = null;
		protected BufferedOutputStream bose = null;

		public ExecuteMMTask() {
		}

		/**
		 * Create the StreamHandler to use with our Execute instance.
		 * 
		 * @return the execute stream handler to manage the input, output and
		 *         error streams.
		 * 
		 * @throws BuildException
		 *             if the execute stream handler cannot be created.
		 */
		public synchronized ExecuteStreamHandler createHandler()
				throws BuildException {
			super.createHandler();

			os = new ByteArrayOutputStream();
			bos = new BufferedOutputStream(os);

			erros = new ByteArrayOutputStream();
			bose = new BufferedOutputStream(erros);

			in = new ByteArrayInputStream(new byte[] { ' ' });

			return new PumpStreamHandler(bos, bose);
		}

		public void addToRows(List responseRows) throws Exception {

			try {
				bos.flush();

				ByteArrayInputStream bais = new ByteArrayInputStream(os
						.toByteArray());
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(bais));
				String oneline;
				while ((oneline = reader.readLine()) != null) {
					List row = new ArrayList(1);
					row.add(oneline);
					responseRows.add(row);
				}
				reader.close();

				boolean first = true;
				bose.flush();
				ByteArrayInputStream baise = new ByteArrayInputStream(erros
						.toByteArray());
				BufferedReader ereader = new BufferedReader(
						new InputStreamReader(baise));
				while ((oneline = ereader.readLine()) != null) {
					if (first) {
						env.getLogger().logError("Error Message:"); //$NON-NLS-1$
						first = false;
					}
					List row = new ArrayList(1);
					row.add(oneline);
					responseRows.add(row);
					env.getLogger().logError(oneline);
				}
				ereader.close();
			} finally {
				if (os != null) {
					os.close();
					os = null;
				}

				if (erros != null) {
					erros.close();
					erros = null;
				}
			}
		}

	}

}
