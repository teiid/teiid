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

package org.teiid.translator.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.InputStreamFactory.FileInputStreamFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;

@Translator(name="file", description="File Translator, reads contents of files or writes to them")
public class FileExecutionFactory extends ExecutionFactory<ConnectionFactory, FileConnection> {
	
	private final class FileProcedureExecution implements ProcedureExecution {
		private final Call command;
		private final FileConnection fc;
		private File[] files = null;
		boolean isText = false;
		private int index;

		private FileProcedureExecution(Call command, FileConnection fc) {
			this.command = command;
			this.fc = fc;
		}

		@Override
		public void execute() throws TranslatorException {
			files = FileConnection.Util.getFiles((String)command.getArguments().get(0).getArgumentValue().getValue(), fc);
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Getting", files != null ? files.length : 0, "file(s)"); //$NON-NLS-1$ //$NON-NLS-2$
			String name = command.getProcedureName();
			if (name.equalsIgnoreCase(GETTEXTFILES)) {
				isText = true;
			} else if (!name.equalsIgnoreCase(GETFILES)) {
				throw new TeiidRuntimeException("Unknown procedure name " + name); //$NON-NLS-1$
			}
		}

		@Override
		public void close() {
			
		}

		@Override
		public void cancel() throws TranslatorException {
			
		}

		@Override
		public List<?> next() throws TranslatorException, DataNotAvailableException {
			if (files == null || index >= files.length) {
				return null;
			}
			ArrayList<Object> result = new ArrayList<Object>(2);
			final File file = files[index++];
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Getting", file); //$NON-NLS-1$
			FileInputStreamFactory isf = new FileInputStreamFactory(file);
			isf.setLength(file.length());
			Object value = null;
			if (isText) {
				ClobImpl clob = new ClobImpl(isf, -1);
				clob.setEncoding(encoding.name());
				value = new ClobType(clob);
			} else {
				value = new BlobType(new BlobImpl(isf));
			}
			result.add(value);
			result.add(file.getName());
			return result;
		}

		@Override
		public List<?> getOutputParameterValues() throws TranslatorException {
			return Collections.emptyList();
		}
	}

	public static BundleUtil UTIL = BundleUtil.getBundleUtil(FileExecutionFactory.class);
	
	public static final String GETTEXTFILES = "getTextFiles"; //$NON-NLS-1$
	public static final String GETFILES = "getFiles"; //$NON-NLS-1$
	public static final String SAVEFILE = "saveFile"; //$NON-NLS-1$
	
	private Charset encoding = Charset.defaultCharset();
	
	@TranslatorProperty(display="File Encoding",advanced=true)
	public String getEncoding() {
		return encoding.name();
	}
	
	public void setEncoding(String encoding) {
		this.encoding = Charset.forName(encoding);
	}
	
	//@Override
	public ProcedureExecution createProcedureExecution(final Call command,
			final ExecutionContext executionContext, final RuntimeMetadata metadata,
			final FileConnection fc) throws TranslatorException {
		if (command.getProcedureName().equalsIgnoreCase(SAVEFILE)) {
			return new ProcedureExecution() {
				
				@Override
				public void execute() throws TranslatorException {
					String filePath = (String)command.getArguments().get(0).getArgumentValue().getValue();
					Object file = command.getArguments().get(1).getArgumentValue().getValue();
					if (file == null || filePath == null) {
						throw new TranslatorException(UTIL.getString("non_null")); //$NON-NLS-1$
					}
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Saving", filePath); //$NON-NLS-1$
					InputStream is = null;
					try {
						if (file instanceof SQLXML) {
							is = ((SQLXML)file).getBinaryStream();
						} else if (file instanceof Clob) {
							is = new ReaderInputStream(((Clob)file).getCharacterStream(), encoding);
						} else if (file instanceof Blob) {
							is = ((Blob)file).getBinaryStream();
						} else {
							throw new TranslatorException(UTIL.getString("unknown_type")); //$NON-NLS-1$
						}
					
						ObjectConverterUtil.write(is, fc.getFile(filePath));
					} catch (IOException e) {
						throw new TranslatorException(e, UTIL.getString("error_writing")); //$NON-NLS-1$
					} catch (SQLException e) {
						throw new TranslatorException(e, UTIL.getString("error_writing")); //$NON-NLS-1$
					}
				}
				
				@Override
				public void close() {
				}
				
				@Override
				public void cancel() throws TranslatorException {
				}
				
				@Override
				public List<?> next() throws TranslatorException, DataNotAvailableException {
					return null;
				}
				
				@Override
				public List<?> getOutputParameterValues() throws TranslatorException {
					return Collections.emptyList();
				}
			};
		}
		return new FileProcedureExecution(command, fc);
	}

	@Override
	public void getMetadata(MetadataFactory metadataFactory, FileConnection connection) throws TranslatorException {
		Procedure p = metadataFactory.addProcedure(GETTEXTFILES);
		p.setAnnotation("Returns text files that match the given path and pattern as CLOBs"); //$NON-NLS-1$
		ProcedureParameter param = metadataFactory.addProcedureParameter("pathAndPattern", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setAnnotation("The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path."); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.CLOB, p); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("filePath", TypeFacility.RUNTIME_NAMES.STRING, p); //$NON-NLS-1$
		
		Procedure p1 = metadataFactory.addProcedure(GETFILES);
		p1.setAnnotation("Returns text files that match the given path and pattern as BLOBs"); //$NON-NLS-1$
		param = metadataFactory.addProcedureParameter("pathAndPattern", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p1); //$NON-NLS-1$
		param.setAnnotation("The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path."); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.BLOB, p1); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("filePath", TypeFacility.RUNTIME_NAMES.STRING, p1); //$NON-NLS-1$
		
		Procedure p2 = metadataFactory.addProcedure(SAVEFILE);
		p2.setAnnotation("Saves the given vale to the given path.  Any existing file will be overriden."); //$NON-NLS-1$
		metadataFactory.addProcedureParameter("filePath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p2); //$NON-NLS-1$
		param = metadataFactory.addProcedureParameter("file", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p2); //$NON-NLS-1$
		param.setAnnotation("The contents to save.  Can be one of CLOB, BLOB, or XML"); //$NON-NLS-1$
	} 
	
	@Override
	public boolean areLobsUsableAfterClose() {
		return true;
	}
	
}
