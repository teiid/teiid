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

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;

import org.teiid.connector.DataPlugin;
import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.InputStreamFactory.FileInputStreamFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.file.FtpFileConnection;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.RuntimeMetadata;
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
		boolean isFtp = false;

		private FileProcedureExecution(Call command, FileConnection fc, boolean isFtp) {
			this.command = command;
			this.fc = fc;
			this.isFtp = isFtp;
		}

		@Override
		public void execute() throws TranslatorException {
			String path = (String)command.getArguments().get(0).getArgumentValue().getValue();
			try {
			    if(isFtp) {
			        FtpFileConnection ftpConn = (FtpFileConnection) fc; 
			        files = ftpConn.getFiles(path);
			    } else {
			        files = FileConnection.Util.getFiles(path, fc, exceptionIfFileNotFound);
			    }
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Getting", files != null ? files.length : 0, "file(s)"); //$NON-NLS-1$ //$NON-NLS-2$
			String name = command.getProcedureName();
			if (name.equalsIgnoreCase(GETTEXTFILES) || name.equalsIgnoreCase(GETFTPTEXTFILES)) {
				isText = true;
			} else if (!name.equalsIgnoreCase(GETFILES) || !name.equalsIgnoreCase(GETFTPFILES)) {
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
				clob.setCharset(encoding);
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
	public static final String DELETEFILE = "deleteFile"; //$NON-NLS-1$
	public static final String GETFTPTEXTFILES = "getFtpTextFiles"; //$NON-NLS-1$
    public static final String GETFTPFILES = "getFtpFiles"; //$NON-NLS-1$
    public static final String SAVEFTPFILE = "saveFtpFile"; //$NON-NLS-1$
    public static final String DELETEFTPFILE = "deleteFtpFile"; //$NON-NLS-1$
    public static final String RENAMEFTPFILE = "renameFtpFile"; //$NON-NLS-1$
	
	private Charset encoding = Charset.defaultCharset();
	private boolean exceptionIfFileNotFound = true;
	
	public FileExecutionFactory() {
		setTransactionSupport(TransactionSupport.NONE);
		setSourceRequiredForMetadata(false);
	}
	
	@TranslatorProperty(display="File Encoding",advanced=true)
	public String getEncoding() {
		return encoding.name();
	}
	
	public void setEncoding(String encoding) {
		this.encoding = Charset.forName(encoding);
	}
	
	@TranslatorProperty(display="Exception if file not found",advanced=true)
	public boolean isExceptionIfFileNotFound() {
		return exceptionIfFileNotFound;
	}
	
	public void setExceptionIfFileNotFound(boolean exceptionIfFileNotFound) {
		this.exceptionIfFileNotFound = exceptionIfFileNotFound;
	}
	
	//@Override
	public ProcedureExecution createProcedureExecution(final Call command,
			final ExecutionContext executionContext, final RuntimeMetadata metadata,
			final FileConnection fc) throws TranslatorException {
		if (command.getProcedureName().equalsIgnoreCase(GETFTPTEXTFILES) || command.getProcedureName().equalsIgnoreCase(GETFTPFILES)) {
		    return new FileProcedureExecution(command, fc, true);
		} else if(command.getProcedureName().equalsIgnoreCase(SAVEFILE) || command.getProcedureName().equalsIgnoreCase(SAVEFTPFILE)) {
			return new ProcedureExecution() {
				
				@Override
				public void execute() throws TranslatorException {
				    String procName = command.getProcedureName();
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
					
						if(procName.equalsIgnoreCase(SAVEFTPFILE)){
						    FtpFileConnection ftpConn = (FtpFileConnection) fc; 
						    ftpConn.write(is, filePath);
						} else {
						    ObjectConverterUtil.write(is, fc.getFile(filePath));
						}
					} catch (IOException e) {
						throw new TranslatorException(e, UTIL.getString("error_writing")); //$NON-NLS-1$
					} catch (SQLException e) {
						throw new TranslatorException(e, UTIL.getString("error_writing")); //$NON-NLS-1$
					} catch (ResourceException e) {
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
		} else if (command.getProcedureName().equalsIgnoreCase(DELETEFILE) || command.getProcedureName().equalsIgnoreCase(DELETEFTPFILE)) {
			return new ProcedureExecution() {
				
				@Override
				public void execute() throws TranslatorException {
				    String procName = command.getProcedureName();
					String filePath = (String)command.getArguments().get(0).getArgumentValue().getValue();
					
					if ( filePath == null) {
						throw new TranslatorException(UTIL.getString("non_null")); //$NON-NLS-1$
					}
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Deleting", filePath); //$NON-NLS-1$
					
					try {
					    if(procName.equalsIgnoreCase(DELETEFTPFILE)) {
					        FtpFileConnection ftpConn = (FtpFileConnection) fc; 
					        ftpConn.remove(filePath);
					    } else {
					        File f = fc.getFile(filePath);
	                        if (!f.exists()) {
	                            if (exceptionIfFileNotFound) {
	                                throw new TranslatorException(DataPlugin.Util.gs("file_not_found", filePath)); //$NON-NLS-1$
	                            }
	                        } else if(!f.delete()){
	                            throw new TranslatorException(UTIL.getString("error_deleting")); //$NON-NLS-1$
	                        }
					    }				
					} catch (ResourceException e) {
						throw new TranslatorException(e, UTIL.getString("error_deleting")); //$NON-NLS-1$
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
		} else if (command.getProcedureName().equalsIgnoreCase(RENAMEFTPFILE)) {
		    return new ProcedureExecution() {

                @Override
                public List<?> next() throws TranslatorException, DataNotAvailableException {
                    return null;
                }

                @Override
                public void close() {                    
                }

                @Override
                public void cancel() throws TranslatorException {                    
                }

                @Override
                public void execute() throws TranslatorException {
                    String oldPath = (String)command.getArguments().get(0).getArgumentValue().getValue();
                    String newPath = (String)command.getArguments().get(1).getArgumentValue().getValue();
                    if ( oldPath == null || newPath == null) {
                        throw new TranslatorException(UTIL.getString("non_null")); //$NON-NLS-1$
                    }
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Renaming", oldPath, newPath); //$NON-NLS-1$
                    FtpFileConnection ftpConn = (FtpFileConnection) fc; 
                    try {
                        ftpConn.rename(oldPath, newPath);
                    } catch (ResourceException e) {
                        throw new TranslatorException(e, UTIL.getString("error_renaming")); //$NON-NLS-1$
                    }
                }

                @Override
                public List<?> getOutputParameterValues() throws TranslatorException {
                    return Collections.emptyList();
                }
		        
		    };
		}
		return new FileProcedureExecution(command, fc, false);
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
		p1.setAnnotation("Returns files that match the given path and pattern as BLOBs"); //$NON-NLS-1$
		param = metadataFactory.addProcedureParameter("pathAndPattern", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p1); //$NON-NLS-1$
		param.setAnnotation("The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path."); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.BLOB, p1); //$NON-NLS-1$
		metadataFactory.addProcedureResultSetColumn("filePath", TypeFacility.RUNTIME_NAMES.STRING, p1); //$NON-NLS-1$
		
		Procedure p2 = metadataFactory.addProcedure(SAVEFILE);
		p2.setAnnotation("Saves the given value to the given path.  Any existing file will be overriden."); //$NON-NLS-1$
		metadataFactory.addProcedureParameter("filePath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p2); //$NON-NLS-1$
		param = metadataFactory.addProcedureParameter("file", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p2); //$NON-NLS-1$
		param.setAnnotation("The contents to save.  Can be one of CLOB, BLOB, or XML"); //$NON-NLS-1$
		
		Procedure p3 = metadataFactory.addProcedure(DELETEFILE);
		p3.setAnnotation("Delete the given file path. "); //$NON-NLS-1$
		metadataFactory.addProcedureParameter("filePath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p3); //$NON-NLS-1$
		
		Procedure p4 = metadataFactory.addProcedure(GETFTPTEXTFILES);
        p4.setAnnotation("Returns text files that match the given path and pattern as CLOBs"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("pathAndPattern", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p4); //$NON-NLS-1$
        param.setAnnotation("The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path."); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.CLOB, p4); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("filePath", TypeFacility.RUNTIME_NAMES.STRING, p4); //$NON-NLS-1$
        
        Procedure p5 = metadataFactory.addProcedure(GETFTPFILES);
        p5.setAnnotation("Returns files that match the given path and pattern as BLOBs"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("pathAndPattern", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p5); //$NON-NLS-1$
        param.setAnnotation("The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path."); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.BLOB, p5); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("filePath", TypeFacility.RUNTIME_NAMES.STRING, p5); //$NON-NLS-1$
        
        Procedure p6 = metadataFactory.addProcedure(SAVEFTPFILE);
        p6.setAnnotation("Saves the given value to the given path.  Any existing file will be overriden."); //$NON-NLS-1$
        metadataFactory.addProcedureParameter("filePath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p6); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("file", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p6); //$NON-NLS-1$
        param.setAnnotation("The contents to save.  Can be one of CLOB, BLOB, or XML"); //$NON-NLS-1$
        
        Procedure p7 = metadataFactory.addProcedure(DELETEFTPFILE);
        p7.setAnnotation("Delete the given file path. "); //$NON-NLS-1$
        metadataFactory.addProcedureParameter("filePath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p7); //$NON-NLS-1$   
        
        Procedure p8 = metadataFactory.addProcedure(RENAMEFTPFILE);
        p8.setAnnotation("Rename the given file."); //$NON-NLS-1$
        metadataFactory.addProcedureParameter("oldPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p8); //$NON-NLS-1$
        metadataFactory.addProcedureParameter("newPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p8); //$NON-NLS-1$
	} 
	
	@Override
	public boolean areLobsUsableAfterClose() {
		return true;
	}
	
}
