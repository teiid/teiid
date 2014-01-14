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

package org.teiid.translator.jdbc.modeshape;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.JDBCPlugin;


/**
 * Reads from {@link DatabaseMetaData} and creates metadata through the {@link MetadataFactory}.
 * 
 * See https://issues.jboss.org/browse/TEIID-2786 which describes this implementation.
 * 
 */
public class ModeShapeJDBCMetdataProcessor extends JDBCMetdataProcessor {
	
	public static final String JCR_ISCHILDNODE_PROC = "jcr_ischildnode"; //$NON-NLS-1$
	public static final String JCR_ISDESCENDANTNODE_PROC = "jcr_isdescendantnode"; //$NON-NLS-1$
	public static final String JCR_ISSAMENODE_PROC = "jcr_issamenode"; //$NON-NLS-1$
	public static final String JCR_CONTAINS_PROC = "jcr_contains"; //$NON-NLS-1$
	public static final String JCR_REFERENCE_PROC = "jcr_reference"; //$NON-NLS-1$
		
	public ModeShapeJDBCMetdataProcessor() {
		super();
		setImportProcedures(true);
		setUseProcedureSpecificName(true);
		setImportIndexes(true);
		
		setImportApproximateIndexes(false);		
		setWidenUnsignedTypes(false);
		setUseQualifiedName(false);
		setUseCatalogName(false);
		setUseFullSchemaName(false);
		
		setAutoCreateUniqueConstraints(false);
		
	}
	
	@Override
	public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
	throws SQLException {
		try {
			super.getConnectorMetadata(conn, metadataFactory);			
		} catch (java.sql.SQLFeatureNotSupportedException sfns) {
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11022, "getConnectorMetadata"));
			sfns.printStackTrace();
		}
	}
	
	@Override
	protected void getProcedures(MetadataFactory metadataFactory,
			DatabaseMetaData metadata) throws SQLException {
		
		// Do not call super.getProcedures(..) because ModeShape doesn't have them
		
		Procedure p = metadataFactory.addProcedure(JCR_ISCHILDNODE_PROC);
		p.setAnnotation("Returns boolean indicating if the jcr path contains the path as a child node"); //$NON-NLS-1$
		ProcedureParameter param1 = metadataFactory.addProcedureParameter("jcrParentPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param1.setAnnotation("The jcr path that may contain the child node."); //$NON-NLS-1$
		param1.setLength(255);
		ProcedureParameter param2 = metadataFactory.addProcedureParameter("jcrPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param2.setAnnotation("The jcr path that may be a child node of the parent jcr path."); //$NON-NLS-1$
		param1.setLength(255);
		metadataFactory.addProcedureResultSetColumn("isChild", TypeFacility.RUNTIME_NAMES.BOOLEAN, p); //$NON-NLS-1$

		
		Procedure p2 = metadataFactory.addProcedure(JCR_ISDESCENDANTNODE_PROC);
		p2.setAnnotation("Returns boolean indicating if the 2nd jcr path is a descendant of the 1st jcr path"); //$NON-NLS-1$
		ProcedureParameter param1_2 = metadataFactory.addProcedureParameter("jcr1stPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p2); //$NON-NLS-1$
		param1_2.setAnnotation("The 1st jcr path."); //$NON-NLS-1$
		param1_2.setLength(255);
		ProcedureParameter param2_2 = metadataFactory.addProcedureParameter("jcr2ndPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p2); //$NON-NLS-1$
		param2_2.setAnnotation("The jcr path that may be a descendant of the 1st jcr path."); //$NON-NLS-1$
		param2_2.setLength(255);
		metadataFactory.addProcedureResultSetColumn("isDescendant", TypeFacility.RUNTIME_NAMES.BOOLEAN, p2); //$NON-NLS-1$

		
		Procedure p3 = metadataFactory.addProcedure(JCR_ISSAMENODE_PROC);
		p3.setAnnotation("Returns boolean indicating if the 2nd jcr path is the same node as the 1st jcr path"); //$NON-NLS-1$
		ProcedureParameter param1_3 = metadataFactory.addProcedureParameter("jcr1stPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p3); //$NON-NLS-1$
		param1_3.setAnnotation("The 1st jcr path."); //$NON-NLS-1$
		param1_3.setLength(255);
		ProcedureParameter param2_3 = metadataFactory.addProcedureParameter("jcr2ndPath", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p3); //$NON-NLS-1$
		param2_3.setAnnotation("The jcr path that may be same node as the 1st jcr path."); //$NON-NLS-1$
		param2_3.setLength(255);
		metadataFactory.addProcedureResultSetColumn("isSameNode", TypeFacility.RUNTIME_NAMES.BOOLEAN, p3); //$NON-NLS-1$
		
		
		Procedure p4 = metadataFactory.addProcedure(JCR_CONTAINS_PROC);
		p4.setAnnotation("Returns boolean indicating if the searchExpr is found in the selector path or property"); //$NON-NLS-1$
		ProcedureParameter param1_4 = metadataFactory.addProcedureParameter("selectorOrProperty", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p4); //$NON-NLS-1$
		param1_4.setAnnotation("The selector path or property."); //$NON-NLS-1$
		param1_4.setLength(255);
		ProcedureParameter param2_4 = metadataFactory.addProcedureParameter("searchExpr", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p4); //$NON-NLS-1$
		param2_4.setAnnotation("The expression to search the selector or property "); //$NON-NLS-1$
		param2_4.setLength(255);
		metadataFactory.addProcedureResultSetColumn("contains", TypeFacility.RUNTIME_NAMES.BOOLEAN, p4); //$NON-NLS-1$
		
		
		Procedure p5 = metadataFactory.addProcedure(JCR_REFERENCE_PROC);
		p5.setAnnotation("Returns String of the reference that matches the selector path or property"); //$NON-NLS-1$
		ProcedureParameter param1_5 = metadataFactory.addProcedureParameter("selectorOrProperty", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p5); //$NON-NLS-1$
		param1_5.setLength(255);
		param1_5.setAnnotation("The selector path or property."); //$NON-NLS-1$
		Column p5_col = metadataFactory.addProcedureResultSetColumn("reference", TypeFacility.RUNTIME_NAMES.STRING, p5); //$NON-NLS-1$
		p5_col.setLength(255);

	}
	
	protected String quoteName(String name) {
		return super.quoteName(name);

	}
	
	protected Map<String, TableInfo> getTables(MetadataFactory metadataFactory,
			DatabaseMetaData metadata) throws SQLException {
		
		if (getTableTypes() == null || getTableTypes().length == 0) {
			setTableTypes( new String[] {"VIEW" } );
		}
		
		return super.getTables(metadataFactory, metadata);
	}
	
	protected void getPrimaryKeys(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Collection<TableInfo> tables)
			throws SQLException {
		try {
			super.getPrimaryKeys(metadataFactory, metadata, tables);			
		} catch (java.sql.SQLFeatureNotSupportedException sfns) {
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11022, "getPrimaryKeys") );
		}		
	}

	public void getIndexes(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Collection<TableInfo> tables, boolean uniqueOnly) throws SQLException {
		try {
			super.getIndexes(metadataFactory, metadata, tables, uniqueOnly);			
		} catch (java.sql.SQLFeatureNotSupportedException sfns) {
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11022, "getIndexes"));
		}		

	}
	
	protected void getForeignKeys(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Collection<TableInfo> tables, Map<String, TableInfo> tableMap) throws SQLException {
		try {
			super.getForeignKeys(metadataFactory, metadata, tables, tableMap);			
		} catch (java.sql.SQLFeatureNotSupportedException sfns) {
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11022, "getForeignKeys"));

		}	
	}
}
