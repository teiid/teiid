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
		
	public ModeShapeJDBCMetdataProcessor() {
		super();
		setWidenUnsignedTypes(false);
		setUseQualifiedName(false);
		setUseCatalogName(false);
		setImportForeignKeys(false);
		
	}
	
	@Override
	public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
	throws SQLException {
		
		if (getTableTypes() == null || getTableTypes().length == 0) {
			setTableTypes( new String[] {"VIEW" } );
		}

		super.getConnectorMetadata(conn, metadataFactory);
	}
	
}
