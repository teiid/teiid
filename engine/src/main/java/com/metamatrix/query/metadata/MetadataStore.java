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

package com.metamatrix.query.metadata;

import java.util.Collection;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixCoreException;

public interface MetadataStore {
	
	ModelRecordImpl getModel(String fullName) throws QueryMetadataException, MetaMatrixComponentException;

	TableRecordImpl findGroup(String fullName) throws QueryMetadataException, MetaMatrixComponentException;
	
	/**
	 * Returns the fully qualified names of groups matching the given partial name.
	 * 
	 * @param partialGroupName expected to be in lowercase
	 * @return
	 * @throws MetaMatrixComponentException
	 * @throws QueryMetadataException
	 */
	Collection<String> getGroupsForPartialName(final String partialGroupName)
    throws MetaMatrixComponentException, QueryMetadataException;
	
	ProcedureRecordImpl getStoredProcedure(final String fullyQualifiedProcedureName)
    throws MetaMatrixComponentException, QueryMetadataException;
	
	Collection<PropertyRecordImpl> getExtensionProperties(AbstractMetadataRecord record) throws MetaMatrixComponentException;
	
	/**
	 * Get the set of model names known to this store.
	 */
	Collection<String> getModelNames();
	
	/**
	 * Search method used by the index connector
	 * @param indexName
	 * @param patterns
	 * @param isPrefix
	 * @param isCaseSensitive
	 * @return
	 * @throws MetaMatrixCoreException
	 */
	Collection<? extends AbstractMetadataRecord> findMetadataRecords(final String indexName,
			String pattern, boolean isPrefix,
			boolean isCaseSensitive) throws MetaMatrixCoreException;
	
	boolean postProcessFindMetadataRecords();

	/**
	 * @deprecated used only for xml
	 * @param table
	 * @return
	 * @throws MetaMatrixComponentException
	 */
	Collection getXMLTempGroups(TableRecordImpl table) throws MetaMatrixComponentException;
	
	/**
	 * @deprecated used only for xml
	 * @param table
	 * @return
	 * @throws MetaMatrixComponentException
	 */
	Collection<? extends AbstractMetadataRecord> findMetadataRecords(final char recordType,
			final String entityName, final boolean isPartialName)
			throws MetaMatrixComponentException;
	
	/**
	 * @deprecated used only by xml and uuid resolving
	 * @param fullName
	 * @return
	 * @throws QueryMetadataException
	 * @throws MetaMatrixComponentException
	 */
	ColumnRecordImpl findElement(String fullName) throws QueryMetadataException, MetaMatrixComponentException;

}
