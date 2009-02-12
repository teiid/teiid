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

package com.metamatrix.jdbc.api;

import java.sql.SQLException;
import java.util.List;

/**
 * The MetaMatrix-specific interface for retrieving metadata from
 * the MetaMatrix server.  This interface provides methods in 
 * addition to the standard JDBC methods. 
 */
public interface DatabaseMetaData extends java.sql.DatabaseMetaData {

    /**
     * Gets a description of models available in a catalog.
     *
     * <P>Only model descriptions matching the catalog, schema, and
     * model are returned.  They are ordered by MODEL_NAME.
     *
     * <P>Each model description has the following columns:
     *  <OL>
     *  <LI><B>MODEL_CAT</B> String => model catalog (may be null)
     *  <LI><B>MODEL_SCHEM</B> String => model schema (may be null)
     *  <LI><B>MODEL_NAME</B> String => model name
     *  <LI><B>DESCRIPTION</B> String => explanatory comment on the model (may be null)
     *  <LI><B>IS_PHYSICAL</B> Boolean => true if the model is a physical model
     *  <LI><B>SUP_WHERE_ALL</B> Boolean => true if queries without a criteria are allowed
     *  <LI><B>SUP_DISTINCT</B> Boolean => true if distinct clause can be used
     *  <LI><B>SUP_JOIN</B> Boolean => true if joins are supported
     *  <LI><B>SUP_OUTER_JOIN</B> Boolean => true if outer joins are supported
     *  <LI><B>SUP_ORDER_BY</B> Boolean => true if order by is supported
     * </OL>
     *
     * <P><B>Note:</B> Some databases may not return information for
     * all models.
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param modelNamePattern a model name pattern 
     * @return <code>ResultSet</code> - each row is a model description
     * @exception SQLException if a database access error occurs
     */
    java.sql.ResultSet getModels(String catalog, String schemaPattern,
        String modelNamePattern) throws SQLException;

    /**
     * Retrieve the XML schemas associated with an XML document.
     * @param documentName A fully-qualified document name
     * @return A list of XML schemas (as String).  The first in the list 
     * will be the primary schema.  
     * @exception SQLException if a database access error occurs
     */
    List getXMLSchemas(String documentName) throws SQLException;

}
