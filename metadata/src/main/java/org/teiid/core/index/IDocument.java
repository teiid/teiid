/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/
package org.teiid.core.index;


/**
 * An <code>IDocument</code> represent a data source, e.g.&nbsp;a <code>File</code> (<code>FileDocument</code>),
 * an <code>IFile</code> (<code>IFileDocument</code>),
 * or other kinds of data sources (URL, ...). An <code>IIndexer</code> indexes an<code>IDocument</code>.
 */

public interface IDocument {
    /**
     * Returns the encoding for this document
     */
    String getEncoding();
    /**
     * returns the name of the document (e.g. its path for a <code>File</code>, or its relative path
     * in the workbench for an <code>IFile</code>).
     */
    String getName();
    /**
     * Returns the type of the document.
     */
    String getType();
}
