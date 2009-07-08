/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials 
 * are made available under the terms of the Common Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/cpl-v10.html
 * 
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/
package org.teiid.internal.core.index;

import java.io.File;

import org.teiid.core.index.IDocument;


/**
 * A <code>FileDocument</code> represents a java.io.File.
 */

public class FileDocument extends PropertyDocument {
	File file;

	public FileDocument(File file) {
		super();
		this.file= file;
	}
	/**
	 * @see org.teiid.core.index.IDocument#getEncoding()
	 */
	public String getEncoding() {
		return null; // no custom encoding
	}

	/**
	 * @see IDocument#getName
	 */
	public String getName() {
		return file.getAbsolutePath().replace(File.separatorChar, IIndexConstants.FILE_SEPARATOR);
	}
	/**
	 * @see IDocument#getType
	 */
	public String getType() {
		int lastDot= file.getPath().lastIndexOf('.');
		if (lastDot == -1)
			return ""; //$NON-NLS-1$
		return file.getPath().substring(lastDot + 1);
	}
}
