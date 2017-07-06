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
package org.teiid.internal.core.index;

import java.util.Arrays;

import org.teiid.core.index.IEntryResult;


public class EntryResult implements IEntryResult {
	private char[] word;
	private int[] fileRefs;

	public EntryResult(char[] word, int[] refs) {
		this.word = word;
		this.fileRefs = refs;
	}

	public boolean equals(Object anObject) {

		if (this == anObject) {
			return true;
		}
		if ((anObject != null) && (anObject instanceof EntryResult)) {
			EntryResult anEntryResult = (EntryResult) anObject;
			if (!Arrays.equals(this.word, anEntryResult.word))
				return false;

			int length;
			int[] refs, otherRefs;
			if ((length = (refs = this.fileRefs).length) != (otherRefs = anEntryResult.fileRefs).length)
				return false;
			for (int i = 0; i < length; i++) {
				if (refs[i] != otherRefs[i])
					return false;
			}
			return true;
		}
		return false;

	}

	public int[] getFileReferences() {
		return fileRefs;
	}

	public char[] getWord() {
		return word;
	}

	public int hashCode() {
		return Arrays.hashCode(word);
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer(word.length * 2);
		buffer.append("EntryResult: word="); //$NON-NLS-1$
		buffer.append(word);
		buffer.append(", refs={"); //$NON-NLS-1$
		for (int i = 0; i < fileRefs.length; i++) {
			if (i > 0)
				buffer.append(',');
			buffer.append(' ');
			buffer.append(fileRefs[i]);
		}
		buffer.append(" }"); //$NON-NLS-1$
		return buffer.toString();
	}
}
