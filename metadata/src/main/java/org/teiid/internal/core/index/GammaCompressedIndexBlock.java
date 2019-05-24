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

import java.io.UTFDataFormatException;

/**
 * Uses prefix coding on words, and gamma coding of document numbers differences.
 */
public class GammaCompressedIndexBlock extends IndexBlock {
    CodeByteStream readCodeStream;
    char[] prevWord= null;
    int offset= 0;

    public GammaCompressedIndexBlock(int blockSize) {
        super(blockSize);
        readCodeStream= new CodeByteStream(field.buffer());
    }
    /**
     * @see IndexBlock#isEmpty
     */
    public boolean isEmpty() {
        return offset == 0;
    }
    /**
     * @see IndexBlock#nextEntry
     */
    public boolean nextEntry(WordEntry entry) {
        try {
            readCodeStream.reset(field.buffer(), offset);
            int prefixLength= readCodeStream.readByte();
            char[] word= readCodeStream.readUTF();
            if (prevWord != null && prefixLength > 0) {
                char[] temp= new char[prefixLength + word.length];
                System.arraycopy(prevWord, 0, temp, 0, Math.min(prefixLength, prevWord.length));
                System.arraycopy(word, 0, temp, Math.min(prefixLength, prevWord.length), word.length);
                word= temp;
            }
            if (word.length == 0) {
                return false;
            }
            entry.reset(word);
            int n= readCodeStream.readGamma();
            int prevRef= 0;
            for (int i= 0; i < n; ++i) {
                int ref= prevRef + readCodeStream.readGamma();
                if (ref < prevRef)
                    throw new InternalError();
                entry.addRef(ref);
                prevRef= ref;
            }
            offset= readCodeStream.byteLength();
            prevWord= word;
            return true;
        } catch (UTFDataFormatException e) {
            return false;
        }
    }
    /**
     * @see IndexBlock#reset
     */
    public void reset() {
        super.reset();
        offset= 0;
        prevWord= null;
    }
}
