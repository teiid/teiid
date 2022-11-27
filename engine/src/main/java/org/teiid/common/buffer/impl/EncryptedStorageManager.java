/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.common.buffer.impl;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.crypto.CryptoException;
import org.teiid.core.crypto.SymmetricCryptor;

/**
 * Implements a block AES cipher over a regular filestore.
 * <br>
 * With ECB mode and no padding, we just replace the
 * the bytes 1 block at a time with some special handling
 * for when not block aligned.
 * <br>
 * A great deal of the security comes from the encryption
 * key only being used on a temporary basis.  We also xor
 * by the block to add a very simple CTR like mode so that
 * identical blocks don't result in the same storage bytes.
 * <br>
 * TODO: use masking for division
 */
public class EncryptedStorageManager implements StorageManager {

    private static final String DEFAULT_ALGORITHM = "AES/ECB/NoPadding"; //$NON-NLS-1$

    static final class EncryptedFileStore extends FileStore {
        private final FileStore file;
        private volatile long len;
        private Cipher encrypt;
        private Cipher decrypt;
        private int blockSize;

        private EncryptedFileStore(FileStore file, SecretKey key) throws GeneralSecurityException {
            this.file = file;
            decrypt = Cipher.getInstance( DEFAULT_ALGORITHM);
            decrypt.init( Cipher.DECRYPT_MODE, key );
            encrypt = Cipher.getInstance( DEFAULT_ALGORITHM);
            blockSize = encrypt.getBlockSize();
            encrypt.init( Cipher.ENCRYPT_MODE, key );
        }

        @Override
        public synchronized void setLength(long length) throws IOException {
            this.len = length;
            if (length % blockSize == 0) {
                file.setLength(length);
            } else {
                file.setLength((length/blockSize + 1)*blockSize);
            }
        }

        @Override
        protected void removeDirect() {
            file.remove();
        }

        @Override
        protected synchronized int readWrite(long fileOffset, byte[] b, int offSet, int length,
                boolean write) throws IOException {
            if (length == 0) {
                return 0;
            }
            long block = fileOffset/blockSize;
            int remainder = (int)(fileOffset%blockSize);
            if (!write) {
                if (fileOffset > len) {
                    throw new IOException("Invalid file position " + fileOffset + " length " + length); //$NON-NLS-1$ //$NON-NLS-2$
                }
                length = (int)Math.min(Integer.MAX_VALUE, Math.min(len - fileOffset, length));
            }
            long adjustedfileOffset = fileOffset;
            int adjustedLength = length;
            byte[] buffer = b;
            int bufferOffset = offSet;
            int remainingLength = (int) ((length+fileOffset)%blockSize);
            if (remainder!=0||remainingLength!=0) {
                adjustedfileOffset -= remainder;
                adjustedLength += remainder;
                if (adjustedLength%blockSize!=0) {
                    //round to the next block
                    adjustedLength=(adjustedLength/blockSize + 1)*blockSize;
                }
                //create a new fully aligned buffer
                buffer = new byte[adjustedLength];
                bufferOffset = 0;
            }
            int blocks = adjustedLength/blockSize;
            int blockOffset = 0;
            if (!write) {
                file.readFully(adjustedfileOffset, buffer, bufferOffset, adjustedLength);
                for (int i = 0; i < blocks; i++) {
                    try {
                        //TODO: directly copy into the output buffer when not block aligned
                        decrypt.doFinal(buffer, blockOffset, blockSize, buffer, blockOffset);
                        xorByBlock(buffer, 0, block + i, blockOffset);
                    } catch (GeneralSecurityException e) {
                        throw new IOException(e);
                    }
                    blockOffset += blockSize;
                }
                if (adjustedLength != length) {
                    //copy back the proper subset into the output buffer
                    System.arraycopy(buffer, remainder, b, offSet, length);
                }
                return length;
            }
            if (remainder!=0) {
                //need to read the partial value and re-encrypt the whole block
                readFully(fileOffset - remainder, buffer, 0, remainder);
            } else if (buffer == b) {
                //create a temp buffer to do the encryption in
                buffer = new byte[adjustedLength];
            }
            System.arraycopy(b, offSet, buffer, remainder, length);
            for (int i = 0; i < blocks; i++) {
                try {
                    if (i + 1 == blocks && remainingLength != 0 && fileOffset + length < len) {
                        //TODO: combine with read above when working with a small enough length
                        readFully(fileOffset + length, buffer, blockOffset + remainingLength, (int)Math.min(blockSize - remainingLength, len - fileOffset - length));
                    }
                    xorByBlock(buffer, offSet, block + i, blockOffset);
                    encrypt.doFinal(buffer, blockOffset, blockSize, buffer, blockOffset);
                } catch (GeneralSecurityException e) {
                    throw new IOException(e);
                }
                blockOffset += blockSize;
            }
            file.write(adjustedfileOffset, buffer, 0, adjustedLength);
            len = Math.max(len, fileOffset + length);
            return length;
        }

        private void xorByBlock(byte[] b, int offSet, long blockMask,
                int blockOffset) {
            for (int j = 0; j < blockSize && blockMask > 0; j++) {
                b[offSet + blockOffset + j] ^= blockMask;
                blockMask >>= 8;
            }
        }

        @Override
        public long getLength() {
            return len;
        }

        FileStore getFile() {
            return file;
        }
    }

    private StorageManager manager;
    private SecretKey key;

    public EncryptedStorageManager(StorageManager manager) {
        this.manager = manager;
    }

    @Override
    public void initialize() throws TeiidComponentException {
        manager.initialize();
        try {
            key = SymmetricCryptor.generateKey();
        } catch (CryptoException e) {
            throw new TeiidComponentException(e);
        }
    }

    @Override
    public EncryptedFileStore createFileStore(String name) {
        final FileStore file = manager.createFileStore(name);
        try {
            return new EncryptedFileStore(file, key);
        } catch (GeneralSecurityException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    @Override
    public long getMaxStorageSpace() {
        return manager.getMaxStorageSpace();
    }

}

