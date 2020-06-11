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
package org.teiid.file.ftp;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.commons.net.ftp.FTP;

public interface FtpConfiguration {
    /**
     * The parent/root directory for ftp operations.
     * <br>Required Property
     */
    String getParentDirectory();

    /**
     * Required Property
     */
    String getUsername();
    /**
     * Required Property
     */
    String getHost();
    /**
     * Required Property
     */
    String getPassword();

    default Integer getPort() {
        return FTP.DEFAULT_PORT;
    }

    default Integer getBufferSize() {
        return 2048;
    }

    default String getFileMapping() {
        return null;
    }

    /**
     * MUST be one of the values with default set, Default value required
     * <ul>
     * <li>{@link org.apache.commons.net.ftp.FTPClient#ACTIVE_LOCAL_DATA_CONNECTION_MODE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTPClient#PASSIVE_LOCAL_DATA_CONNECTION_MODE}</li>
     * </ul>
     * @return
     */
    default Integer getClientMode() {
        return org.apache.commons.net.ftp.FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE;
    }

    /**
     * File types defined by {@link org.apache.commons.net.ftp.FTP} constants: default file required
     * <ul>
     * <li>{@link org.apache.commons.net.ftp.FTP#ASCII_FILE_TYPE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTP#EBCDIC_FILE_TYPE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTP#BINARY_FILE_TYPE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTP#LOCAL_FILE_TYPE}</li>
     * </ul>
     * @return fileType The file type.
     */
    default Integer getFileType() {
        return org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;
    }

    default String getControlEncoding() {
        return FTP.DEFAULT_CONTROL_ENCODING;
    }

    default Integer getConnectTimeout() {
        return null;
    }

    default Integer getDefaultTimeout() {
        return null;
    }

    default Integer getDataTimeout() {
        return null;
    }

    default Boolean getIsFtps() {
        return false;
    }

    default String getProtocol() {
        return null;
    }

    default Boolean isImplicit() {
        return false;
    }

    // needed when SFTP in play
    default String getAuthValue() {
        return null;
    }
    default TrustManager getTrustManager() {
        return null;
    }
    default KeyManager getKeyManager() {
        return null;
    }
    default Boolean getNeedClientAuth() {
        return null;
    }
    default Boolean getWantsClientAuth() {
        return null;
    }
    default String[] getSupportedCipherSuites() {
        return null;
    }
    default String[] getSupportedProtocols() {
        return null;
    }
    default Boolean getSessionCreation() {
        return null;
    }
    default Boolean getUseClientMode() {
        return null;
    }
    /**
     * Value for the PROT command when SFTP is chosen.
     * Defaults to P - private
     */
    default String getExecProt() {
        return "P";
    }
}
