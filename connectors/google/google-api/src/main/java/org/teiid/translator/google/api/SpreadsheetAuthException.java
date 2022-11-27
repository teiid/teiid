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

package org.teiid.translator.google.api;

/**
 * Used for errors during authentication or authorization for GoogleSpreadsheet
 * @author fnguyen
 *
 */
public class SpreadsheetAuthException extends RuntimeException {

    private static final long serialVersionUID = 5098286312672469818L;

    public SpreadsheetAuthException() {
        super();
    }

    public SpreadsheetAuthException(String message, Throwable cause) {
        super(message, cause);
    }

    public SpreadsheetAuthException(String message) {
        super(message);
    }

    public SpreadsheetAuthException(Throwable cause) {
        super(cause);
    }

}
