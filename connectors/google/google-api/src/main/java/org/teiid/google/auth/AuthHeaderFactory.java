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

package org.teiid.google.auth;

/**
 * Google services are authenticated using Http headers. Format and content
 * of this header differs based on authentication mechanism.
 *
 * Implementors of this interface should choose provide the header for auth purposes.
 *
 * @author fnguyen
 *
 */
public interface AuthHeaderFactory {

    /**
     * Gets the authorization header. Typically performs the login (interaction
     * with google services). Should be called only when necessary (first login, google session expires)
     */
    public void refreshToken();
    public String getAuthHeader();
}
