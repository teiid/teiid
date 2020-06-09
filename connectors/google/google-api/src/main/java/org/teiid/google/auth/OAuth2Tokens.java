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

import java.util.Date;

public class OAuth2Tokens {
    private String accessToken = null;
    private String refreshToken = null;
    private Integer expiresIn = null;
    private Date created = null;

    public String getAccessToken() {
        return accessToken;
    }
    public String getRefreshToken() {
        return refreshToken;
    }
    public Integer getExpiresIn() {
        return expiresIn;
    }


    public Date getCreated() {
        return created;
    }
    public OAuth2Tokens(Object accessToken, Object refreshToken,
            Object expiresIn) {
        super();
        this.accessToken = (String)accessToken;
        this.refreshToken =(String) refreshToken;
        Number n = (Number)expiresIn;
        if (n != null) {
            this.expiresIn = n.intValue();
        }
        created = new Date();
    }

    @SuppressWarnings("nls")
    @Override
    public String toString() {
        return "AccessTokens [accessToken=" + accessToken
                + ", refreshToken=" + refreshToken + ", expiresIn="
                + expiresIn + "]";
    }
}
