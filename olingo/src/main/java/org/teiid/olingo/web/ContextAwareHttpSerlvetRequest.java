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
package org.teiid.olingo.web;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class ContextAwareHttpSerlvetRequest extends HttpServletRequestWrapper {
    private String contextPath;

    public ContextAwareHttpSerlvetRequest(HttpServletRequest delegate) {
        super(delegate);
    }

    public void setContextPath(String path) {
        this.contextPath = path;
    }

    @Override
    public StringBuffer getRequestURL() {
        //Workaround for https://issues.apache.org/jira/browse/OLINGO-1324
        StringBuffer result = super.getRequestURL();
        if (result.charAt(result.length()-1) == '/') {
            result.setLength(result.length() - 1);
        }
        return result;
    }

    @Override
    public String getContextPath() {
        if (contextPath != null) {
            return contextPath;
        }
        return super.getContextPath();
    }
}
