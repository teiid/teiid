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

package org.teiid.jboss.rest;

import java.util.Enumeration;

import javax.servlet.FilterConfig;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.teiid.core.util.StringUtil;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.swagger.annotations.ApiOperation;
import io.swagger.jaxrs.listing.BaseApiListingResource;

/**
 * Workaround for using a Resteasy Filter to make a ServletContext available
 * https://github.com/swagger-api/swagger-core/issues/2239
 */

@Path("/swagger.{type:json|yaml}")
public class CustomApiListingResource extends BaseApiListingResource {

    @Context
    ServletContext context;

    @GET
    @Produces({MediaType.APPLICATION_JSON, "application/yaml"})
    @ApiOperation(value = "The swagger definition in either JSON or YAML", hidden = true)
    public Response getListing(
            @Context Application app,
            @Context ServletConfig sc,
            @Context FilterConfig fc,
            @Context HttpHeaders headers,
            @Context UriInfo uriInfo,
            @PathParam("type") String type) throws JsonProcessingException {
        sc = getConfig(sc, fc);
        if (!StringUtil.isEmpty(type) && type.trim().equalsIgnoreCase("yaml")) {
            return getListingYamlResponse(app, context, sc, headers, uriInfo);
        } else {
            return getListingJsonResponse(app, context, sc, headers, uriInfo);
        }
    }

    private ServletConfig getConfig(ServletConfig sc, FilterConfig fc) {
        try {
            sc.getInitParameter("test"); //$NON-NLS-1$
        } catch (Exception e) {
            return new ServletConfigWrapper(fc);
        }
        return sc;
    }

    private static class ServletConfigWrapper implements ServletConfig {

        private FilterConfig fc;

        public ServletConfigWrapper(FilterConfig fc) {
            this.fc = fc;
        }

        @Override
        public String getServletName() {
            return fc.getFilterName();
        }

        @Override
        public ServletContext getServletContext() {
            return fc.getServletContext();
        }

        @Override
        public String getInitParameter(String name) {
            return fc.getInitParameter(name);
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return fc.getInitParameterNames();
        }
    }

}
