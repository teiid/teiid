/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.vdb.materialization.template;

import java.io.Reader;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.language.DefaultTemplateLexer;

import com.metamatrix.vdb.materialization.DatabaseDialect;


/** 
 * Uses a set of data values and templates to generate an expanded template.
 * 
 * For example, this is used to generate a script file from a template.
 * 
 * Hides all of the 3rd party classes used in the template processing.
 * 
 * @since 4.2
 */
public class TemplateExpander {
    private TemplateData data;
    private DatabaseDialect database;

    public TemplateExpander(TemplateData data, DatabaseDialect database) {
        this.data = data;
        this.database = database;
    }
    
    /**
     * Produce the name and contents of an expanded template. 
     * 
     * @param nameTemplate is a template for the name of the expanded template.
     * @param templateReaders are the Reader objects that provide the contents of the templates to expand 
     * to produce the results.
     * @param templateName is the name of the main template to invoke from all of the templates provided 
     * by the templateReaders.
     * @return the ExpandedTemplate.
     * @since 4.2
     */
    public ExpandedTemplate expand( String nameTemplate, Reader[] templateReaders, String templateName ) {
        return new ExpandedTemplate( expandText( nameTemplate ), expandFromReaders( templateReaders, templateName ), this.database.getType());
    }
     
    private String expandFromReaders(Reader[] readers, String templateName ) {        
        StringTemplateGroup group = new StringTemplateGroup( readers[0] , DefaultTemplateLexer.class);
        for (int i=1; i<readers.length; i++) {
            StringTemplateGroup tempGroup = group;
            group = new StringTemplateGroup( readers[i] , DefaultTemplateLexer.class);
            group.setSuperGroup(tempGroup);            
        }
        return processTemplate( group.getInstanceOf( templateName ) ); 
    }

    public String expandText(String templateText) {
        return processTemplate(new StringTemplate(templateText));
    }

    private String processTemplate(final StringTemplate template) {
        data.populateTemplate( new Template() {
            public void setAttribute(String attributeName, Object value) {
                template.setAttribute(attributeName, value);
            }
        }, database );
        return template.toString();
    }
}
