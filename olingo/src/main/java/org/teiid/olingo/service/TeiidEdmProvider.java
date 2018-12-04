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
package org.teiid.olingo.service;

import java.io.InputStreamReader;
import java.net.URI;

import javax.xml.stream.XMLStreamException;

import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression.ConstantExpressionType;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.commons.api.edmx.EdmxReferenceInclude;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.server.core.MetadataParser;
import org.apache.olingo.server.core.SchemaBasedEdmProvider;

public class TeiidEdmProvider extends SchemaBasedEdmProvider {
    
    private String baseUri;
    
    public TeiidEdmProvider(String baseUri, CsdlSchema schema,
            String invalidXmlReplacementChar) throws XMLStreamException {
        
        this.baseUri = baseUri;
        
        EdmxReference olingoRef = new EdmxReference(URI.create(baseUri+"/static/org.apache.olingo.v1.xml"));
        EdmxReferenceInclude include = new EdmxReferenceInclude("org.apache.olingo.v1", "olingo-extensions");
        olingoRef.addInclude(include);
        addReference(olingoRef);
        
        MetadataParser parser = new MetadataParser();
        parser.parseAnnotations(true);
        parser.useLocalCoreVocabularies(true);
        parser.implicitlyLoadCoreVocabularies(true);
        SchemaBasedEdmProvider provider = parser.buildEdmProvider(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("org.apache.olingo.v1.xml")));
        addVocabularySchema("org.apache.olingo.v1", provider);
        
        // <Annotation Term="org.apache.olingo.v1.xml10-incompatible-char-replacement" String="xxx"/>
        if (invalidXmlReplacementChar != null) {
            CsdlAnnotation xmlCharReplacement = new CsdlAnnotation();
            xmlCharReplacement.setTerm("org.apache.olingo.v1.xml10-incompatible-char-replacement");
            xmlCharReplacement.setExpression(new CsdlConstantExpression(
                    ConstantExpressionType.String, invalidXmlReplacementChar));
            schema.getAnnotations().add(xmlCharReplacement);
        }
        addSchema(schema);        
    }
        
    public void addReferenceSchema(String vdbName, String ns, String alias, SchemaBasedEdmProvider provider) {
        super.addReferenceSchema(ns, provider);
        String uri = baseUri + "/" + vdbName + "/" + alias + "/$metadata";
        super.addReference(new EdmxReference(URI.create(uri))
                .addInclude(new EdmxReferenceInclude(ns, alias)));
    }
    
}

