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
package org.teiid.olingo.service;

import java.io.InputStreamReader;
import java.net.URI;

import javax.xml.stream.XMLStreamException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.CsdlTerm;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression.ConstantExpressionType;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.commons.api.edmx.EdmxReferenceInclude;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.server.core.MetadataParser;
import org.apache.olingo.server.core.SchemaBasedEdmProvider;
import org.teiid.core.TeiidRuntimeException;

public class TeiidEdmProvider extends SchemaBasedEdmProvider {

    private String baseUri;

    public TeiidEdmProvider(String baseUri, CsdlSchema schema,
            String invalidXmlReplacementChar) throws XMLStreamException {

        this.baseUri = baseUri;

        EdmxReference olingoRef = new EdmxReference(URI.create(baseUri+"/static/org.apache.olingo.v1.xml"));
        EdmxReferenceInclude include = new EdmxReferenceInclude("org.apache.olingo.v1", "olingo-extensions");
        olingoRef.addInclude(include);
        addReference(olingoRef);

        EdmxReference teiidRef = new EdmxReference(URI.create(baseUri+"/static/org.teiid.v1.xml"));
        EdmxReferenceInclude teiidInclude = new EdmxReferenceInclude("org.teiid.v1", "teiid");
        teiidRef.addInclude(teiidInclude);
        addReference(teiidRef);

        MetadataParser parser = new MetadataParser();
        parser.parseAnnotations(true);
        parser.useLocalCoreVocabularies(true);
        parser.implicitlyLoadCoreVocabularies(true);
        SchemaBasedEdmProvider provider = parser.buildEdmProvider(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("org.apache.olingo.v1.xml")));
        addVocabularySchema("org.apache.olingo.v1", provider);

        provider = parser.buildEdmProvider(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("org.teiid.v1.xml")));
        addVocabularySchema("org.teiid.v1", provider);

        provider = parser.buildEdmProvider(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("Org.OData.Core.V1.xml")));
        addVocabularySchema("Org.OData.Core.V1", provider);

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

    @Override
    public CsdlTerm getTerm(FullQualifiedName fqn) throws ODataException {
        CsdlTerm term = super.getTerm(fqn);
        if (term == null) {
            throw new TeiidRuntimeException(fqn.toString() + " unknown term"); //$NON-NLS-1$
        }
        return term;
    }
}

