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
package org.teiid.jboss;

import static junit.framework.Assert.assertEquals;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;
import static org.junit.Assert.fail;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import junit.framework.Assert;

import org.jboss.as.controller.*;
import org.jboss.as.controller.client.OperationBuilder;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.common.CommonProviders;
import org.jboss.as.controller.operations.global.GlobalOperationHandlers;
import org.jboss.as.controller.persistence.ConfigurationPersistenceException;
import org.jboss.as.controller.persistence.ConfigurationPersister;
import org.jboss.as.controller.persistence.SubsystemMarshallingContext;
import org.jboss.as.controller.registry.ModelNodeRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.dmr.Property;
import org.jboss.staxmapper.XMLElementWriter;
import org.jboss.staxmapper.XMLMapper;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

@SuppressWarnings("nls")
public class TestTeiidConfiguration {
    static ModelNode profileAddress = new ModelNode();
    static {
        profileAddress.add("profile", "test");
    }
    
	@Test
	public void testValidateSchema() throws Exception {
		InputStream content = Thread.currentThread().getContextClassLoader().getResourceAsStream("teiid-sample-config.xml");
		URL xsdURL = Thread.currentThread().getContextClassLoader().getResource("schema/jboss-teiid.xsd");
			
		SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
		Schema schema = factory.newSchema(xsdURL);
		
		Validator validator = schema.newValidator();
		Source source = new StreamSource(content);
		validator.setErrorHandler(new ErrorHandler() {
			
			@Override
			public void warning(SAXParseException exception) throws SAXException {
				fail(exception.getMessage());
			}
			
			@Override
			public void fatalError(SAXParseException exception) throws SAXException {
				fail(exception.getMessage());
			}
			
			@Override
			public void error(SAXParseException exception) throws SAXException {
				fail(exception.getMessage());
			}
		});
		
		validator.validate(source);

	}
	
    @Test
    public void testTeiidConfiguration() throws Exception {
        List<ModelNode> updates = createSubSystem(ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-sample-config.xml")));
        assertEquals(1, updates.size());
        for (ModelNode update : updates) {
            try {
                controller.executeForResult(update);
            } catch (OperationFailedException e) {
                throw new RuntimeException(e.getFailureDescription().toString());
            }
        }

        ModelNode subsystem = model.require("profile").require("test").require("subsystem").require("teiid");
        ModelNode bufferService = subsystem.require("buffer-service");
        assertEquals(9, bufferService.keys().size());
        assertEquals("true", bufferService.require("useDisk").asString());
    }	
    
    
    private ModelNode model;
    private TestController controller;
    @Before
    public void setup() throws Exception {
        model = new ModelNode();
        controller = new TestController();
        model.get("profile", "test", "subsystem");

        final ModelNodeRegistration testProfileRegistration = controller.getRegistry().registerSubModel(PathElement.pathElement("profile", "*"), new DescriptionProvider() {

            @Override
            public ModelNode getModelDescription(Locale locale) {
                ModelNode node = new ModelNode();
                node.get(DESCRIPTION).set("A named set of subsystem configs");
                node.get(ATTRIBUTES, NAME, TYPE).set(ModelType.STRING);
                node.get(ATTRIBUTES, NAME, DESCRIPTION).set("The name of the profile");
                node.get(ATTRIBUTES, NAME, REQUIRED).set(true);
                node.get(ATTRIBUTES, NAME, MIN_LENGTH).set(1);
                node.get(CHILDREN, SUBSYSTEM, DESCRIPTION).set("The subsystems that make up the profile");
                node.get(CHILDREN, SUBSYSTEM, MIN_OCCURS).set(1);
                node.get(CHILDREN, SUBSYSTEM, MODEL_DESCRIPTION);
                return node;
            }
        });

        TestNewExtensionContext context = new TestNewExtensionContext(testProfileRegistration);
        TeiidExtension extension = new TeiidExtension();
        extension.initialize(context);
        Assert.assertNotNull(context.createdRegistration);

    }      
	
    static List<ModelNode> createSubSystem(String xmlContent) throws XMLStreamException {

        final Reader reader = new StringReader(xmlContent);
        XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(reader);

        XMLMapper xmlMapper = XMLMapper.Factory.create();
        xmlMapper.registerRootElement(new QName(Namespace.CURRENT.getUri(), "subsystem") , new TeiidSubsystemParser());

        List<ModelNode> updates = new ArrayList<ModelNode>();
        xmlMapper.parseDocument(updates, xmlReader);

        // Process subsystems
        for(final ModelNode update : updates) {
            // Process relative subsystem path address
            final ModelNode subsystemAddress = profileAddress.clone();
            for(final Property path : update.get(OP_ADDR).asPropertyList()) {
                subsystemAddress.add(path.getName(), path.getValue().asString());
            }
            update.get(OP_ADDR).set(subsystemAddress);
        }

        return updates;
    }	
    
    static class TestNewExtensionContext implements ExtensionContext {
        final ModelNodeRegistration testProfileRegistration;
        ModelNodeRegistration createdRegistration;

        TestNewExtensionContext(ModelNodeRegistration testProfileRegistration) {
            this.testProfileRegistration = testProfileRegistration;
        }

        @Override
        public SubsystemRegistration registerSubsystem(final String name) throws IllegalArgumentException {
            return new SubsystemRegistration() {
                @Override
                public ModelNodeRegistration registerSubsystemModel(final DescriptionProvider descriptionProvider) {
                    if (descriptionProvider == null) {
                        throw new IllegalArgumentException("descriptionProvider is null");
                    }
                    createdRegistration = testProfileRegistration.registerSubModel(PathElement.pathElement("subsystem", name), descriptionProvider);
                    Assert.assertEquals("teiid", name);
                    return createdRegistration;
                }

                @Override
                public ModelNodeRegistration registerDeploymentModel(final DescriptionProvider descriptionProvider) {
                    throw new IllegalStateException("Not implemented");
                }

                @Override
                public void registerXMLElementWriter(XMLElementWriter<SubsystemMarshallingContext> writer) {
                    Assert.assertNotNull(writer);
                }
            };
        }
    }
    
    class TestController extends BasicModelController {

        protected TestController() {
            super(model, new ConfigurationPersister() {
                @Override
                public void store(ModelNode model) throws ConfigurationPersistenceException {
                }
                @Override
                public void marshallAsXml(ModelNode model, OutputStream output) throws ConfigurationPersistenceException {
                }
                @Override
                public List<ModelNode> load() throws ConfigurationPersistenceException {
                    return null;
                }
                @Override
                public void successfulBoot() throws ConfigurationPersistenceException {
                }
                @Override
                public String snapshot() {
                    return null;
                }
                @Override
                public SnapshotInfo listSnapshots() {
                    return NULL_SNAPSHOT_INFO;
                }
                @Override
                public void deleteSnapshot(String name) {
                }
            }, new DescriptionProvider() {
                @Override
                public ModelNode getModelDescription(Locale locale) {
                    ModelNode node = new ModelNode();
                    node.get(DESCRIPTION).set("The root node of the test management API");
                    node.get(CHILDREN, PROFILE, DESCRIPTION).set("A list of profiles");
                    node.get(CHILDREN, PROFILE, MIN_OCCURS).set(1);
                    node.get(CHILDREN, PROFILE, MODEL_DESCRIPTION);
                    return node;
                }
            });

            getRegistry().registerOperationHandler(READ_RESOURCE_DESCRIPTION_OPERATION, GlobalOperationHandlers.READ_RESOURCE_DESCRIPTION, CommonProviders.READ_RESOURCE_DESCRIPTION_PROVIDER, true);
        }

        @Override
        protected ModelNodeRegistration getRegistry() {
            return super.getRegistry();
        }

        /**
         * Override to get the actual result from the response.
         */
        public ModelNode executeForResult(ModelNode operation) throws OperationFailedException {
            ModelNode rsp = super.execute(OperationBuilder.Factory.create(operation).build());
            if (FAILED.equals(rsp.get(OUTCOME).asString())) {
                throw new OperationFailedException(rsp.get(FAILURE_DESCRIPTION));
            }
            return rsp.get(RESULT);
        }
    }
    
    @Test
    public void testSubSystemDescription() throws IOException {
    	TeiidSubsystemDescription tsd = new TeiidSubsystemDescription();
    	assertEquals(ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-model-config.txt")), tsd.getModelDescription(null).toString());
    }
}
