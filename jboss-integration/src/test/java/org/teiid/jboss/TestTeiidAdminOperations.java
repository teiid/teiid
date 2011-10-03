package org.teiid.jboss;

import static junit.framework.Assert.assertEquals;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import junit.framework.Assert;

import org.jboss.as.controller.OperationContext.Type;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.as.subsystem.test.AdditionalInitialization;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

@SuppressWarnings("nls")
public class TestTeiidAdminOperations extends AbstractSubsystemTest {

	public TestTeiidAdminOperations() {
		super(TeiidExtension.TEIID_SUBSYSTEM, new TeiidExtension());
	}

    @Test
    public void testDescribeHandler() throws Exception {
        //Parse the subsystem xml and install into the first controller
        String subsystemXml ="<subsystem xmlns=\"urn:jboss:domain:teiid:1.0\">" +
        		"<async-thread-group>async</async-thread-group>"+
        		" <query-engine name=\"default\">" +
        		" </query-engine>" +
        		"</subsystem>";
        KernelServices servicesA = super.installInController(subsystemXml);
        //Get the model and the describe operations from the first controller
        ModelNode modelA = servicesA.readWholeModel();
        String marshalled = servicesA.getPersistedSubsystemXml();
        
        ModelNode describeOp = new ModelNode();
        describeOp.get(OP).set(DESCRIBE);
        describeOp.get(OP_ADDR).set(
                PathAddress.pathAddress(
                        PathElement.pathElement(SUBSYSTEM, TeiidExtension.TEIID_SUBSYSTEM)).toModelNode());
        List<ModelNode> operations = super.checkResultAndGetContents(servicesA.executeOperation(describeOp)).asList();


        //Install the describe options from the first controller into a second controller
        KernelServices servicesB = super.installInController(operations);
        ModelNode modelB = servicesB.readWholeModel();

        //Make sure the models from the two controllers are identical
        super.compare(modelA, modelB);
    }

    @Test
    public void testOutputPerisitence() throws Exception {
    	String subsystemXml = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-sample-config.xml"));

    	String json = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-model-json.txt"));
    	ModelNode testModel = ModelNode.fromJSONString(json);
        String triggered = outputModel(testModel);

        KernelServices services = super.installInController(
                new AdditionalInitialization() {
                    @Override
                    protected Type getType() {
                        return Type.MANAGEMENT;
                    }
                },
                subsystemXml);
        //Get the model and the persisted xml from the controller
        ModelNode model = services.readWholeModel();
        String marshalled = services.getPersistedSubsystemXml();

        System.out.println(marshalled);
        
        Assert.assertEquals(marshalled, triggered);
        Assert.assertEquals(normalizeXML(marshalled), normalizeXML(triggered));
    }
    
    @Test
    public void testOutputModel() throws Exception {
    	String subsystemXml = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-sample-config.xml"));

    	String json = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-model-json.txt"));
    	ModelNode testModel = ModelNode.fromJSONString(json);
        String triggered = outputModel(testModel);

        KernelServices services = super.installInController(
                new AdditionalInitialization() {
                    @Override
                    protected Type getType() {
                        return Type.MANAGEMENT;
                    }
                },
                subsystemXml);
        //Get the model and the persisted xml from the controller
        ModelNode model = services.readWholeModel();
        String marshalled = services.getPersistedSubsystemXml();

        Assert.assertEquals(marshalled, triggered);
        Assert.assertEquals(normalizeXML(marshalled), normalizeXML(triggered));
    }    
    
    @Test
    public void testSchema() throws Exception {
    	String subsystemXml = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-sample-config.xml"));
        KernelServices services = super.installInController(
                new AdditionalInitialization() {
                    @Override
                    protected Type getType() {
                        return Type.MANAGEMENT;
                    }
                },
                subsystemXml);
        //Get the model and the persisted xml from the controller
        ModelNode model = services.readWholeModel();
        String marshalled = services.getPersistedSubsystemXml();

		URL xsdURL = Thread.currentThread().getContextClassLoader().getResource("schema/jboss-teiid.xsd");
		
		SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
		Schema schema = factory.newSchema(xsdURL);
		
		Validator validator = schema.newValidator();
		Source source = new StreamSource(new ByteArrayInputStream(marshalled.getBytes()));
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
				if (!exception.getMessage().contains("cvc-enumeration-valid") && !exception.getMessage().contains("cvc-type"))
					fail(exception.getMessage());
			}
		});
		
		validator.validate(source);
    }
    
    @Test
    public void testSubSystemDescription() throws IOException {
    	ModelNode node = new ModelNode();
    	QueryEngineAdd.describeQueryEngine(node, ATTRIBUTES, IntegrationPlugin.getResourceBundle(null));
    	assertEquals(ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-model-config.txt")), node.toString());
    }
    
    @Test
    public void testParseSubsystem() throws Exception {
        //Parse the subsystem xml into operations
    	String subsystemXml = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-sample-config.xml"));
        List<ModelNode> operations = super.parse(subsystemXml);

        ///Check that we have the expected number of operations
        Assert.assertEquals(5, operations.size());

        //Check that each operation has the correct content
        ModelNode addSubsystem = operations.get(0);
        Assert.assertEquals(ADD, addSubsystem.get(OP).asString());
        PathAddress addr = PathAddress.pathAddress(addSubsystem.get(OP_ADDR));
        Assert.assertEquals(1, addr.size());
        PathElement element = addr.getElement(0);
        Assert.assertEquals(SUBSYSTEM, element.getKey());
        Assert.assertEquals(TeiidExtension.TEIID_SUBSYSTEM, element.getValue());
    }
    
    @Test
    public void testQueryOperatrions() throws Exception {
    	String subsystemXml = ObjectConverterUtil.convertToString(new FileReader("src/test/resources/teiid-sample-config.xml"));

        KernelServices services = super.installInController(
                new AdditionalInitialization() {
                    @Override
                    protected Type getType() {
                        return Type.MANAGEMENT;
                    }
                },
                subsystemXml);
        
        PathAddress addr = PathAddress.pathAddress(
                PathElement.pathElement(SUBSYSTEM, TeiidExtension.TEIID_SUBSYSTEM),
                PathElement.pathElement("query-engine", "default"));
        ModelNode addOp = new ModelNode();
        addOp.get(OP).set("read-operation-names");
        addOp.get(OP_ADDR).set(addr.toModelNode());
        
        ModelNode result = services.executeOperation(addOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        
        List<String> opNames = getList(result);
        assertEquals(22, opNames.size());
        String [] ops = {"add", "cancel-request", "execute-query", "list-requests", "list-sessions", "list-transactions", 
        		"long-running-queries", "read-attribute", "read-children-names", "read-children-resources", 
        		"read-children-types", "read-operation-description", "read-operation-names", "read-resource", 
        		"read-resource-description", "remove", "requests-per-session", "requests-per-vdb", 
        		"terminate-session", "terminate-transaction", "workerpool-statistics", "write-attribute"};
        assertEquals(Arrays.asList(ops), opNames);
    }
    
    private static List<String> getList(ModelNode operationResult) {
        if(!operationResult.hasDefined("result"))
            return Collections.emptyList();

        List<ModelNode> nodeList = operationResult.get("result").asList();
        if(nodeList.isEmpty())
            return Collections.emptyList();

        List<String> list = new ArrayList<String>(nodeList.size());
        for(ModelNode node : nodeList) {
            list.add(node.asString());
        }
        return list;
    }    
}
