package com.metamatrix.data.metadata.runtime;

import java.util.Arrays;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.GlobalCapabilitiesProvider;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.basic.BasicConnectorCapabilities;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.language.IQueryCommand;
import com.metamatrix.data.language.ISelectSymbol;

public class MockConnector implements Connector, GlobalCapabilitiesProvider{

	RuntimeMetadata metadata;
	
	public Execution createExecution(int executionMode, @SuppressWarnings("unused") ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		this.metadata = metadata;
		if (executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND) {
			return SimpleMock.createSimpleMock(this, SynchQueryCommandExecution.class);
		}
		else if (executionMode == ConnectorCapabilities.EXECUTION_MODE.PROCEDURE) {
			return SimpleMock.createSimpleMock(this, ProcedureExecution.class);
		}
		return null;
	}
	
	public ConnectorCapabilities getCapabilities() {
		Object[] actuals = new Object[] {this, new BasicConnectorCapabilities()};
		return SimpleMock.createSimpleMock(actuals, ConnectorCapabilities.class);
	}
	
	public void execute( IQueryCommand query, @SuppressWarnings("unused") int maxBatchSize ) throws ConnectorException {
		Properties groupProps = new Properties();
		groupProps.setProperty("customName", "CustomTableA");//$NON-NLS-1$ //$NON-NLS-2$
		IGroup group = (IGroup)query.getProjectedQuery().getFrom().getItems().get(0);			
		MetadataObject groupMD = this.metadata.getObject(group.getMetadataID());
		TestCase.assertEquals(groupProps, groupMD.getProperties());
		
		
		ISelectSymbol symbl = (ISelectSymbol)query.getProjectedQuery().getSelect().getSelectSymbols().get(0);
		IElement element = (IElement)symbl.getExpression();
		Element elementMD = (Element)this.metadata.getObject(element.getMetadataID());

		Properties elementProps = new Properties();
		elementProps.setProperty("customPosition", "11");//$NON-NLS-1$ //$NON-NLS-2$
		
		TestCase.assertEquals(0, elementMD.getLength());
		TestCase.assertEquals("Foo", elementMD.getDefaultValue()); //$NON-NLS-1$
		TestCase.assertEquals("TrimNulls", elementMD.getFormat()); //$NON-NLS-1$
		TestCase.assertEquals(String.class, elementMD.getJavaType()); 
		TestCase.assertEquals(null, elementMD.getMaximumValue());
		TestCase.assertEquals(null, elementMD.getMinimumValue());
		TestCase.assertEquals("http://www.w3.org/2001/XMLSchema#anySimpleType", elementMD.getModeledBaseType()); //$NON-NLS-1$
		TestCase.assertEquals("http://www.w3.org/2001/XMLSchema#string", elementMD.getModeledPrimitiveType()); //$NON-NLS-1$
		TestCase.assertEquals("http://www.w3.org/2001/XMLSchema#string", elementMD.getModeledType()); //$NON-NLS-1$
		TestCase.assertEquals("COLUMN1", elementMD.getNameInSource()); //$NON-NLS-1$
		TestCase.assertEquals("STR", elementMD.getNativeType()); //$NON-NLS-1$
		TestCase.assertEquals(1, elementMD.getNullability());
		TestCase.assertEquals(0, elementMD.getPosition());
		TestCase.assertEquals(0, elementMD.getPrecision());
		TestCase.assertEquals(0, elementMD.getScale());
		TestCase.assertEquals(3, elementMD.getSearchability());
		TestCase.assertEquals(false, elementMD.isAutoIncremented());
		TestCase.assertEquals(true, elementMD.isCaseSensitive());
		TestCase.assertEquals(elementProps, elementMD.getProperties());
		
		
		ISelectSymbol symbl2 = (ISelectSymbol)query.getProjectedQuery().getSelect().getSelectSymbols().get(1);
		IElement element2 = (IElement)symbl2.getExpression();
		Element elementMD2 = (Element)this.metadata.getObject(element2.getMetadataID());

		Properties elementProps2 = new Properties();
		elementProps2.setProperty("customPosition", "12");//$NON-NLS-1$ //$NON-NLS-2$
		
		TestCase.assertEquals(10, elementMD2.getLength());
		TestCase.assertEquals("23", elementMD2.getDefaultValue()); //$NON-NLS-1$
		TestCase.assertEquals("YesFormat", elementMD2.getFormat()); //$NON-NLS-1$
		TestCase.assertEquals(Integer.class, elementMD2.getJavaType());
		TestCase.assertEquals("1", elementMD2.getMaximumValue()); //$NON-NLS-1$
		TestCase.assertEquals("100", elementMD2.getMinimumValue()); //$NON-NLS-1$
		TestCase.assertEquals("http://www.w3.org/2001/XMLSchema#long", elementMD2.getModeledBaseType()); //$NON-NLS-1$
		TestCase.assertEquals("http://www.w3.org/2001/XMLSchema#decimal", elementMD2.getModeledPrimitiveType()); //$NON-NLS-1$
		TestCase.assertEquals("http://www.w3.org/2001/XMLSchema#int", elementMD2.getModeledType()); //$NON-NLS-1$
		TestCase.assertEquals("COLUMN2", elementMD2.getNameInSource()); //$NON-NLS-1$
		TestCase.assertEquals("INT", elementMD2.getNativeType()); //$NON-NLS-1$
		TestCase.assertEquals(0, elementMD2.getNullability());
		TestCase.assertEquals(1, elementMD2.getPosition());
		TestCase.assertEquals(0, elementMD2.getPrecision());
		TestCase.assertEquals(10, elementMD2.getScale());
		TestCase.assertEquals(3, elementMD2.getSearchability());
		TestCase.assertEquals(true, elementMD2.isAutoIncremented());
		TestCase.assertEquals(false, elementMD2.isCaseSensitive());
		
		TestCase.assertEquals(elementProps2, elementMD2.getProperties());
		
	}
	
	public void execute(IProcedure procedure, @SuppressWarnings("unused") int maxBatchSize) throws ConnectorException{
		Properties props = new Properties();
		props.setProperty("customBehaviour", "SkipExecute");//$NON-NLS-1$ //$NON-NLS-2$
	
        MetadataObject metaObject = this.metadata.getObject(procedure.getMetadataID());
        
        TestCase.assertEquals("AnyModel.ProcedureB",procedure.getProcedureName()); //$NON-NLS-1$
        TestCase.assertEquals("PROC", metaObject.getNameInSource()); //$NON-NLS-1$
        TestCase.assertEquals(props, metaObject.getProperties());
		
	}
	
	public Batch nextBatch() throws ConnectorException {
		BasicBatch b = new BasicBatch();
		b.addRow(Arrays.asList(new String[] {"A", "1"}));//$NON-NLS-1$ //$NON-NLS-2$
		b.addRow(Arrays.asList(new String[] {"B", "2"}));//$NON-NLS-1$ //$NON-NLS-2$
		b.setLast();
		return b;
	}		
	
	public boolean supportsExecutionMode(int executionMode) {
		if (executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND || executionMode == ConnectorCapabilities.EXECUTION_MODE.PROCEDURE) {
			return true;
		}
		return false;
	}
	
	
	@Override
	public Connection getConnection(SecurityContext context) throws ConnectorException {
		Connection conn = SimpleMock.createSimpleMock(this, Connection.class);
		return conn;
	}

	@Override
	public void initialize(ConnectorEnvironment environment)
			throws ConnectorException {
	}

	@Override
	public void start() throws ConnectorException {
	}

	@Override
	public void stop() {
	}

}
