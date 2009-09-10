package org.teiid.connector.metadata.runtime;

import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.connector.basic.BasicConnectorCapabilities;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


public class MockConnector extends BasicConnector {
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		return new BasicConnectorCapabilities();
	}
	
	@Override
	public Connection getConnection(ExecutionContext context) throws ConnectorException {
		return new BasicConnection() {
			@Override
			public void close() {
				
			}
			
			@Override
			public ProcedureExecution createProcedureExecution(
					IProcedure procedure, ExecutionContext executionContext,
					RuntimeMetadata metadata) throws ConnectorException {
				Properties props = new Properties();
				props.setProperty("customBehaviour", "SkipExecute");//$NON-NLS-1$ //$NON-NLS-2$
			
		        MetadataObject metaObject = procedure.getMetadataObject();
		        
		        TestCase.assertEquals("AnyModel.ProcedureB",procedure.getProcedureName()); //$NON-NLS-1$
		        TestCase.assertEquals("PROC", metaObject.getNameInSource()); //$NON-NLS-1$
		        TestCase.assertEquals(props, metaObject.getProperties());
		        ProcedureExecution exec = Mockito.mock(ProcedureExecution.class);
		        Mockito.stub(exec.next()).toReturn(null);
		        return exec;
			}
			
			@Override
			public ResultSetExecution createResultSetExecution(
					IQueryCommand query, ExecutionContext executionContext,
					RuntimeMetadata metadata) throws ConnectorException {
				Properties groupProps = new Properties();
				groupProps.setProperty("customName", "CustomTableA");//$NON-NLS-1$ //$NON-NLS-2$
				IGroup group = (IGroup)query.getProjectedQuery().getFrom().getItems().get(0);			
				MetadataObject groupMD = group.getMetadataObject();
				TestCase.assertEquals(groupProps, groupMD.getProperties());
				
				
				ISelectSymbol symbl = (ISelectSymbol)query.getProjectedQuery().getSelect().getSelectSymbols().get(0);
				IElement element = (IElement)symbl.getExpression();
				Element elementMD = element.getMetadataObject();

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
				Element elementMD2 = element2.getMetadataObject();

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
				ResultSetExecution exec = Mockito.mock(ResultSetExecution.class);
		        Mockito.stub(exec.next()).toReturn(null);
		        return exec;
			}
			
		};
	}

	@Override
	public void start(ConnectorEnvironment environment)
			throws ConnectorException {
	}

	@Override
	public void stop() {
	}

}
