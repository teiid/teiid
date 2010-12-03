package org.teiid.query.function.metadata;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.teiid.metadata.FunctionMethod;

@XmlType
@XmlRootElement(namespace="http://www.omg.org/XMI", name="XMI")
public class FunctionMetadataReader {

	@XmlElement(namespace="http://www.metamatrix.com/metamodels/MetaMatrixFunction", name="ScalarFunction")
	List<FunctionMethod> functionMethods = new ArrayList<FunctionMethod>();
	
	public static List<FunctionMethod> loadFunctionMethods(InputStream source) throws JAXBException {
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {FunctionMetadataReader.class});
		Unmarshaller marshaller = jc.createUnmarshaller();
		FunctionMetadataReader md = (FunctionMetadataReader) marshaller.unmarshal(source);
		return md.functionMethods;
	}
	
}
