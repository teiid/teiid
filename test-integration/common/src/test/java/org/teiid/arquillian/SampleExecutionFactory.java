package org.teiid.arquillian;

import org.teiid.translator.ExecutionFactory;

@org.teiid.translator.Translator(name = "orcl")
public class SampleExecutionFactory extends ExecutionFactory<Object, Object> {
	public SampleExecutionFactory() {
		setSupportsSelectDistinct(true);
	}
}