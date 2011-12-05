package org.teiid.arquillian;

import org.teiid.translator.loopback.LoopbackExecutionFactory;

@org.teiid.translator.Translator(name = "loopy")
public class SampleExecutionFactory extends LoopbackExecutionFactory {
	public SampleExecutionFactory() {
		setSupportsSelectDistinct(true);
		setWaitTime(10);
		setRowCount(200);
	}
}