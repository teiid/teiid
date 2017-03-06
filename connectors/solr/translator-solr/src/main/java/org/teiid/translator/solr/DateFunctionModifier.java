package org.teiid.translator.solr;

import java.util.List;
import org.teiid.language.Function;
import org.teiid.translator.jdbc.FunctionModifier;

public class DateFunctionModifier extends FunctionModifier {

	@Override
	public List<?> translate(Function function) {
		function.setName("");
		function.getParameters().remove(1);
		return null;
	}

}