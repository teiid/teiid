package org.teiid.arquillian;

import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class SampleMetadataRepository implements MetadataRepository {

    @Override
    public void loadMetadata(MetadataFactory factory,
            ExecutionFactory executionFactory, Object connectionFactory)
            throws TranslatorException {
        Table x = factory.addTable("xxx");
        x.setVirtual(true);
        x.setTableType(Type.View);
        x.setSelectTransformation("select 'a'");
        factory.addColumn("y", "string", x);
    }
}
