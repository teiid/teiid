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

package org.teiid.translator.salesforce.execution;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.translator.TranslatorException;

/**
 *
 * The structure of the getUpdated procedure is:
 * Salesforce object type: String: IN param
 * startDate: datatime: IN param
 * enddate: datetime: IN param
 * latestDateCovered: datetime: OUT param
 * getUpdatedResult: resultset: OUT param
 *
 */

public class GetUpdatedExecutionImpl implements SalesforceProcedureExecution {

    private ProcedureExecutionParent parent;
    private UpdatedResult updatedResult;
    private int idIndex = 0;
    DatatypeFactory factory;

    public GetUpdatedExecutionImpl(
            ProcedureExecutionParent procedureExecutionParent) throws TranslatorException {
        this.parent = procedureExecutionParent;
        try {
            factory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new TranslatorException(e.getMessage());
        }
    }

    @Override
    public void cancel() {
        // nothing to do here
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public void execute(ProcedureExecutionParent procedureExecutionParent) throws TranslatorException {
        Call command = parent.getCommand();
        List<Argument> params = command.getArguments();

        Argument object = params.get(OBJECT);
        String objectName = (String) object.getArgumentValue().getValue();

        Argument start = params.get(STARTDATE);
        Timestamp startTime = (Timestamp) start.getArgumentValue().getValue();
        GregorianCalendar startCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
        startCalendar.setTime(startTime);

        Argument end = params.get(ENDDATE);
        Timestamp endTime = (Timestamp) end.getArgumentValue().getValue();
        GregorianCalendar endCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
        endCalendar.setTime(endTime);

        updatedResult = parent.getConnection().getUpdated(objectName, startCalendar, endCalendar);
    }

    @Override
    public List<Timestamp> getOutputParameterValues() {
        List<Timestamp> result = new ArrayList<Timestamp>(1);
        result.add(new Timestamp(updatedResult.getLatestDateCovered().getTimeInMillis()));
        return result;
    }

    @Override
    public List<?> next() {
        List<Object> result = null;
        if(updatedResult.getIDs() != null && idIndex < updatedResult.getIDs().size()){
            result = new ArrayList<Object>(1);
            result.add(updatedResult.getIDs().get(idIndex));
            idIndex++;
        }
        return result;
    }

}
