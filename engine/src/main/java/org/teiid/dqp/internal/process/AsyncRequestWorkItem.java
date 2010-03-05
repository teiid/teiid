package org.teiid.dqp.internal.process;

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;

public class AsyncRequestWorkItem extends RequestWorkItem {

	public AsyncRequestWorkItem(DQPCore dqpCore, RequestMessage requestMsg,
			Request request, ResultsReceiver<ResultsMessage> receiver,
			RequestID requestID, DQPWorkContext workContext) {
		super(dqpCore, requestMsg, request, receiver, requestID, workContext);
	}
	
	
	@Override
	protected void resumeProcessing() {
		dqpCore.addWork(this);
	}
	
	@Override
	protected void pauseProcessing() {
	}
}
