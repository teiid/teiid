package org.teiid.common.queue;

import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;

import org.mockito.Mockito;

public class FakeWorkManager implements WorkManager {
	private Thread t;
	
	@Override
	public void doWork(Work arg0) throws WorkException {
		execute(arg0, null, true);
	}

	@Override
	public void doWork(Work arg0, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
		execute(arg0, arg3, true);
	}

	@Override
	public void scheduleWork(Work arg0) throws WorkException {
		execute(arg0, null, false);
	}

	@Override
	public void scheduleWork(Work arg0, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
		execute(arg0, arg3, false);
	}

	@Override
	public long startWork(Work arg0) throws WorkException {
		execute(arg0, null, false);
		return 0;
	}

	@Override
	public long startWork(Work arg0, long arg1, ExecutionContext arg2, WorkListener arg3) throws WorkException {
		execute(arg0, arg3, false);
		return 0;
	}

	void execute(final Work arg0, final WorkListener arg3, boolean join) throws WorkException {
		if (arg3 != null) {
			arg3.workAccepted(Mockito.mock(WorkEvent.class));
			arg3.workStarted(Mockito.mock(WorkEvent.class));
		}
		
		t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				arg0.run();
				if (arg3 != null) {
					arg3.workCompleted(Mockito.mock(WorkEvent.class));
				}							
			}
		});
		t.start();
		if (join) {
			try {
				t.join();
			} catch (InterruptedException e) {
				throw new WorkException(e);
			}
		}
	}
		
}
