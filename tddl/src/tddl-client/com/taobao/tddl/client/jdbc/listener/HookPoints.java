package com.taobao.tddl.client.jdbc.listener;

/*
 * @author guangxia
 * @since 1.0, 2010-4-12 下午02:19:17
 */
public class HookPoints {
	
    private Handler beforeExecute = Handler.DUMMY_HANDLER;
    private Handler afterExecute = Handler.DUMMY_HANDLER;
//    private Handler afterRollBack = Handler.DUMMY_HANDLER;
    
	public void setBeforeExecute(Handler beforeExecute) {
		this.beforeExecute = beforeExecute;
	}
	public void appendBeforeExecuteFinally(Handler beforeExecute) {
		if(this.beforeExecute == Handler.DUMMY_HANDLER) {
			this.beforeExecute = beforeExecute;
		} else {
			this.beforeExecute.appendFinally(beforeExecute);
		}
	}
	public Handler getBeforeExecute() {
		return beforeExecute;
	}
	public void setAfterExecute(Handler afterExecute) {
		this.afterExecute = afterExecute;
	}
	public void appendAfterExecuteFinally(Handler afterExecute) {
		if(this.afterExecute == Handler.DUMMY_HANDLER) {
			this.afterExecute = afterExecute;
		} else {
			this.afterExecute.appendFinally(afterExecute);
		}
	}
	public Handler getAfterExecute() {
		return afterExecute;
	}
//	public void setAfterRollBack(Handler afterRollBack) {
//		this.afterRollBack = afterRollBack;
//	}
//	public void appendAfterRollBackFinally(Handler afterRollBack) {
//		if(this.afterRollBack == Handler.DUMMY_HANDLER) {
//			this.setAfterRollBack(afterRollBack);
//		} else {
//			this.afterRollBack.appendFinally(afterRollBack);
//		}
//	}
//	public Handler getAfterRollBack() {
//		return afterRollBack;
//	}
    
    public final static HookPoints DEFAULT = new HookPoints() {
    	@Override
    	public void setBeforeExecute(Handler beforeExecute) {
    		throw new UnsupportedOperationException();
    	}
    	@Override
    	public void setAfterExecute(Handler afterExecute) {
    		throw new UnsupportedOperationException();
    	}
//    	@Override
//    	public void setAfterRollBack(Handler afterRollBack) {
//    		throw new UnsupportedOperationException();
//    	}
    	@Override
    	public void appendBeforeExecuteFinally(Handler beforeExecute) {
    		throw new UnsupportedOperationException();
    	}
    	@Override
    	public void appendAfterExecuteFinally(Handler afterExecute) {
    		throw new UnsupportedOperationException();
    	}
//    	@Override
//    	public void appendAfterRollBackFinally(Handler afterRollBack) {
//    		throw new UnsupportedOperationException();
//    	}
    	@Override
    	public String toString() {
    		return "HookPoints.DEFAULT";
    	}
    };
}
