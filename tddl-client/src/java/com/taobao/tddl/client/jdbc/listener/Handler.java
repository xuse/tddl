package com.taobao.tddl.client.jdbc.listener;

import java.sql.SQLException;

/*
 * @author guangxia
 * @since 1.0, 2010-4-12 ÏÂÎç02:19:53
 */
public abstract class Handler {
	
	protected abstract boolean run (Context context) throws SQLException;
	protected Handler onSuccess;
	protected Handler onFail;
	protected Handler onFinally;
	
	public void execute(Context context) throws SQLException {
		try {
			if(run(context)) {
				if(onSuccess != null) {
					onSuccess.execute(context);
				}
			} else {
				if(onFail != null) {
					onFail.execute(context);
				}
			}
		} finally {
			if(onFinally != null) {
				onFinally.execute(context);
			}
		}
	}
	
	public void setOnSuccess(Handler onSuccess) {
		this.onSuccess = onSuccess;
	}

	public Handler getOnSuccess() {
		return onSuccess;
	}

	public void setOnFail(Handler onFail) {
		this.onFail = onFail;
	}

	public Handler getOnFail() {
		return onFail;
	}

	public void setOnFinally(Handler onFinally) {
		this.onFinally = onFinally;
	}

	public void appendFinally(Handler onFinally) {
		Handler insertPoint = this;
		for(; insertPoint.getOnFinally() != null; insertPoint = insertPoint.getOnFinally());
		insertPoint.setOnFinally(onFinally);
	}
	
	public Handler getOnFinally() {
		return onFinally;
	}
	
	public static final Handler DUMMY_HANDLER = new Handler() {
		@Override
		public boolean run (Context context) {
			return true;
		}
		@Override
		public void setOnSuccess(Handler onSuccess) {
			throw new UnsupportedOperationException();
		}
		@Override
		public void setOnFail(Handler onFail) {
			throw new UnsupportedOperationException();
		}
		@Override
		public void setOnFinally(Handler onFinally) {
			throw new UnsupportedOperationException();
		}
		@Override
		public void appendFinally(Handler onFinally) {
			throw new UnsupportedOperationException();
		}
		@Override
		public String toString() {
			return "Handler.DUMMY_HANDLER";
		}
	};
}
