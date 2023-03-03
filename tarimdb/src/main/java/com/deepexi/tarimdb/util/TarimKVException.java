package com.deepexi.tarimdb.util;

/**
 * TarimKVException
 *
 */

public class TarimKVException extends Exception {

  /* @Nullable */ private final Status status;

  public TarimKVException(final Status status) {
    super(status.getMsg());
    this.status = status;
  }

  public TarimKVException(final Status status, final String msg) {
    super(msg);
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public int getErrorCode() {
    return status.getCode();
  }

  @Override
  public String getMessage() {
    return status.getMsg();
  }
}
