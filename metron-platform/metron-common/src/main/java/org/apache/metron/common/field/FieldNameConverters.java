package org.apache.metron.common.field;

public enum FieldNameConverters {

  /**
   * A {@link FieldNameConverter} that does not rename any fields.  All field
   * names remain unchanged.
   */
  NOOP(new NoopFieldNameConverter()),

  /**
   * A {@link FieldNameConverter} that replaces all field names containing dots
   * with colons.
   */
  DEDOT(new DeDotFieldNameConverter());

  private FieldNameConverter converter;

  FieldNameConverters(FieldNameConverter converter) {
    this.converter = converter;
  }

  public FieldNameConverter get() {
    return converter;
  }
}
