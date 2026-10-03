package com.ewoodbury.scarlet.columnar

/** Physical type of one column. Nested values are not in this set. */
enum LogicalType:
  case Int32, Int64, Float64, Bool, Utf8Dict
