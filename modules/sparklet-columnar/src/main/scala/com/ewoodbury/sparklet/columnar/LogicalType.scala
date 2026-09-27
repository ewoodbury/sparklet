package com.ewoodbury.sparklet.columnar

/** Physical type of one column. Strings and nested values are not in this set yet. */
enum LogicalType:
  case Int32, Int64, Float64, Bool
