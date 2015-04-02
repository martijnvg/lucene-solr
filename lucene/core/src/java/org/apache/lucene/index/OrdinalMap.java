package org.apache.lucene.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;

/**
 */
public interface OrdinalMap extends Accountable {

  /**
   * Given a segment number, return a {@link org.apache.lucene.util.LongValues} instance that maps
   * segment ordinals to global ordinals.
   */
  LongValues getGlobalOrds(int segmentIndex);

  /**
   * Given global ordinal, returns the ordinal of the first segment which contains
   * this ordinal (the corresponding to the segment return {@link #getFirstSegmentNumber}).
   */
  long getFirstSegmentOrd(long globalOrd);

  /**
   * Given a global ordinal, returns the index of the first
   * segment that contains this term.
   */
  int getFirstSegmentNumber(long globalOrd);

  /**
   * Returns the total number of unique terms in global ord space.
   */
  long getValueCount();

}
