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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedLongValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Like {@link ImmutableOrdinalMap} but allows a limited number of modifications without rebuilding the entire ordinal map.
 */
public final class UpdateableOrdinalMap implements OrdinalMap {

  public static UpdateableOrdinalMap reopen(Object owner, UpdateableOrdinalMap previous, List<LeafReaderContext> leafs, SortedDocValues[] values, float acceptableOverheadRatio) throws IOException {
    long[] valueCounts = new long[values.length];
    TermsEnum[] tenums = new TermsEnum[values.length];
    for (int i = 0; i < values.length; i++) {
      valueCounts[i] = values[i].getValueCount();
      tenums[i] = values[i].termsEnum();
    }
    SortedDocValuesLookup lookup = new SortedDocValuesLookup(previous, values);
    return reopen(owner, previous, leafs, valueCounts, tenums, lookup, acceptableOverheadRatio);
  }

  public static UpdateableOrdinalMap reopen(Object owner, UpdateableOrdinalMap previous, List<LeafReaderContext> leafs, SortedSetDocValues[] values, float acceptableOverheadRatio) throws IOException {
    long[] valueCounts = new long[values.length];
    TermsEnum[] tenums = new TermsEnum[values.length];
    for (int i = 0; i < values.length; i++) {
      valueCounts[i] = values[i].getValueCount();
      tenums[i] = values[i].termsEnum();
    }
    SortedSetDocValuesLookup lookup = new SortedSetDocValuesLookup(previous, values);
    return reopen(owner, previous, leafs, valueCounts, tenums, lookup, acceptableOverheadRatio);
  }

  private static UpdateableOrdinalMap reopen(Object owner, UpdateableOrdinalMap previous, List<LeafReaderContext> leafs, long[] valueCounts, TermsEnum[] subs, TermToGlobalOrdinalLookup lookup, float acceptableOverheadRatio) throws IOException {
    if (previous == null) {
      return rebuild(owner, leafs, valueCounts, subs, acceptableOverheadRatio, null);
    }

    List<Integer> segmentsToReopen = new ArrayList<>();
    for (LeafReaderContext leaf : leafs) {
      if (previous.leafReaderCacheKeys.contains(leaf.reader().getCoreCacheKey())) {
        continue;
      }

      long valueCount = valueCounts[leaf.ord];
      double ratio = valueCount / (double) previous.ordinalMap.getValueCount();
      if (ratio < 0.1 || valueCount < 128 ) {
        segmentsToReopen.add(leaf.ord);
      } else {
        return rebuild(owner, leafs, valueCounts, subs, acceptableOverheadRatio, previous.stats);
      }
    }

    if (segmentsToReopen.isEmpty()) {
      return previous;
    } else if (segmentsToReopen.size() > 20) {
      return rebuild(owner, leafs, valueCounts, subs, acceptableOverheadRatio, previous.stats);
    }

    Set<Object> leafReaderCacheKeys = new HashSet<>();
    for (LeafReaderContext leaf : leafs) {
      leafReaderCacheKeys.add(leaf.reader().getCoreCacheKey());
    }

    for (Object previousReaderKeys : previous.leafReaderCacheKeys) {
      if (!leafReaderCacheKeys.contains(previousReaderKeys)) {
        return rebuild(owner, leafs, valueCounts, subs, acceptableOverheadRatio, previous.stats);
      }
    }

    Map<Integer, PackedLongValues.Builder> segmentOrdToGlobalOrdBuilders = new HashMap<>();
    for (Integer segment : segmentsToReopen) {
      segmentOrdToGlobalOrdBuilders.put(segment, PackedLongValues.monotonicBuilder(acceptableOverheadRatio));
    }

    TermsEnum[] newTenums = new TermsEnum[segmentsToReopen.size()];
    for (int i = 0; i < newTenums.length; i++) {
      newTenums[i] = subs[segmentsToReopen.get(i)];
    }

    MultiTermsEnum newMte = openMergedView(newTenums);
    for (BytesRef term = newMte.next(); term != null; term = newMte.next()) {
      long globalOrd;
      long existingGlobalOrd = lookup.lookupTerm(term);
      if (existingGlobalOrd >= 0) {
        globalOrd = existingGlobalOrd;
      } else {
        return rebuild(owner, leafs, valueCounts, subs, acceptableOverheadRatio, previous.stats);
      }

      MultiTermsEnum.TermsEnumWithSlice matches[] = newMte.getMatchArray();
      for (int i = 0; i < newMte.getMatchCount(); i++) {
        int segmentIndex = segmentsToReopen.get(matches[i].index);
        segmentOrdToGlobalOrdBuilders.get(segmentIndex).add(globalOrd);
      }
    }

    Map<Integer, LongValues> segmentOrdToGlobalOrdLookups = new HashMap<>();
    if (previous.segmentToGlobalOrds != null) {
      segmentOrdToGlobalOrdLookups.putAll(previous.segmentToGlobalOrds);
    }
    for (Map.Entry<Integer, PackedLongValues.Builder> entry : segmentOrdToGlobalOrdBuilders.entrySet()) {
      segmentOrdToGlobalOrdLookups.put(entry.getKey(), entry.getValue().build());
    }
    return new UpdateableOrdinalMap(leafReaderCacheKeys, segmentOrdToGlobalOrdLookups, previous);
  }

  private static UpdateableOrdinalMap rebuild(Object owner, List<LeafReaderContext> leafs, long[] valueCounts, TermsEnum[] tenums, float acceptableOverheadRatio, Stats previousStats) throws IOException {
    Set<Object> leafReaderCacheKeys = new HashSet<>();
    for (LeafReaderContext leaf : leafs) {
      leafReaderCacheKeys.add(leaf.reader().getCoreCacheKey());
    }
    OrdinalMap ordinalMap = ImmutableOrdinalMap.build(owner, tenums, valueCounts, acceptableOverheadRatio);
    return new UpdateableOrdinalMap(leafReaderCacheKeys, ordinalMap, previousStats);
  }

  private static MultiTermsEnum openMergedView(TermsEnum[] subs) throws IOException {
    ReaderSlice slices[] = new ReaderSlice[subs.length];
    MultiTermsEnum.TermsEnumIndex indexes[] = new MultiTermsEnum.TermsEnumIndex[slices.length];
    for (int i = 0; i < slices.length; i++) {
      slices[i] = new ReaderSlice(0, 0, i);
      indexes[i] = new MultiTermsEnum.TermsEnumIndex(subs[i], i);
    }
    MultiTermsEnum mte = new MultiTermsEnum(slices);
    mte.reset(indexes);
    return mte;
  }

  private final Set<Object> leafReaderCacheKeys;
  private final OrdinalMap ordinalMap;
  private final Map<Integer, LongValues> segmentToGlobalOrds;
  private final Stats stats;

  public UpdateableOrdinalMap(Set<Object> leafReaderCacheKeys, OrdinalMap ordinalMap, Stats previousStats) {
    this.leafReaderCacheKeys = leafReaderCacheKeys;
    this.ordinalMap = ordinalMap;
    this.segmentToGlobalOrds = null;
    if (previousStats != null) {
      this.stats = new Stats(previousStats, true);
    } else {
      this.stats = new Stats();
    }
  }

  public UpdateableOrdinalMap(Set<Object> leafReaderCacheKeys, Map<Integer, LongValues> segmentToGlobalOrds, UpdateableOrdinalMap previous) {
    this.leafReaderCacheKeys = leafReaderCacheKeys;
    this.segmentToGlobalOrds = segmentToGlobalOrds;
    this.ordinalMap = previous.ordinalMap;
    this.stats = new Stats(previous.stats, false);
  }

  @Override
  public LongValues getGlobalOrds(int segmentIndex) {
    LongValues longValues = segmentToGlobalOrds != null ? segmentToGlobalOrds.get(segmentIndex) : null;
    if (longValues == null) {
      longValues = ordinalMap.getGlobalOrds(segmentIndex);
    }
    return longValues;
  }

  @Override
  public long getFirstSegmentOrd(long globalOrd) {
    return ordinalMap.getFirstSegmentOrd(globalOrd);
  }

  @Override
  public int getFirstSegmentNumber(long globalOrd) {
    return ordinalMap.getFirstSegmentNumber(globalOrd);
  }

  @Override
  public long getValueCount() {
    return ordinalMap.getValueCount();
  }

  @Override
  public long ramBytesUsed() {
    return ordinalMap.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return ordinalMap.getChildResources();
  }

  public Stats getStats() {
    return stats;
  }

  public static class Stats {

    private final int version;
    private final long rebuilds;

    private Stats(Stats stats, boolean rebuild) {
      if (rebuild) {
        this.version = 0;
        this.rebuilds = stats.rebuilds + 1;
      } else {
        this.version = stats.version + 1;
        this.rebuilds = stats.rebuilds;
      }
    }

    public Stats() {
      this.version = 0;
      this.rebuilds = 0;
    }

    /**
     * @return The number of times the updateable ordinalmap has been modified without rebuilding it completely
     */
    public int getVersion() {
      return version;
    }

    /**
     * @return The number of times the updateable ordinalmap has been rebuild
     */
    public long getRebuilds() {
      return rebuilds;
    }
  }

  private static abstract class TermToGlobalOrdinalLookup {

    private final UpdateableOrdinalMap ordinalMap;

    TermToGlobalOrdinalLookup(UpdateableOrdinalMap ordinalMap) {
      this.ordinalMap = ordinalMap;
    }

    final long lookupTerm(BytesRef key) {
      long low = 0;
      long high = ordinalMap.getValueCount() - 1;

      while (low <= high) {
        long mid = (low + high) >>> 1;
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(mid);
        final int readerIndex = ordinalMap.getFirstSegmentNumber(mid);
        final BytesRef term = lookupValue(readerIndex, segmentOrd);
        int cmp = term.compareTo(key);

        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return mid; // key found
        }
      }

      return -(low + 1);  // key not found.
    }
    
    abstract BytesRef lookupValue(int readerIndex, long segmentOrdinal);

  }

  private final static class SortedSetDocValuesLookup extends TermToGlobalOrdinalLookup {

    private final SortedSetDocValues[] values;

    private SortedSetDocValuesLookup(UpdateableOrdinalMap ordinalMap, SortedSetDocValues[] values) {
      super(ordinalMap);
      this.values = values;
    }

    @Override
    BytesRef lookupValue(int readerIndex, long segmentOrdinal) {
      return values[readerIndex].lookupOrd(segmentOrdinal);
    }

  }

  private final static class SortedDocValuesLookup extends TermToGlobalOrdinalLookup {

    private final SortedDocValues[] values;

    private SortedDocValuesLookup(UpdateableOrdinalMap ordinalMap, SortedDocValues[] values) {
      super(ordinalMap);
      this.values = values;
    }

    @Override
    BytesRef lookupValue(int readerIndex, long segmentOrdinal) {
      return values[readerIndex].lookupOrd((int) segmentOrdinal);
    }
    
  }
}
