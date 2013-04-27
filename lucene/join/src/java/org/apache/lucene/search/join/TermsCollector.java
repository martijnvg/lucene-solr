package org.apache.lucene.search.join;

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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.MultiTermsEnum;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.SortedSetDocValuesTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A collector that collects all terms from a specified field matching the query.
 *
 * @lucene.experimental
 */
abstract class TermsCollector extends Collector {

  final String field;
  final BytesRefHash collectorTerms = new BytesRefHash();

  TermsCollector(String field) {
    this.field = field;
  }

  public BytesRefIterable getCollectorTerms() {
    return new BytesRefHashIterable(collectorTerms);
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  /**
   * Chooses the right {@link TermsCollector} implementation.
   *
   * @param field                     The field to collect terms for
   * @param multipleValuesPerDocument Whether the field to collect terms for has multiple values per document.
   * @return a {@link TermsCollector} instance
   */
  static TermsCollector create(String field, boolean multipleValuesPerDocument) {
    return multipleValuesPerDocument ? new MV(field) : new SV(field);
  }

  // impl that works with multiple values per document
  static class MV extends TermsCollector {
    final BytesRef scratch = new BytesRef();
    private SortedSetDocValues docTermOrds;

    MV(String field) {
      super(field);
    }

    @Override
    public void collect(int doc) throws IOException {
      docTermOrds.setDocument(doc);
      long ord;
      while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        currentBits.set((int) ord);
      }
    }

    // per-segment resources: if this works well e.g. we'd move to TermsCollector base class maybe
    private final List<OpenBitSet> bits = new ArrayList<OpenBitSet>();
    private final List<SortedSetDocValues> values = new ArrayList<SortedSetDocValues>();
    private OpenBitSet currentBits; // current bits for speed

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), field);
      currentBits = new OpenBitSet(docTermOrds.getValueCount());
      values.add(docTermOrds);
      bits.add(currentBits);
    }

    @Override
    public BytesRefIterable getCollectorTerms() {
      return new BytesRefIterable() {

        @Override
        public BytesRefIterator iterator() {
          int numSubs = values.size();
          ReaderSlice slices[] = new ReaderSlice[numSubs];
          MultiTermsEnum.TermsEnumIndex indexes[] = new MultiTermsEnum.TermsEnumIndex[numSubs];
          for (int i = 0; i < slices.length; i++) {
            slices[i] = new ReaderSlice(0, 0, i);
            TermsEnum te = new DocValuesConsumer.BitsFilteredTermsEnum(new SortedSetDocValuesTermsEnum(values.get(i)), bits.get(i));
            indexes[i] = new MultiTermsEnum.TermsEnumIndex(te, i);
          }
          MultiTermsEnum mte = new MultiTermsEnum(slices);
          try {
            mte.reset(indexes);
          } catch (IOException nocommit) {
            throw new RuntimeException(nocommit);
          }
          return mte;
        }
      };
    }
  }

  // impl that works with single value per document
  static class SV extends TermsCollector {

    final BytesRef spare = new BytesRef();
    private BinaryDocValues fromDocTerms;

    SV(String field) {
      super(field);
    }

    @Override
    public void collect(int doc) throws IOException {
      fromDocTerms.get(doc, spare);
      collectorTerms.add(spare);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      fromDocTerms = FieldCache.DEFAULT.getTerms(context.reader(), field);
    }
  }

}
