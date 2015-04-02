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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Locale;

/**
 */
public class TestUpdateableOrdinalMap extends LuceneTestCase {

  public void testNRTOrdinalMap_noMergeAndNoNewTerms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random()));
    cfg.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter iw = new IndexWriter(dir, cfg);

    int numValues = TestUtil.nextInt(random(), 1, 99);
    for (int i = 0; i < numValues; i++) {
      Document d = new Document();
      d.add(new SortedSetDocValuesField("field", new BytesRef(String.format(Locale.ROOT, "%10d", i))));
      iw.addDocument(d);
    }
    iw.commit();

    DirectoryReader r = iw.getReader();
    UpdateableOrdinalMap ordinalMap = sortedSetReOpen("key", null, "field", r, PackedInts.DEFAULT);
    assertEquals(numValues, ordinalMap.getValueCount());
    assertEquals(0, ordinalMap.getStats().getVersion());
    assertEquals(0, ordinalMap.getStats().getRebuilds());
    for (int i = 0; i < numValues; i++) {
      assertEquals(i, ordinalMap.getGlobalOrds(0).get(i));
      assertEquals(0, ordinalMap.getFirstSegmentNumber(i));
    }

    int numRounds = TestUtil.nextInt(random(), 1, 10);
    for (int round = 0; round < numRounds; round++) {
      for (int i = 0; i < numValues; i++) {
        Document d = new Document();
        d.add(new SortedSetDocValuesField("field", new BytesRef(String.format(Locale.ROOT, "%10d", i))));
        iw.addDocument(d);
      }

      r.close();
      r = iw.getReader();
      ordinalMap = sortedSetReOpen("key", ordinalMap, "field", r, PackedInts.DEFAULT);
      assertEquals(numValues, ordinalMap.getValueCount());
      assertEquals(round + 1, ordinalMap.getStats().getVersion());
      assertEquals(0, ordinalMap.getStats().getRebuilds());
      for (int segment = 0; segment < round + 2; segment++) {
        for (int i = 0; i < numValues; i++) {
          assertEquals(i, ordinalMap.getGlobalOrds(0).get(i));
          assertEquals(0, ordinalMap.getFirstSegmentNumber(i));
        }
      }
    }

    r.close();
    iw.close();
    dir.close();
  }

  public void testNRTOrdinalMap_newTerm() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random()));
    cfg.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter iw = new IndexWriter(dir, cfg);

    int numValues = TestUtil.nextInt(random(), 1, 98);
    for (int i = 0; i < numValues; i++) {
      Document d = new Document();
      d.add(new SortedSetDocValuesField("field", new BytesRef(String.format(Locale.ROOT, "%10d", i))));
      iw.addDocument(d);
    }

    DirectoryReader r = iw.getReader();
    UpdateableOrdinalMap ordinalMap = sortedSetReOpen("key", null, "field", r, PackedInts.DEFAULT);
    assertEquals(numValues, ordinalMap.getValueCount());
    assertEquals(0, ordinalMap.getStats().getVersion());
    assertEquals(0, ordinalMap.getStats().getRebuilds());
    for (int i = 0; i < numValues; i++) {
      assertEquals(i, ordinalMap.getGlobalOrds(0).get(i));
      assertEquals(0, ordinalMap.getFirstSegmentNumber(i));
    }

    Document d = new Document();
    d.add(new SortedSetDocValuesField("field", new BytesRef(String.format(Locale.ROOT, "%10d", numValues++))));
    iw.addDocument(d);

    r.close();
    r = iw.getReader();
    ordinalMap = sortedSetReOpen("key", ordinalMap, "field", r, PackedInts.DEFAULT);
    assertEquals(numValues, ordinalMap.getValueCount());
    assertEquals(0, ordinalMap.getStats().getVersion());
    assertEquals(1, ordinalMap.getStats().getRebuilds());

    for (int i = 0; i < numValues - 1; i++) {
      assertEquals(i, ordinalMap.getGlobalOrds(0).get(i));
      assertEquals(0, ordinalMap.getFirstSegmentNumber(i));
      assertEquals(i, ordinalMap.getFirstSegmentOrd(i));
    }

    assertEquals(numValues - 1, ordinalMap.getGlobalOrds(1).get(0));
    assertEquals(1, ordinalMap.getFirstSegmentNumber(numValues - 1));
    assertEquals(0, ordinalMap.getFirstSegmentOrd(numValues - 1));

    r.close();
    iw.close();
    dir.close();
  }

  public void testNRTOrdinalMap_merge() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random()));
    cfg.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter iw = new IndexWriter(dir, cfg);

    int numValues = TestUtil.nextInt(random(), 1, 99);
    for (int i = 0; i < numValues; i++) {
      Document d = new Document();
      d.add(new SortedSetDocValuesField("field", new BytesRef(String.format(Locale.ROOT, "%10d", i))));
      iw.addDocument(d);
    }

    DirectoryReader r = iw.getReader();
    UpdateableOrdinalMap ordinalMap = sortedSetReOpen("key", null, "field", r, PackedInts.DEFAULT);
    assertEquals(numValues, ordinalMap.getValueCount());
    assertEquals(0, ordinalMap.getStats().getRebuilds());
    assertEquals(0, ordinalMap.getStats().getVersion());
    for (int i = 0; i < numValues; i++) {
      assertEquals(i, ordinalMap.getGlobalOrds(0).get(i));
      assertEquals(0, ordinalMap.getFirstSegmentNumber(i));
    }

    int numRounds = TestUtil.nextInt(random(), 1, 10);
    for (int round = 0; round < numRounds; round++) {
      for (int i = 0; i < numValues; i++) {
        Document d = new Document();
        d.add(new SortedSetDocValuesField("field", new BytesRef(String.format(Locale.ROOT, "%10d", i))));
        iw.addDocument(d);
      }

      r.close();
      r = iw.getReader();
      ordinalMap = sortedSetReOpen("key", ordinalMap, "field", r, PackedInts.DEFAULT);
      assertEquals(numValues, ordinalMap.getValueCount());
      assertEquals(0, ordinalMap.getStats().getRebuilds());
      assertEquals(round + 1, ordinalMap.getStats().getVersion());
      for (int segment = 0; segment < round + 2; segment++) {
        for (int i = 0; i < numValues; i++) {
          assertEquals(i, ordinalMap.getGlobalOrds(0).get(i));
          assertEquals(0, ordinalMap.getFirstSegmentNumber(i));
        }
      }
    }

    r.close();
    iw.close();
    iw = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
    iw.forceMerge(1);
    r = DirectoryReader.open(dir);
    ordinalMap = sortedSetReOpen("key", ordinalMap, "field", r, PackedInts.DEFAULT);
    assertEquals(numValues, ordinalMap.getValueCount());
    assertEquals(0, ordinalMap.getStats().getVersion());
    assertEquals(1, ordinalMap.getStats().getRebuilds());

    r.close();
    iw.close();
    dir.close();
  }


  public static UpdateableOrdinalMap sortedSetReOpen(Object owner, UpdateableOrdinalMap previous, String field, DirectoryReader reader, float acceptableOverheadRatio) throws IOException {
    SortedSetDocValues[] values = new SortedSetDocValues[reader.leaves().size()];
    for (LeafReaderContext context : reader.leaves()) {
      values[context.ord] = context.reader().getSortedSetDocValues(field);
    }
    return UpdateableOrdinalMap.reopen(owner, previous, reader.leaves(), values, acceptableOverheadRatio);
  }
}
