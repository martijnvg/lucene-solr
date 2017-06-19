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

import java.util.Arrays;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;

public class TestDocValuesBlockJoin extends LuceneTestCase {

  public void testSimple() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    ParentChildBlock block = new ParentChildBlock(
        "_type", "offset_to_first_child", "resume"
    );
    block.nextLevel(makeResume("Lisa", "United Kingdom"), "job");
    block.addLeafDocuments(makeJob("java", 2007));
    block.addLeafDocuments(makeJob("python", 2010));
    block.previousLevel();
    w.addDocuments(block.flatten());

    block = new ParentChildBlock(
        "_type", "offset_to_first_child", "resume"
    );
    block.nextLevel(makeResume("Frank", "United States"), "job");
    block.addLeafDocuments(makeJob("ruby", 2005));
    block.addLeafDocuments(makeJob("java", 2006));
    block.previousLevel();
    w.addDocuments(block.flatten());

    block = new ParentChildBlock(
        "_type", "offset_to_first_child", "resume"
    );
    block.nextLevel(makeResume("Frits", "Germany"), "job");
    block.addLeafDocuments(makeJob("ruby", 2013));
    block.addLeafDocuments(makeJob("go", 2014));
    block.addLeafDocuments(makeJob("rust", 2015));
    block.previousLevel();
    w.addDocuments(block.flatten());

    block = new ParentChildBlock(
        "_type", "offset_to_first_child", "resume"
    );
    block.nextLevel(makeResume("Jim", "Australia"), "job");
    block.addLeafDocuments(makeJob("c", 1995));
    block.addLeafDocuments(makeJob("c++", 1999));
    block.nextLevel(makeJob("java", 2000), "endorsement");
    block.addLeafDocuments(makeEndorsement("Rob", "ceo"));
    block.previousLevel();
    block.previousLevel();
    w.addDocuments(block.flatten());

    block = new ParentChildBlock(
        "_type", "offset_to_first_child", "resume"
    );
    block.nextLevel(makeResume("Theodor", "Canada"), "job");
    block.nextLevel(makeJob("cobol", 1979), "endorsement");
    block.addLeafDocuments(makeEndorsement("Tim", "coworker"));
    block.previousLevel();
    block.addLeafDocuments(makeJob("c++", 1992));
    block.nextLevel(makeJob("java", 1995), "endorsement");
    block.addLeafDocuments(makeEndorsement("Mike", "cto"));
    block.previousLevel();
    block.previousLevel();
    w.addDocuments(block.flatten());

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    ToParentDocValuesBlockJoinQuery joinQuery = new ToParentDocValuesBlockJoinQuery("offset_to_first_child",
        new TermQuery(new Term("_type", "resume")), new MatchAllDocsQuery(), ScoreMode.None);
//    assertEquals(5, s.count(joinQuery));

    Query childJoinQuery = new ToParentDocValuesBlockJoinQuery("offset_to_first_child", new TermQuery(new Term("_type", "job")),
        new MatchAllDocsQuery(), ScoreMode.None);
    joinQuery = new ToParentDocValuesBlockJoinQuery("offset_to_first_child", new TermQuery(new Term("_type", "resume")),
        childJoinQuery, ScoreMode.None);
//    assertEquals(5, s.count(joinQuery));

    ScoreMode scoreMode = RandomPicks.randomFrom(random(),
        Arrays.asList(ScoreMode.Min, ScoreMode.Max, ScoreMode.Total, ScoreMode.Max));
    joinQuery = new ToParentDocValuesBlockJoinQuery("offset_to_first_child", new TermQuery(new Term("_type", "resume")),
        new TermQuery(new Term("skill", "java")), scoreMode);

    TopDocs result = s.search(joinQuery, 10);
    assertEquals(4, result.totalHits);
    assertEquals(2, result.scoreDocs[0].doc);
    assertEquals(5, result.scoreDocs[1].doc);
    assertEquals(14, result.scoreDocs[2].doc);
    assertEquals(20, result.scoreDocs[3].doc);

    BooleanQuery.Builder fullQuery = new BooleanQuery.Builder();
    Query parentQuery = new TermQuery(new Term("country", "Canada"));
    fullQuery.add(new BooleanClause(parentQuery, MUST));
    fullQuery.add(new BooleanClause(joinQuery, MUST));
    result = s.search(fullQuery.build(), 10);
    assertEquals(1, result.totalHits);
    assertEquals(20, result.scoreDocs[0].doc);

    childJoinQuery = new ToParentDocValuesBlockJoinQuery("offset_to_first_child", new TermQuery(new Term("_type", "job")),
        new TermQuery(new Term("role", "ceo")), scoreMode);
    joinQuery = new ToParentDocValuesBlockJoinQuery("offset_to_first_child", new TermQuery(new Term("_type", "resume")),
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("skill", "java")), MUST)
            .add(childJoinQuery, MUST)
            .build(), scoreMode);

    result = s.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertEquals(14, result.scoreDocs[0].doc);

    r.close();
    dir.close();
  }

  public void testDuel() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numCVs = 256 + random().nextInt(256);
    int numSkills = numCVs / 4;
    String[] skills = new String[numSkills];
    for (int i = 0; i < numSkills; i++) {
      skills[i] = randomText(6);
    }

    for (int i = 0; i < numCVs; i++) {
      ParentChildBlock block = new ParentChildBlock(
          "_type", "offset_to_first_child", "resume"
      );
      block.nextLevel(makeResume(randomText(4), randomText(8)), "job");
      int numJobs = 2 + random().nextInt(16);
      for (int j = 0; j < numJobs; j++) {
        String job = RandomPicks.randomFrom(random(), skills);
        block.addLeafDocuments(makeJob(job, 1980 + random().nextInt(37)));
      }
      block.previousLevel();
      w.addDocuments(block.flatten());
    }

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    for (String skill : skills) {
      ScoreMode scoreMode = RandomPicks.randomFrom(random(), ScoreMode.values());
      Query parentQuery = new TermQuery(new Term("_type", "resume"));
      Query childQuery = new TermQuery(new Term("skill", skill));
      ToParentBlockJoinQuery joinQuery =
          new ToParentBlockJoinQuery(childQuery, new QueryBitSetProducer(parentQuery), scoreMode);
      TopDocs result1 = s.search(joinQuery, numCVs);
      ToParentDocValuesBlockJoinQuery dvJoinQuery =
          new ToParentDocValuesBlockJoinQuery("offset_to_first_child", parentQuery, childQuery, scoreMode);
      TopDocs result2 = s.search(dvJoinQuery, numCVs);
      assertEquals(result1.totalHits, result2.totalHits);
      assertEquals(result1.scoreDocs.length, result2.scoreDocs.length);
      assertEquals(result1.getMaxScore(), result2.getMaxScore(), 0f);
      for (int i = 0; i < result1.scoreDocs.length; i++) {
        ScoreDoc scoreDoc1 = result1.scoreDocs[i];
        ScoreDoc scoreDoc2 = result2.scoreDocs[i];
        assertEquals(scoreDoc1.doc, scoreDoc2.doc);
        assertEquals(scoreDoc1.score, scoreDoc2.score, 0f);
      }
    }

    r.close();
    dir.close();
  }

  private static Document makeResume(String name, String country) {
    Document resume = new Document();
    resume.add(newStringField("name", name, Field.Store.NO));
    resume.add(newStringField("country", country, Field.Store.NO));
    return resume;
  }

  private static Document makeJob(String skill, int year) {
    Document job = new Document();
    job.add(newStringField("skill", skill, Field.Store.NO));
    job.add(new IntPoint("year", year));
    job.add(new StoredField("year", year));
    return job;
  }

  private static Document makeEndorsement(String name, String role) {
    Document endorsement = new Document();
    endorsement.add(newStringField("name", name, Field.Store.NO));
    endorsement.add(newStringField("role", role, Field.Store.NO));
    return endorsement;
  }

  private static String randomText(int length) {
    char[] table = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append(RandomPicks.randomFrom(random(), table));
    }
    return builder.toString();
  }

}
