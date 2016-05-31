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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

public final class ToParentDocValuesBlockJoinQuery extends Query {

  // The doc values field is used the record the offset in docids how far back the first child is from the parent,
  // so that from the parent docid space we can move forward the child iterator in case of advance
  private final String offsetToFirstChildField;
  // the big upside is that we then always can easily validate that docs are in the right order.
  // Also we are then able to only add doc values field to the child documents if we index the parent first and then
  // child docs. Right now children are indexed before the parent.
  private final Query parentQuery;
  private final Query childQuery;
  private final ScoreMode scoreMode;

  public ToParentDocValuesBlockJoinQuery(String offsetToFirstChildField, Query parentQuery, Query childQuery, ScoreMode scoreMode) {
    this.offsetToFirstChildField = offsetToFirstChildField;
    this.parentQuery = parentQuery;
    this.childQuery = childQuery;
    this.scoreMode = scoreMode;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewrittenChildQuery = childQuery.rewrite(reader);
    if (rewrittenChildQuery != childQuery) {
      return new ToParentDocValuesBlockJoinQuery(offsetToFirstChildField, parentQuery, rewrittenChildQuery, scoreMode);
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    boolean requireScores = needsScores && scoreMode != ScoreMode.None;
    Weight parentWeight = parentQuery.createWeight(searcher, false, boost);
    Weight childWeight = childQuery.createWeight(searcher, requireScores, boost);
    return new FilterWeight(this, childWeight) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return Explanation.noMatch("not implemented yet");
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(false);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        NumericDocValues offsetToFirstChildDV = context.reader().getNumericDocValues(offsetToFirstChildField);
        if (offsetToFirstChildDV == null) {
          return null;
        }

        ScorerSupplier childScorerSupplier = childWeight.scorerSupplier(context);
        if (childScorerSupplier == null) {
          return null;
        }

        ScorerSupplier parentScorerSupplier = parentWeight.scorerSupplier(context);
        if (parentScorerSupplier == null) {
          return null;
        }

        final Weight weight = this;
        return new ScorerSupplier() {
          @Override
          public Scorer get(boolean randomAccess) throws IOException {
            DocIdSetIterator parentIterator = parentScorerSupplier.get(randomAccess).iterator();
            Scorer childScorer = childScorerSupplier.get(randomAccess);
            if (requireScores) {
              return new BlockJoinScorer(weight, scoreMode, offsetToFirstChildDV, parentIterator, childScorer);
            } else {
              DocIdSetIterator childIterator = childScorer.iterator();
              return new ConstantScoreScorer(weight, 1f, new DVBlockJoinIterator(parentIterator, childIterator, offsetToFirstChildDV));
            }
          }

          @Override
          public long cost() {
            return childScorerSupplier.cost();
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if ((o instanceof ToParentDocValuesBlockJoinQuery) == false) {
      return false;
    }
    ToParentDocValuesBlockJoinQuery other = (ToParentDocValuesBlockJoinQuery) o;
    return Objects.equals(childQuery, other.childQuery) &&
        Objects.equals(offsetToFirstChildField, other.offsetToFirstChildField) &&
        Objects.equals(scoreMode, other.scoreMode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), childQuery, offsetToFirstChildField, scoreMode);
  }

  @Override
  public String toString(String field) {
    return null;
  }

  final class BlockJoinScorer extends Scorer {

    final ScoreMode scoreMode;
    final NumericDocValues offsetToFirstChildDV;

    final Scorer childScorer;
    final DocIdSetIterator parentIterator;
    final DocIdSetIterator childIt;
    final TwoPhaseIterator childTwoPhaseIterator;
    final ToParentBlockJoinQuery.ParentTwoPhase parentTwoPhase;

    float score;
    int freq;

    BlockJoinScorer(Weight weight, ScoreMode scoreMode, NumericDocValues offsetToFirstChildDV, DocIdSetIterator parentIterator,
                    Scorer childScorer) throws IOException {
      super(weight);
      this.scoreMode = scoreMode;
      this.offsetToFirstChildDV = offsetToFirstChildDV;

      this.childScorer = childScorer;
      this.childTwoPhaseIterator = childScorer.twoPhaseIterator();
      if (this.childTwoPhaseIterator == null) {
        this.childIt = childScorer.iterator();
        this.parentIterator = new DVBlockJoinIterator(parentIterator, childIt, offsetToFirstChildDV);
        this.parentTwoPhase = null;
      } else {
        this.childIt = childTwoPhaseIterator.approximation();
        this.parentIterator = new DVBlockJoinIterator(parentIterator, childIt, offsetToFirstChildDV);
        this.parentTwoPhase = new ToParentBlockJoinQuery.ParentTwoPhase(this.parentIterator, childTwoPhaseIterator);
      }
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(childScorer, "DV_BLOCK_JOIN"));
    }

    @Override
    public int docID() {
      return parentIterator.docID();
    }

    @Override
    public float score() throws IOException {
      setScoreAndFreq();
      return score;
    }

    @Override
    public int freq() throws IOException {
      setScoreAndFreq();
      return freq;
    }

    @Override
    public DocIdSetIterator iterator() {
      if (parentTwoPhase == null) {
        return parentIterator;
      } else {
        return TwoPhaseIterator.asDocIdSetIterator(parentTwoPhase);
      }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return parentTwoPhase;
    }

    private void setScoreAndFreq() throws IOException {
      if (childIt.docID() >= parentIterator.docID()) {
        return;
      }
      int freq = 1;
      double score = childScorer.score();
      while (childIt.nextDoc() < parentIterator.docID()) {
        if (childTwoPhaseIterator == null || childTwoPhaseIterator.matches()) {
          final float childScore = childScorer.score();
          freq += 1;
          switch (scoreMode) {
            case Total:
            case Avg:
              score += childScore;
              break;
            case Min:
              score = Math.min(score, childScore);
              break;
            case Max:
              score = Math.max(score, childScore);
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      if (scoreMode == ScoreMode.Avg) {
        score /= freq;
      }
      this.score = (float) score;
      this.freq = freq;
    }

  }

  final class DVBlockJoinIterator extends DocIdSetIterator {

    final DocIdSetIterator parentIt;
    final DocIdSetIterator childIt;
    final NumericDocValues offsetToFirstChildDV;

    int parentDocId = -1;
    int childDocId;

    DVBlockJoinIterator(DocIdSetIterator parentIt, DocIdSetIterator childIt, NumericDocValues offsetToFirstChildDV) throws IOException {
      this.parentIt = parentIt;
      this.childIt = childIt;
      this.offsetToFirstChildDV = offsetToFirstChildDV;
      this.childDocId = childIt.nextDoc();
    }

    @Override
    public int docID() {
      return parentDocId;
    }

    @Override
    public int nextDoc() throws IOException {
      if (childDocId == NO_MORE_DOCS) {
        return parentDocId = NO_MORE_DOCS;
      }
      parentDocId = parentIt.advance(childDocId);
      childDocId = childIt.advance(parentDocId);
      return parentDocId;
    }

    @Override
    public int advance(int parentTarget) throws IOException {
      if (parentTarget == NO_MORE_DOCS) {
        return parentDocId = NO_MORE_DOCS;
      }

      boolean advanced = offsetToFirstChildDV.advanceExact(parentTarget);
      assert advanced : "All parent docs must have an offset field";
      int firstChildOffset = (int) offsetToFirstChildDV.longValue();
      int childTarget = parentTarget - firstChildOffset;
      if (childTarget > childDocId) {
        childDocId = childIt.advance(childTarget);
      }
      return nextDoc();
    }

    @Override
    public long cost() {
      return childIt.cost() + 1 /* ... + 1 for advanceExact(...) ? */;
    }
  }

}
