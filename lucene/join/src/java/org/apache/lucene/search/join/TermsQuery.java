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

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

import java.io.IOException;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 *
 * @lucene.experimental
 */
class TermsQuery extends MultiTermQuery {

  private final BytesRefIterable terms;
  private final Query fromQuery; // Used for equals() only

  /**
   * @param field The field that should contain terms that are specified in the previous parameter
   * @param terms The terms that matching documents should have. The terms must be sorted by natural order.
   */
  TermsQuery(String field, Query fromQuery, BytesRefIterable terms) {
    super(field);
    this.fromQuery = fromQuery;
    this.terms = terms;
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return new SeekingTermSetTermsEnum(terms.iterator(null), this.terms);
  }

  @Override
  public String toString(String string) {
    return "TermsQuery{" +
        "field=" + field +
        '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } if (!super.equals(obj)) {
      return false;
    } if (getClass() != obj.getClass()) {
      return false;
    }

    TermsQuery other = (TermsQuery) obj;
    if (!fromQuery.equals(other.fromQuery)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result += prime * fromQuery.hashCode();
    return result;
  }

  // this intersects one termsenum with another...
  static class SeekingTermSetTermsEnum extends FilteredTermsEnum {
    private BytesRef current; // from the iterator
    private final BytesRefIterator iterator;


    SeekingTermSetTermsEnum(TermsEnum tenum, BytesRefIterable terms) {
      super(tenum);
      this.iterator = terms.iterator();
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
      if (currentTerm == null) {
        // first term
        return current = iterator.next();
      } else {
        do {
          if (current.compareTo(currentTerm) > 0) {
            return current;
          }
        } while ((current = iterator.next()) != null);
        return null;
      }
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      if (current == null) {
        return AcceptStatus.END;
      }
      
      int cmp = term.compareTo(current);
      if (cmp == 0) {
        return AcceptStatus.YES_AND_SEEK;
      } else if (cmp < 0) {
        return AcceptStatus.NO_AND_SEEK;
      } else {
        // we need to catch up
        while ((current = iterator.next()) != null) {
          cmp = term.compareTo(current);
          if (cmp == 0) {
            return AcceptStatus.YES;
          } else if (cmp < 0) {
            return AcceptStatus.NO_AND_SEEK; // will be returned by nextSeekTerm()
          }
        }
        return AcceptStatus.END;
      }
    }
  }
}
