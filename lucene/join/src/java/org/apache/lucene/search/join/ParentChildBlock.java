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

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;

/**
 * Helper class to index parent and child documents into the right order as a single block
 */
public class ParentChildBlock {

  private final String typeField;
  private final String offsetToFirstChildField;

  private final Level root = new Level(0);
  private final Deque<Level> levels = new LinkedList<>();

  public ParentChildBlock(String typeField, String offsetToFirstChildField, String parentType)  {
    this.typeField = typeField;
    this.offsetToFirstChildField = offsetToFirstChildField;
    root.type(parentType);
    levels.add(root);
  }

  public List<Document> flatten() {
    List<Document> documents = new ArrayList<>();
    root.flatten(documents);
    return documents;
  }

  public void nextLevel(Iterable<? extends IndexableField> document, String type) {
    Level level = levels.peekLast().nextLevel(document);
    level.type(type);
    levels.add(level);
  }

  public void nextType(String type) {
    levels.peekLast().type(type);
  }

  public void previousLevel() {
    levels.removeLast();
  }

  public void addLeafDocuments(Iterable<? extends IndexableField> document) {
    levels.peekLast().addChildDocument(document);
  }

  private class Level {

    final int depth;
    List<DocBlock> childDocs = new ArrayList<>();

    String type;

    private Level(int depth) {
      this.depth = depth;
    }

    public void type(String type) {
      this.type = type;
    }

    public void addChildDocument(Iterable<? extends IndexableField> document) {
      Document copy = new Document();
      copy.add(new StringField(typeField, type, Field.Store.NO));
      document.forEach(copy::add);
      childDocs.add(new DocBlock(null, copy));
    }

    public Level nextLevel(Iterable<? extends IndexableField> document) {
      Level nextLevel = new Level(depth + 1);
      Document copy = new Document();
      copy.add(new StringField(typeField, type, Field.Store.NO));
      document.forEach(copy::add);
      childDocs.add(new DocBlock(nextLevel, copy));
      return nextLevel;
    }

    void flatten(List<Document> docs) {
      for (DocBlock docBlock : childDocs) {
        if (docBlock.level != null) {
          int numChildDocs = docBlock.level.numDocs();
          docBlock.level.flatten(docs);
          docBlock.document.add(new NumericDocValuesField(offsetToFirstChildField, numChildDocs));
        }
        docs.add(docBlock.document);
      }
    }

    int numDocs() {
      int numDocs = 0;
      for (DocBlock childDoc : childDocs) {
        numDocs += childDoc.numDocs();
      }
      return numDocs;
    }

    private class DocBlock {

      final Level level;
      final Document document;

      private DocBlock(Level level, Document document) {
        this.level = level;
        this.document = document;
      }

      int numDocs() {
        int numDocs = 1;
        if (level != null) {
          numDocs += level.numDocs();
        }
        return numDocs;
      }

    }

  }

}
