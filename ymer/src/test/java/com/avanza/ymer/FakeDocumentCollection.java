/*
 * Copyright 2015 Avanza Bank AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.avanza.ymer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
class FakeDocumentCollection implements DocumentCollection {

	private final ConcurrentLinkedQueue<DBObject> collection = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<Document> collection2 = new ConcurrentLinkedQueue<>();
	private final AtomicInteger idGenerator = new AtomicInteger(0);

	@Override
	public Stream<DBObject> findAll(SpaceObjectFilter<?> filter) {
		return new ArrayList<>(collection).stream();
	}

	@Override
	public Stream<Document> findAll2(SpaceObjectFilter<?> objectFilter)  {
		return new ArrayList<>(collection2).stream();
	}

	@Override
	public Stream<DBObject> findAll() {
		return new ArrayList<>(collection).stream();
	}

	@Override
	public Stream<Document> findAll2()  {
		return new ArrayList<>(collection2).stream();
	}

	@Override
	public void replace(DBObject oldVersion, DBObject newVersion) {
		// Note that the Iterator of the list associated with the given collectionName may reflect changes to the
		// underlying list. This behavior is similar to a database cursor who may returned elements
		// that are inserted/updated after the cursor is created.
		collection.remove(oldVersion);
		collection.add(newVersion);
	}

	@Override
	public void replace(Document oldVersion, Document newVersion) {
		// Note that the Iterator of the list associated with the given collectionName may reflect changes to the
		// underlying list. This behavior is similar to a database cursor who may returned elements
		// that are inserted/updated after the cursor is created.
		collection2.remove(oldVersion);
		collection2.add(newVersion);
	}

	public void addDocument(String collectionName, BasicDBObject doc) {
		collection.add(doc);
	}

	@Override
	public void update(DBObject newVersion) {
		Iterator<DBObject> it = collection.iterator();
		while (it.hasNext()) {
			DBObject dbObject = it.next();
			if (dbObject.get("_id").equals(newVersion.get("_id"))) {
				it.remove();
				collection.add(newVersion);
				return;
			}
		}
		// No object found, do insert
		insert(newVersion);
	}

	@Override
	public void update(Document newVersion) {
		Iterator<Document> it = collection2.iterator();
		while (it.hasNext()) {
			Document document = it.next();
			if (document.get("_id").equals(newVersion.get("_id"))) {
				it.remove();
				collection2.add(newVersion);
				return;
			}
		}
		// No object found, do insert
		insert(newVersion);
	}

	@Override
	public void insert(DBObject dbObject) {
		for (DBObject object : collection) {
			if (object.get("_id").equals(dbObject.get("_id"))) {
				throw new DuplicateDocumentKeyException("_id: " + dbObject.get("_id"));
			}
		}
		if (dbObject.get("_id") == null) {
			dbObject.put("_id", "testid_" + idGenerator.incrementAndGet());
		}
		collection.add(dbObject);
	}

	@Override
	public void insert(Document document) {
		for (Document doc : collection2) {
			if (doc.get("_id").equals(document.get("_id"))) {
				throw new DuplicateDocumentKeyException("_id: " + document.get("_id"));
			}
		}
		if (document.get("_id") == null) {
			document.put("_id", "testid_" + idGenerator.incrementAndGet());
		}
		collection2.add(document);
	}

	@Override
	public void delete(BasicDBObject dbObject) {
		BasicDBObject idOBject = new BasicDBObject();
		idOBject.put("_id", dbObject.get("_id"));
		if (idOBject.equals(dbObject)) {
			removeById(idOBject);
		} else {
			removeByTemplate(dbObject);
		}
	}

	@Override
	public void delete(Document document) {
		Document doc = new Document();
		doc.put("_id", document.get("_id"));
		if (doc.equals(document)) {
			removeById(doc);
		} else {
			removeByTemplate(document);
		}
	}

	private void removeByTemplate(BasicDBObject dbObject) {
		Iterator<DBObject> it = collection.iterator();
		while (it.hasNext()) {
			DBObject next = it.next();
			if (next.equals(dbObject)) {
				it.remove();
				return;
			}
		}
	}

	private void removeById(BasicDBObject dbObject) {
		Iterator<DBObject> it = collection.iterator();
		while (it.hasNext()) {
			DBObject next = it.next();
			if (next.get("_id").equals(dbObject.get("_id"))) {
				it.remove();
				return;
			}
		}
	}

	private void removeByTemplate(Document document) {
		Iterator<Document> it = collection2.iterator();
		while (it.hasNext()) {
			Document next = it.next();
			if (next.equals(document)) {
				it.remove();
				return;
			}
		}
	}

	private void removeById(Document document) {
		Iterator<Document> it = collection2.iterator();
		while (it.hasNext()) {
			Document next = it.next();
			if (next.get("_id").equals(document.get("_id"))) {
				it.remove();
				return;
			}
		}
	}

	@Override
	public void insertAll(DBObject... dbObjects) {
		for (DBObject dbObject : dbObjects) {
			insert(dbObject);
		}
	}

	@Override
	public void insertAll(Document... documents) {
		for (Document document : documents) {
			insert(document);
		}
	}

	@Override
	public DBObject findById(Object id) {
		for (DBObject next : collection) {
			if (next.get("_id").equals(id)) {
				return next;
			}
		}
		return null;
	}

	@Override
	public Document findById2(Object id) {
		for (Document next : collection2) {
			if (next.get("_id").equals(id)) {
				return next;
			}
		}
		return null;
	}

	@Override
	public Stream<DBObject> findByQuery(Query query) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<Document> findByQuery2(Query query) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<DBObject> findByTemplate(BasicDBObject object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<Document> findByTemplate(Document template) {
		throw new UnsupportedOperationException();
	}
}
