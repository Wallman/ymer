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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Holds information about all documents that are mirrored by a given VersionedMongoDBExternalDataSource. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 */
final class MirroredDocuments {

	private final Map<Class<?>, MirroredDocument<?>> documentByMirroredType = new ConcurrentHashMap<>();
	
	/**
	 * Creates MirroredDocuments holding the given documents. <p>
	 * 
	 * @param mirroredDocuments
	 */
	MirroredDocuments(MirroredDocument<?>... mirroredDocuments) {
		Stream.of(mirroredDocuments).forEach(mirroredDocument -> {
			this.documentByMirroredType.put(mirroredDocument.getMirroredType(), mirroredDocument);
		});
	}
	
	MirroredDocuments(Stream<MirroredDocumentDefinition<?>> mirroredDocuments) {
		mirroredDocuments.map(MirroredDocumentDefinition::buildMirroredDocument).forEach(mirroredDocument -> {
			this.documentByMirroredType.put(mirroredDocument.getMirroredType(), mirroredDocument);
		});
	}
	
	/**
	 * Returns a set of all mirrored types <p>
	 * 
	 * @return
	 */
	public Set<Class<?>> getMirroredTypes() {
		return documentByMirroredType.keySet();
	}
	
	/**
	 * Returns a Collection of all MirroredDocument's
	 * @return
	 */
	public Collection<MirroredDocument<?>> getMirroredDocuments() {
		return documentByMirroredType.values();
	}
	
	/**
	 * Returns the fully qualified classname for all mirrored types. <p>
	 * 
	 * @return
	 */
	public Set<String> getMirroredTypeNames() {
		Set<String> result = new HashSet<>();
		for (MirroredDocument<?> doc : documentByMirroredType.values()) {
			result.add(doc.getMirroredType().getName());
		}
		return result;
	}

	/**
	 * Returns the MirroredDocument for a given type. <p>
	 * 
	 * @param type
	 * @return
	 * @throws NonMirroredTypeException if the given type is not mirrored
	 */
	@SuppressWarnings("unchecked")
	public <T> MirroredDocument<T> getMirroredDocument(Class<T> type) {
		MirroredDocument<T> result = (MirroredDocument<T>) documentByMirroredType.get(type);
		if (result == null) {
			throw new NonMirroredTypeException(type);
		}
		return result;
	}

	public boolean isMirroredType(Class<?> type) {
		return getMirroredTypes().contains(type);
	}
	
}