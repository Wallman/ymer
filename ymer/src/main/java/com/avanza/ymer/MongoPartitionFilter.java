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

import java.util.Arrays;
import java.util.Objects;

import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.query.Criteria;

import com.avanza.ymer.SpaceObjectFilter.PartitionFilter;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

class MongoPartitionFilter {

	private final Bson filter;

	public MongoPartitionFilter(Bson filter) {
		this.filter = Objects.requireNonNull(filter);
	}

	public static MongoPartitionFilter create(SpaceObjectFilter<?> spaceObjectFilter) {
		return new MongoPartitionFilter(buildFilter(spaceObjectFilter.getPartitionFilter()));
	}

	public static MongoPartitionFilter createBsonFilter(SpaceObjectFilter<?> spaceObjectFilter) {
		return new MongoPartitionFilter(buildBsonFilter(spaceObjectFilter.getPartitionFilter()));
	}

	public static boolean canCreateFrom(SpaceObjectFilter<?> spaceObjectFilter) {
		return spaceObjectFilter.hasPartitionFilter();
	}

	private static BasicDBObject buildFilter(PartitionFilter<?> partitionFilter) {
//		int maxShards = 1024;
//		int[] shards = IntStream.range(0, maxShards).filter(i -> i%partitionFilter.getCurrentPartition() == 0).toArray();
//		return new BasicDBObject("_shard", new BasicDBObject("$in", shards));
		return new BasicDBObject("$or", Arrays.asList(
				new BasicDBObject(MirroredObject.DOCUMENT_ROUTING_KEY, new BasicDBObject("$mod", Arrays.asList(partitionFilter.getTotalPartitions(), partitionFilter.getCurrentPartition() - 1))),
				new BasicDBObject(MirroredObject.DOCUMENT_ROUTING_KEY, new BasicDBObject("$mod", Arrays.asList(partitionFilter.getTotalPartitions(), -(partitionFilter.getCurrentPartition() - 1)))),
				new BasicDBObject(MirroredObject.DOCUMENT_ROUTING_KEY, new BasicDBObject("$exists", false))
				));
	}

	public Bson toBson() {
		return filter;
	}

	public static Bson buildBsonFilter(PartitionFilter<?> partitionFilter) {
		return Filters.or(Filters.mod(MirroredObject.DOCUMENT_ROUTING_KEY,
									  partitionFilter.getTotalPartitions(),
									  partitionFilter.getCurrentPartition() - 1),
						  Filters.mod(MirroredObject.DOCUMENT_ROUTING_KEY,
									  partitionFilter.getTotalPartitions(),
									  -(partitionFilter.getCurrentPartition() - 1)),
						  Filters.exists(MirroredObject.DOCUMENT_ROUTING_KEY, false));
	}

	public static Criteria buildCriteriaFilter(PartitionFilter<?> partitionFilter) {
		return new Criteria().orOperator(Criteria.where(MirroredObject.DOCUMENT_ROUTING_KEY).mod(partitionFilter.getTotalPartitions(), partitionFilter.getCurrentPartition() - 1),
				Criteria.where(MirroredObject.DOCUMENT_ROUTING_KEY).mod(partitionFilter.getTotalPartitions(), -(partitionFilter.getCurrentPartition() - 1)),
				Criteria.where(MirroredObject.DOCUMENT_ROUTING_KEY).exists(false));
		
	}

	@Override
	public String toString() {
		return filter.toString();
	}

}
