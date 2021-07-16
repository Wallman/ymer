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
package com.avanza.ymer.util;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.springframework.util.StringUtils;

/**
 * @see com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils
 * Note that Ymer uses the term "partition id" for what Gigaspaces calls "instance id", ie. starting at 1.
 * The Gigaspaces term "instance id" is one less than the "instance id", so starting at 0.
 */
public final class GigaSpacesPartitionIdUtil {
	private static final Pattern PARTITION_ID_PATTERN = Pattern.compile("_container(\\d+)");

	private GigaSpacesPartitionIdUtil() {
	}

	public static int getPartitionId(Object routingKey, int partitionCount) {
		return safeAbsoluteValue(routingKey.hashCode()) % partitionCount + 1;
	}

	public static Optional<Integer> extractPartitionIdFromSpaceName(@Nullable String spaceName) {
		// Format: qaSpace_container2_1:qaSpace
		if (StringUtils.isEmpty(spaceName)) {
			return Optional.empty();
		}

		Matcher matcher = PARTITION_ID_PATTERN.matcher(spaceName);
		if (matcher.find()) {
			return Optional.of(Integer.valueOf(matcher.group(1)));
		} else {
			return Optional.empty();
		}
	}

	private static int safeAbsoluteValue(int value) {
		return value == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(value);
	}

}
