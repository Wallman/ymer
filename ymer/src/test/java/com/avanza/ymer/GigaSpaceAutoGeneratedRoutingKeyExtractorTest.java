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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;


public class GigaSpaceAutoGeneratedRoutingKeyExtractorTest {

	@SpaceClass
	private static class TestWithAutoGenerateClass {
		private final String id;
		
		public TestWithAutoGenerateClass(String id) {
			this.id = id;
		}

		@SpaceId(autoGenerate = true)
		public String getId() {
			return id;
		}
	}
	
	@SpaceClass
	private static class TestWithoutAutoGenerateClass {
		private String id;
		
		@SpaceId(autoGenerate = false)
		public String getId() {
			return id;
		}
	}
	
	@Test
	public void isApplicable() throws Exception {
		assertTrue(RoutingKeyExtractor.GsAutoGenerated.isApplicable(TestWithAutoGenerateClass.class.getMethod("getId")));
		assertFalse(RoutingKeyExtractor.GsAutoGenerated.isApplicable(TestWithoutAutoGenerateClass.class.getMethod("getId")));
	}
	
	@Test
	public void throwsIllegalArgumentExceptionWhenNotApplicable() throws Exception {
		assertThrows(IllegalArgumentException.class, () -> new RoutingKeyExtractor.GsAutoGenerated(TestWithoutAutoGenerateClass.class.getMethod("getId")));
	}
	
	@Test
	public void extractId() throws Exception {
		assertEquals("A3", new RoutingKeyExtractor.GsAutoGenerated(TestWithAutoGenerateClass.class.getMethod("getId")).getRoutingKey(new TestWithAutoGenerateClass("A3^24324234324324^434")));
		assertEquals("A3", new RoutingKeyExtractor.GsAutoGenerated(TestWithAutoGenerateClass.class.getMethod("getId")).getRoutingKey(new TestWithAutoGenerateClass("A3_1^24324234324324^434")));
		assertEquals("A13", new RoutingKeyExtractor.GsAutoGenerated(TestWithAutoGenerateClass.class.getMethod("getId")).getRoutingKey(new TestWithAutoGenerateClass("A13^24324234324324^434")));
	}
	
}
