/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.util;

import org.apache.flink.metrics.Meter;

public class TestMeter implements Meter {

	@Override
	public void markEvent() { }

	@Override
	public void markEvent(long n) { }

	@Override
	public double getOneMinuteRate() {
								   return 1;
											}

	@Override
	public double getFiveMinuteRate() {
									return 5;
											 }

	@Override
	public double getFifteenMinuteRate() {
									   return 15;
												 }

	@Override
	public double getMeanRate() {
							  return 10;
										}

	@Override
	public long getCount() {
						 return 100L;
									 }
}
