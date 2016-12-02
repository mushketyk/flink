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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.connectors.kafka.internals.TypeUtil;
import org.apache.flink.streaming.util.serialization.AvroRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.DefaultGenericRecordToRowConverterFactory;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;

public class Kafka08AvroTableSourceTest extends KafkaTableSourceTestBase {

	@Override
	protected KafkaTableSource createTableSource(String topic, Properties properties, String[] fieldNames, TypeInformation<?>[] typeInfo) {
		return new Kafka08AvroTableSource(topic, properties, fieldNames, typeInfo,
										TypeUtil.createRowAvroSchema(fieldNames, typeInfo),
										new DefaultGenericRecordToRowConverterFactory());
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Class<DeserializationSchema<Row>> getDeserializationSchema() {
		return (Class) AvroRowDeserializationSchema.class;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Class<FlinkKafkaConsumerBase<Row>> getFlinkKafkaConsumer() {
		return (Class) FlinkKafkaConsumer08.class;
	}
}

