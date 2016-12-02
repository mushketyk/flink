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
package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Arrays;

public class TypeUtil {
	private TypeUtil() {}

	/**
	 * Creates TypeInformation array for an array of Classes.
	 * @param fieldTypes classes to extract type information from
	 * @return type information
	 */
	public static TypeInformation<?>[] toTypeInfo(Class<?>[] fieldTypes) {
		TypeInformation<?>[] typeInfos = new TypeInformation[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			typeInfos[i] = TypeExtractor.getForClass(fieldTypes[i]);
		}
		return typeInfos;
	}

	/**
	 * Create Avro serialization/deserialization schema for a row definition
	 * @param fieldNames row field names
	 * @param fieldTypes row field fieldTypes
	 * @return Avro serialization/deserialization schema
	 */
	public static Schema createRowAvroSchema(String[] fieldNames, TypeInformation[] fieldTypes) {
		SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
			.record("Row").namespace("org.apache.flink.streaming")
			.fields();

		final Schema nullSchema = Schema.create(Schema.Type.NULL);

		for (int i = 0; i < fieldNames.length; i++) {
			Schema schema = ReflectData.get().getSchema(fieldTypes[i].getTypeClass());
			Schema unionSchema = Schema.createUnion(Arrays.asList(nullSchema, schema));
			fieldAssembler.name(fieldNames[i]).type(unionSchema).noDefault();
		}

		return fieldAssembler.endRecord();
	}
}
