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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory

abstract class FlinkTable[T](
    val typeInfo: TypeInformation[T],
    val fieldIndexes: Array[Int],
    val fieldNames: Array[String])
  extends AbstractTable {

  if (fieldIndexes.length != fieldNames.length) {
    throw new TableException(
      "Number of field indexes and field names must be equal.")
  }

  // check uniqueness of field names
  if (fieldNames.length != fieldNames.toSet.size) {
    throw new TableException(
      "Table field names must be unique.")
  }

  val fieldTypes: Array[TypeInformation[_]] = {
    val fieldNames = TableEnvironment.getFieldNames(typeInfo)
    if (fieldNames.length == fieldIndexes.length) {
      TableEnvironment.getFieldTypes(typeInfo)
    } else {
      throw new TableException(
        s"Arity of type (" + fieldNames.deep + ") " +
          "not equal to number of field names " + fieldNames.deep + ".")
    }
  }

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    flinkTypeFactory.buildRowDataType(fieldNames, fieldTypes)
  }

}
