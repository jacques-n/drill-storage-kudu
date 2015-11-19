/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.kudu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.hadoop.kudu.HTableDescriptor;
import org.apache.hadoop.kudu.client.KuduAdmin;
import org.apache.hadoop.kudu.util.Bytes;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class DrillKuduTable extends DrillTable implements DrillKuduConstants {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillKuduTable.class);

  private HTableDescriptor table;

  public DrillKuduTable(String storageEngineName, KuduStoragePlugin plugin, KuduScanSpec scanSpec) {
    super(storageEngineName, plugin, scanSpec);
    try(KuduAdmin admin = new KuduAdmin(plugin.getConfig().getKuduConf())) {
      table = admin.getTableDescriptor(KuduUtils.getBytes(scanSpec.getTableName()));
    } catch (IOException e) {
      throw UserException.dataReadError()
          .message("Failure while loading table %s in database %s.", scanSpec.getTableName(), storageEngineName)
          .addContext("Message: ", e.getMessage())
          .build(logger);
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    ArrayList<RelDataType> typeList = new ArrayList<>();
    ArrayList<String> fieldNameList = new ArrayList<>();

    fieldNameList.add(ROW_KEY);
    typeList.add(typeFactory.createSqlType(SqlTypeName.ANY));

    Set<byte[]> families = table.getFamiliesKeys();
    for (byte[] family : families) {
      fieldNameList.add(Bytes.toString(family));
      typeList.add(typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.ANY)));
    }
    return typeFactory.createStructType(typeList, fieldNameList);
  }

}
