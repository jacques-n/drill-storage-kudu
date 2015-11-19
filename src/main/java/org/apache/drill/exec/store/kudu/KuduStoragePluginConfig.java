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

import java.util.Map;

import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

@JsonTypeName(KuduStoragePluginConfig.NAME)
public class KuduStoragePluginConfig extends StoragePluginConfigBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduStoragePluginConfig.class);

  private Map<String, String> config;

  @JsonIgnore
  private Configuration kuduConf;

  @JsonIgnore
  private Boolean sizeCalculatorEnabled;

  public static final String NAME = "kudu";

  @JsonCreator
  public KuduStoragePluginConfig(@JsonProperty("config") Map<String, String> props, @JsonProperty("size.calculator.enabled") Boolean sizeCalculatorEnabled) {
    this.config = props;
    if (config == null) {
      config = Maps.newHashMap();
    }
    logger.debug("Initializing Kudu StoragePlugin configuration with zookeeper quorum '{}', port '{}'.",
        config.get(HConstants.ZOOKEEPER_QUORUM), config.get(HBASE_ZOOKEEPER_PORT));
    if (sizeCalculatorEnabled == null) {
      this.sizeCalculatorEnabled = false;
    } else {
      this.sizeCalculatorEnabled = sizeCalculatorEnabled;
    }
  }

  @JsonProperty
  public Map<String, String> getConfig() {
    return ImmutableMap.copyOf(config);
  }

  @JsonProperty("size.calculator.enabled")
  public boolean isSizeCalculatorEnabled() {
    return sizeCalculatorEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KuduStoragePluginConfig that = (KuduStoragePluginConfig) o;
    return config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return this.config != null ? this.config.hashCode() : 0;
  }

  @JsonIgnore
  public Configuration getKuduConf() {
    if (kuduConf == null) {
      kuduConf = KuduConfiguration.create();
      if (config != null) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
          kuduConf.set(entry.getKey(), entry.getValue());
        }
      }
    }
    return kuduConf;
  }

  @JsonIgnore
  public String getZookeeperQuorum() {
    return getKuduConf().get(HConstants.ZOOKEEPER_QUORUM);
  }

  @JsonIgnore
  public String getZookeeperport() {
    return getKuduConf().get(HBASE_ZOOKEEPER_PORT);
  }

  @JsonIgnore
  @VisibleForTesting
  public void setZookeeperPort(int zookeeperPort) {
    this.config.put(HBASE_ZOOKEEPER_PORT, String.valueOf(zookeeperPort));
    getKuduConf().setInt(HBASE_ZOOKEEPER_PORT, zookeeperPort);
  }

}
