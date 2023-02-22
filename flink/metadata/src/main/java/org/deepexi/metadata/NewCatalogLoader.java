package org.deepexi.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;

public interface NewCatalogLoader extends CatalogLoader {
    static NewCatalogLoader tarim(String name, Configuration hadoopConf, Map<String, String> properties) {
        return new NewCatalogLoader.TarimCatalogLoader(name, hadoopConf, properties);
    }

    public static class TarimCatalogLoader implements NewCatalogLoader {
        private final String catalogName;
        private final SerializableConfiguration hadoopConf;
        private final String warehouseLocation;
        private final Map<String, String> properties;

        private TarimCatalogLoader(String catalogName, Configuration conf, Map<String, String> properties) {
            this.catalogName = catalogName;
            this.hadoopConf = new SerializableConfiguration(conf);
            this.warehouseLocation = (String)properties.get("warehouse");
            this.properties = Maps.newHashMap(properties);
        }

        public Catalog loadCatalog() {
            TarimCatalog tarimCatalog = new TarimCatalog();
            tarimCatalog.setConf(this.hadoopConf.get());
            tarimCatalog.initialize(this.catalogName, this.properties);

            return tarimCatalog;
        }

        public String toString() {
            return MoreObjects.toStringHelper(this).add("catalogName", this.catalogName).add("warehouseLocation", this.warehouseLocation).toString();
        }
    }
}