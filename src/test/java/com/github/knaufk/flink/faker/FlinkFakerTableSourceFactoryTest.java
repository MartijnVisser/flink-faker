package com.github.knaufk.flink.faker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

class FlinkFakerTableSourceFactoryTest {

  private static final ResolvedSchema VALID_SCHEMA =
      new ResolvedSchema(
          Arrays.asList(
              Column.physical("f0", DataTypes.TINYINT()),
              Column.physical("f1", DataTypes.SMALLINT()),
              Column.physical("f2", DataTypes.INT()),
              Column.physical("f3", DataTypes.BIGINT()),
              Column.physical("f4", DataTypes.DOUBLE()),
              Column.physical("f5", DataTypes.FLOAT()),
              Column.physical("f6", DataTypes.DECIMAL(6, 2)),
              Column.physical("f7", DataTypes.CHAR(10)),
              Column.physical("f8", DataTypes.VARCHAR(255)),
              Column.physical("f9", DataTypes.STRING()),
              Column.physical("f10", DataTypes.BOOLEAN()),
              Column.physical("f11", DataTypes.ARRAY(DataTypes.INT())),
              Column.physical("f12", DataTypes.MAP(DataTypes.INT(), DataTypes.VARCHAR(255))),
              Column.physical("f13", DataTypes.ROW(DataTypes.FIELD("age", DataTypes.INT()))),
              Column.physical("f14", DataTypes.MULTISET(DataTypes.CHAR(10)))),
          Collections.emptyList(),
          null);

  private static final ResolvedSchema INVALID_SCHEMA =
      new ResolvedSchema(
          Arrays.asList(
              Column.physical("f0", DataTypes.STRING()),
              Column.physical("f1", DataTypes.VARCHAR(100)),
              Column.physical("f2", DataTypes.NULL())),
          Collections.emptyList(),
          null);

  private static final ResolvedSchema TINY_SCHEMA =
      new ResolvedSchema(
          Collections.singletonList(Column.physical("f0", DataTypes.TINYINT())),
          Collections.emptyList(),
          null);

  @Test
  public void testSchemaWithNonSupportedTypesIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
              properties.put("fields.f1.expression", "#{number.numberBetween '-32768','32767'}");
              properties.put(
                  "fields.f2.expression", "#{number.numberBetween '-2147483648','2147483647'}");
              properties.put("fields.f3.expression", "#{number.randomNumber '12','false'}");
              properties.put("fields.f4.expression", "#{number.randomDouble '3','-1000','1000'}");
              properties.put("fields.f5.expression", "#{number.randomDouble '3','-1000','1000'}");
              properties.put("fields.f6.expression", "#{number.randomDouble '3','-1000','1000'}");
              properties.put("fields.f7.expression", "#{Lorem.characters '10'}");
              properties.put("fields.f8.expression", "#{Lorem.characters '255'}");
              properties.put("fields.f9.expression", "#{Lorem.sentence}");
              properties.put("fields.f10.expression", "#{regexify '(true|false){1}'}");
              createTableSource(properties, INVALID_SCHEMA);
            })
        .withStackTraceContaining("f2 is NULL.");
  }

  @Test
  public void testValidNullRateIsValid() {
    Map<String, String> properties = new HashMap();
    properties.put(FactoryUtil.CONNECTOR.key(), "faker");
    properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
    properties.put("fields.f0.null-rate", "0.1");

    createTableSource(properties, TINY_SCHEMA);
  }

  @Test
  public void testNegativeNullRateIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
              properties.put("fields.f0.null-rate", "-0.8");

              createTableSource(properties, TINY_SCHEMA);
            })
        .withStackTraceContaining("needs to be in [0,1]");
  }

  @Test
  public void testNullRateGreaterOneIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
              properties.put("fields.f0.null-rate", "1.01");

              createTableSource(properties, TINY_SCHEMA);
            })
        .withStackTraceContaining("needs to be in [0,1]");
  }

  @Test
  public void testPropertiesWithoutExpressionForOnecolumnIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.randomDigit}");
              properties.put("fields.f1.expression", "#{number.randomDigit}");

              createTableSource(properties, VALID_SCHEMA);
            })
        .withStackTraceContaining("No expression found for f2.");
  }

  @Test
  public void testInvalidExpressionIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.abc}");
              properties.put("fields.f1.expression", "#{number.randomDigit}");

              createTableSource(properties, VALID_SCHEMA);
            })
        .withStackTraceContaining("Invalid expression for column \"f0\".");
  }

  @Test
  public void testValidTableSourceIsValid() {

    Map<String, String> properties = new HashMap();
    properties.put(FactoryUtil.CONNECTOR.key(), "faker");
    properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
    properties.put("fields.f1.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f2.expression", "#{number.numberBetween '-2147483648','2147483647'}");
    properties.put("fields.f3.expression", "#{number.randomNumber '12','false'}");
    properties.put("fields.f4.expression", "#{number.randomDouble '3','-1000','1000'}");
    properties.put("fields.f5.expression", "#{number.randomDouble '3','-1000','1000'}");
    properties.put("fields.f6.expression", "#{number.randomDouble '3','-1000','1000'}");
    properties.put("fields.f7.expression", "#{Lorem.characters '10'}");
    properties.put("fields.f8.expression", "#{Lorem.characters '255'}");
    properties.put("fields.f9.expression", "#{Lorem.sentence}");
    properties.put("fields.f10.expression", "#{regexify '(true|false){1}'}");
    properties.put("fields.f11.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f12.key.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f12.value.expression", "#{Lorem.characters '255'}");
    properties.put("fields.f13.age.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f14.expression", "#{Lorem.characters '10'}");

    createTableSource(properties, VALID_SCHEMA);
  }

  @Test
  public void testTimestamps() {

    Map<String, String> properties = new HashMap();
    properties.put(FactoryUtil.CONNECTOR.key(), "faker");

    properties.put("fields.f0.expression", "#{date.past '15','SECONDS'}");
    properties.put("fields.f1.expression", "#{date.past '15','SECONDS'}");
    properties.put("fields.f2.expression", "#{date.past '15','SECONDS'}");

    ResolvedSchema schema =
        new ResolvedSchema(
            Arrays.asList(
                Column.physical("f0", DataTypes.TIMESTAMP()),
                Column.physical("f1", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                Column.physical("f2", DataTypes.TIMESTAMP_WITH_TIME_ZONE())),
            Collections.emptyList(),
            null);

    createTableSource(properties, schema);
  }

  private DynamicTableSource createTableSource(
      Map<String, String> properties, ResolvedSchema resolvedSchema) {

    // Convert ResolvedSchema to Schema for CatalogTable
    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Column column : resolvedSchema.getColumns()) {
      if (column instanceof Column.PhysicalColumn) {
        schemaBuilder.column(column.getName(), column.getDataType());
      }
    }
    Schema schema = schemaBuilder.build();

    CatalogTable catalogTable =
        CatalogTable.newBuilder()
            .schema(schema)
            .partitionKeys(Collections.emptyList())
            .options(properties)
            .build();

    ResolvedCatalogTable resolvedCatalogTable =
        new ResolvedCatalogTable(catalogTable, resolvedSchema);

    FlinkFakerTableSourceFactory factory = new FlinkFakerTableSourceFactory();

    DynamicTableFactory.Context context =
        new DynamicTableFactory.Context() {
          @Override
          public ObjectIdentifier getObjectIdentifier() {
            return ObjectIdentifier.of("default_catalog", "default_database", "test_table");
          }

          @Override
          public ResolvedCatalogTable getCatalogTable() {
            return resolvedCatalogTable;
          }

          @Override
          public Configuration getConfiguration() {
            return new Configuration();
          }

          @Override
          public ClassLoader getClassLoader() {
            return Thread.currentThread().getContextClassLoader();
          }

          @Override
          public boolean isTemporary() {
            return true;
          }
        };

    return factory.createDynamicTableSource(context);
  }
}
