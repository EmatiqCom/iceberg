/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.glue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Table;

public class TestPrestoViews {

  private static final String CATALOG = "awsdatacatalog";
  private static final TableIdentifier IDENTIFIER =
      TableIdentifier.of(Namespace.of("hd_mapper"), "source_market");

  @Test
  public void testCatalogQualifierDroppedAndIdentifiersBackquoted() {
    assertThat(
            PrestoViews.toEngineIdentifiers(
                "SELECT a FROM \"awsdatacatalog\".\"hd_mapper\".\"source_market\"", CATALOG))
        .isEqualTo("SELECT a FROM `hd_mapper`.`source_market`");
  }

  @Test
  public void testOnlyTheCatalogQualifierIsDropped() {
    // A column or alias that happens to be called awsdatacatalog is not a qualifier: no trailing
    // dot.
    assertThat(PrestoViews.toEngineIdentifiers("SELECT \"awsdatacatalog\" FROM \"t\"", CATALOG))
        .isEqualTo("SELECT `awsdatacatalog` FROM `t`");
  }

  @Test
  public void testStringLiteralsAreNeverTouched() {
    // The double quotes here are data, not an identifier. Rewriting them would change the rows.
    String sql = "SELECT \"c\" FROM \"t\" WHERE \"c\" = 'say \"hi\"'";
    assertThat(PrestoViews.toEngineIdentifiers(sql, CATALOG))
        .isEqualTo("SELECT `c` FROM `t` WHERE `c` = 'say \"hi\"'");
  }

  @Test
  public void testEscapedQuoteInsideStringLiteral() {
    String sql = "SELECT \"c\" FROM \"t\" WHERE \"c\" = 'it''s \"x\"'";
    assertThat(PrestoViews.toEngineIdentifiers(sql, CATALOG))
        .isEqualTo("SELECT `c` FROM `t` WHERE `c` = 'it''s \"x\"'");
  }

  @Test
  public void testEscapedQuoteInsideIdentifier() {
    assertThat(PrestoViews.toEngineIdentifiers("SELECT \"a\"\"b\" FROM \"t\"", CATALOG))
        .isEqualTo("SELECT `a\"b` FROM `t`");
  }

  @Test
  public void testBackquoteInsideIdentifierIsEscaped() {
    assertThat(PrestoViews.toEngineIdentifiers("SELECT \"a`b\" FROM \"t\"", CATALOG))
        .isEqualTo("SELECT `a``b` FROM `t`");
  }

  @Test
  public void testCommentsAreCopiedVerbatim() {
    String sql = "SELECT \"a\" -- keep \"this\"\nFROM /* and \"this\" */ \"t\"";
    assertThat(PrestoViews.toEngineIdentifiers(sql, CATALOG))
        .isEqualTo("SELECT `a` -- keep \"this\"\nFROM /* and \"this\" */ `t`");
  }

  @Test
  public void testUnterminatedLiteralAndIdentifierFailLoudly() {
    assertThatThrownBy(() -> PrestoViews.toEngineIdentifiers("SELECT 'oops", CATALOG))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("unterminated string literal");

    assertThatThrownBy(() -> PrestoViews.toEngineIdentifiers("SELECT \"oops", CATALOG))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("unterminated quoted identifier");
  }

  @Test
  public void testScalarTypesFromGlueColumns() {
    Schema schema =
        PrestoViews.schema(
            ImmutableList.of(
                column("a", "string"),
                column("b", "int"),
                column("c", "bigint"),
                column("d", "double"),
                column("e", "float"),
                column("f", "boolean"),
                column("g", "date"),
                column("h", "timestamp"),
                column("i", "varchar(20)"),
                column("j", "decimal(38,9)")),
            IDENTIFIER);

    assertThat(schema.columns()).hasSize(10);
    assertThat(schema.findType("a")).isEqualTo(Types.StringType.get());
    assertThat(schema.findType("b")).isEqualTo(Types.IntegerType.get());
    assertThat(schema.findType("c")).isEqualTo(Types.LongType.get());
    assertThat(schema.findType("d")).isEqualTo(Types.DoubleType.get());
    assertThat(schema.findType("e")).isEqualTo(Types.FloatType.get());
    assertThat(schema.findType("f")).isEqualTo(Types.BooleanType.get());
    assertThat(schema.findType("g")).isEqualTo(Types.DateType.get());
    assertThat(schema.findType("h")).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(schema.findType("i")).isEqualTo(Types.StringType.get());
    assertThat(schema.findType("j")).isEqualTo(Types.DecimalType.of(38, 9));

    // A view column is nullable, and nothing here can prove otherwise.
    assertThat(schema.columns()).allMatch(field -> field.isOptional());
  }

  @Test
  public void testArrayType() {
    Schema schema = PrestoViews.schema(ImmutableList.of(column("a", "array<string>")), IDENTIFIER);
    assertThat(schema.findType("a")).isInstanceOf(Types.ListType.class);
    assertThat(schema.findType("a").asListType().elementType()).isEqualTo(Types.StringType.get());
  }

  @Test
  public void testUnsupportedTypeFailsRatherThanGuessing() {
    List<Column> columns = ImmutableList.of(column("a", "struct<x:int>"));
    assertThatThrownBy(() -> PrestoViews.schema(columns, IDENTIFIER))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("unsupported type struct<x:int>");
  }

  @Test
  public void testNoColumnsFails() {
    List<Column> empty = ImmutableList.of();
    assertThatThrownBy(() -> PrestoViews.schema(empty, IDENTIFIER))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("no columns");
  }

  @Test
  public void testProjectionStarIsDetected() {
    assertThat(PrestoViews.hasProjectionStar("SELECT * FROM \"t\"")).isTrue();
    assertThat(PrestoViews.hasProjectionStar("SELECT\n  amr.*\n, x\nFROM \"t\"")).isTrue();
  }

  @Test
  public void testStarsThatCannotWidenTheViewAreNotFlagged() {
    // An aggregate star: the projection is one column whatever the source does.
    assertThat(PrestoViews.hasProjectionStar("SELECT count(*) FROM \"t\"")).isFalse();
    // Multiplication, which Trino parenthesises.
    assertThat(PrestoViews.hasProjectionStar("SELECT (a * b) AS c FROM \"t\"")).isFalse();
    // A star inside a CTE or subquery cannot widen the outer projection.
    assertThat(PrestoViews.hasProjectionStar("WITH c AS (SELECT * FROM \"t\") SELECT a FROM c"))
        .isFalse();
    assertThat(PrestoViews.hasProjectionStar("SELECT a FROM (SELECT * FROM \"t\") x")).isFalse();
    // A star in a literal or a comment is not a projection.
    assertThat(PrestoViews.hasProjectionStar("SELECT a FROM \"t\" WHERE b = '*'")).isFalse();
    assertThat(PrestoViews.hasProjectionStar("SELECT a -- keep *\nFROM \"t\"")).isFalse();
  }

  @Test
  public void testIcebergViewIsNeverTreatedAsPrestoView() {
    // An Iceberg view's properties reach the Glue parameters, so presto_view can ride along.
    Table both =
        Table.builder()
            .name("v")
            .tableType("VIRTUAL_VIEW")
            .parameters(ImmutableMap.of("table_type", "iceberg-view", "presto_view", "true"))
            .build();
    assertThat(PrestoViews.isPrestoView(both)).isFalse();

    Table presto =
        Table.builder()
            .name("v")
            .tableType("VIRTUAL_VIEW")
            .parameters(ImmutableMap.of("presto_view", "true"))
            .build();
    assertThat(PrestoViews.isPrestoView(presto)).isTrue();
  }

  @Test
  public void testCatalogNameIsKeptWhenItIsNotARelationQualifier() {
    // Two-part: an alias or field that happens to carry the catalog's name. Dropping the qualifier
    // would resolve something else, so it stays and the engine decides.
    assertThat(
            PrestoViews.toEngineIdentifiers(
                "SELECT \"awsdatacatalog\".\"x\" FROM \"t\" \"awsdatacatalog\"", CATALOG))
        .isEqualTo("SELECT `awsdatacatalog`.`x` FROM `t` `awsdatacatalog`");

    // Three-part: the shape Trino writes for a relation, so the catalog goes.
    assertThat(
            PrestoViews.toEngineIdentifiers(
                "SELECT a FROM \"awsdatacatalog\".\"db\".\"t\"", CATALOG))
        .isEqualTo("SELECT a FROM `db`.`t`");
  }

  @Test
  public void testParenthesisedRootQueryDoesNotBypassTheStarCheck() {
    assertThat(PrestoViews.hasProjectionStar("(SELECT * FROM \"t\")")).isTrue();
    assertThat(PrestoViews.hasProjectionStar("  ((SELECT * FROM \"t\"))  ")).isTrue();
    // Unwrapping must not turn an explicit projection into a refusal.
    assertThat(PrestoViews.hasProjectionStar("(SELECT a FROM \"t\")")).isFalse();
  }

  @Test
  public void testUnlocatableProjectionIsRefused() {
    // The root query is nested in a shape the walk cannot unwrap, so it never sees a depth-zero
    // SELECT list. Refusing beats serving a possible star off a possibly stale schema.
    assertThat(PrestoViews.hasProjectionStar("WITH x AS (SELECT 1) (SELECT * FROM x)")).isTrue();
  }

  private static Column column(String name, String type) {
    return Column.builder().name(name).type(type).build();
  }
}
