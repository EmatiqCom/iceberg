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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewVersion;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Read-only view support for the views Athena and Trino create in Glue.
 *
 * <p>Those carry {@code presto_view=true} and keep the query in {@code ViewOriginalText} as {@code
 * /* Presto View: <base64 json> *}{@code /}, rather than the {@code table_type=iceberg-view} plus
 * {@code metadata_location} pair {@link GlueViewOperations} understands. Everything an Iceberg view
 * needs is already in the Glue entry, so it can be presented as a {@link ViewMetadata} synthesized
 * on load, with no metadata file and no write path.
 *
 * <p>The SQL is handed to the engine as a {@code trino} dialect representation and is rewritten
 * only as far as identifier quoting: {@code "x"} to {@code `x`}, and the catalog qualifier dropped.
 * That is lossless, so a view whose SQL the engine cannot handle fails in the engine's parser
 * rather than returning different rows than Athena would. Nothing here translates types or
 * functions.
 */
class PrestoViews {

  private static final String PRESTO_VIEW_PARAM = "presto_view";
  private static final String VIEW_TEXT_PREFIX = "/* Presto View: ";
  private static final String VIEW_TEXT_SUFFIX = " */";
  private static final String DIALECT = "trino";

  /** Not a dependency on iceberg-spark: the property name is part of the view format. */
  private static final class SparkViewProperties {
    private static final String QUERY_COLUMN_NAMES = "spark.query-column-names";

    private SparkViewProperties() {}
  }

  private PrestoViews() {}

  static boolean isPrestoView(Table glueTable) {
    return glueTable != null
        && GlueCatalog.GLUE_VIRTUAL_VIEW_TYPE.equalsIgnoreCase(glueTable.tableType())
        && glueTable.parameters() != null
        && "true".equalsIgnoreCase(glueTable.parameters().get(PRESTO_VIEW_PARAM));
  }

  /** Builds the read-only {@link ViewOperations} a {@code BaseView} needs for a Presto view. */
  static ViewOperations viewOps(Table glueTable, TableIdentifier identifier) {
    return new ReadOnlyViewOperations(metadata(glueTable, identifier));
  }

  private static ViewMetadata metadata(Table glueTable, TableIdentifier identifier) {
    JsonNode definition = decodeViewText(glueTable.viewOriginalText(), identifier);
    String originalSql = definition.get("originalSql").asText();
    String catalog = definition.has("catalog") ? definition.get("catalog").asText() : null;

    // The Glue column list is written once, when the view is created, and Athena re-resolves the
    // query at read time instead of trusting it. An engine reading through Iceberg does trust it:
    // Spark aligns the query to this schema positionally, so a stale list silently serves the wrong
    // columns whenever the types happen to line up. For an explicit projection the list cannot go
    // stale without the view itself being replaced, which rewrites it. For a star it can, so
    // refuse.
    ValidationException.check(
        !hasProjectionStar(originalSql),
        "Cannot read Presto view %s: its outer projection selects *, so the column list Glue stores "
            + "for it may be stale. Recreate it with an explicit projection, or read it in Athena.",
        identifier);

    Schema schema = schema(glueTable.storageDescriptor().columns(), identifier);

    ViewVersion version =
        ImmutableViewVersion.builder()
            .versionId(1)
            .schemaId(schema.schemaId())
            .timestampMillis(System.currentTimeMillis())
            .defaultCatalog(null)
            .defaultNamespace(identifier.namespace())
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .dialect(DIALECT)
                    .sql(toEngineIdentifiers(originalSql, catalog))
                    .build())
            .build();

    // No setMetadataLocation: there is no metadata file, and the location is only ever used to
    // place one. ReadOnlyViewOperations rejects every commit, so it stays unused.
    return ViewMetadata.builder()
        .setProperties(
            ImmutableMap.of(
                PRESTO_VIEW_PARAM,
                "true",
                "comment",
                "Presto/Athena view, read-only through Iceberg",
                // Written for format consistency with the views iceberg-spark creates. Spark 4.1
                // does not read it back - no class outside the connector interface references
                // queryColumnNames - so it is not what keeps a stale schema safe. See
                // hasProjectionStar for that.
                SparkViewProperties.QUERY_COLUMN_NAMES,
                String.join(",", queryColumnNames(schema))))
        .setLocation(String.format("glue://%s", identifier))
        .setCurrentVersion(version, schema)
        .build();
  }

  private static List<String> queryColumnNames(Schema schema) {
    return schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
  }

  private static JsonNode decodeViewText(String viewOriginalText, TableIdentifier identifier) {
    ValidationException.check(
        viewOriginalText != null
            && viewOriginalText.startsWith(VIEW_TEXT_PREFIX)
            && viewOriginalText.endsWith(VIEW_TEXT_SUFFIX),
        "Cannot read Presto view %s: ViewOriginalText is not a Presto view envelope",
        identifier);

    String encoded =
        viewOriginalText.substring(
            VIEW_TEXT_PREFIX.length(), viewOriginalText.length() - VIEW_TEXT_SUFFIX.length());

    try {
      JsonNode definition = JsonUtil.mapper().readTree(Base64.getDecoder().decode(encoded.trim()));
      ValidationException.check(
          definition.hasNonNull("originalSql"),
          "Cannot read Presto view %s: no originalSql in the view definition",
          identifier);
      return definition;
    } catch (IllegalArgumentException e) {
      throw new ValidationException(
          e, "Cannot read Presto view %s: ViewOriginalText is not valid base64", identifier);
    } catch (java.io.IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Builds the view schema from the Glue columns rather than from the types inside the base64
   * definition: those are Trino type names, while the Glue columns are what Athena itself reports
   * for the view.
   */
  @VisibleForTesting
  static Schema schema(List<Column> columns, TableIdentifier identifier) {
    ValidationException.check(
        columns != null && !columns.isEmpty(),
        "Cannot read Presto view %s: no columns in the Glue table",
        identifier);

    // Every column is optional: a view column is nullable unless the engine proves otherwise, and
    // claiming required here would let a null through a non-null contract.
    AtomicInteger nextId = new AtomicInteger(1);
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(columns.size());
    for (Column column : columns) {
      int id = nextId.getAndIncrement();
      fields.add(
          Types.NestedField.optional(
              id, column.name(), icebergType(column.type(), column.name(), nextId)));
    }

    return new Schema(0, fields);
  }

  /**
   * Maps a Hive type name from the Glue column to an Iceberg type. Anything outside the small set
   * Athena views actually use throws, so the view fails to load with a clear reason instead of
   * being served under a guessed schema.
   */
  private static Type icebergType(String hiveType, String columnName, AtomicInteger nextId) {
    String type = hiveType == null ? "" : hiveType.trim().toLowerCase(Locale.ROOT);

    if (type.startsWith("array<") && type.endsWith(">")) {
      String element = type.substring("array<".length(), type.length() - 1);
      int elementId = nextId.getAndIncrement();
      return Types.ListType.ofOptional(elementId, icebergType(element, columnName, nextId));
    }

    if (type.startsWith("decimal")) {
      return decimalType(type, columnName);
    }

    // varchar(n) / char(n): the length carries no meaning in Iceberg.
    String base = type.contains("(") ? type.substring(0, type.indexOf('(')) : type;

    switch (base) {
      case "string":
      case "varchar":
      case "char":
        return Types.StringType.get();
      case "int":
      case "integer":
      case "smallint":
      case "tinyint":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "double":
        return Types.DoubleType.get();
      case "float":
      case "real":
        return Types.FloatType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "date":
        return Types.DateType.get();
      case "timestamp":
        return Types.TimestampType.withoutZone();
      case "binary":
        return Types.BinaryType.get();
      default:
        throw new ValidationException(
            "Cannot read Presto view: unsupported type %s on column %s", hiveType, columnName);
    }
  }

  private static Type decimalType(String type, String columnName) {
    if (type.equals("decimal")) {
      return Types.DecimalType.of(10, 0); // the Hive default
    }

    int open = type.indexOf('(');
    int close = type.indexOf(')');
    ValidationException.check(
        open > 0 && close > open,
        "Cannot read Presto view: malformed decimal type %s on column %s",
        type,
        columnName);
    String args = type.substring(open + 1, close);
    int comma = args.indexOf(',');
    ValidationException.check(
        comma > 0,
        "Cannot read Presto view: malformed decimal type %s on column %s",
        type,
        columnName);
    return Types.DecimalType.of(
        Integer.parseInt(args.substring(0, comma).trim()),
        Integer.parseInt(args.substring(comma + 1).trim()));
  }

  /**
   * Rewrites Trino's quoted identifiers to the backquoted form engines like Spark expect, and drops
   * the catalog qualifier, which no engine reading this catalog has a name for.
   *
   * <p>Quoting only. String literals and comments are copied through untouched, so this cannot
   * change what the query means - an incompatible construct surfaces as a parse error in the
   * engine.
   */
  @VisibleForTesting
  static String toEngineIdentifiers(String sql, String catalog) {
    StringBuilder out = new StringBuilder(sql.length());
    int i = 0;

    while (i < sql.length()) {
      char c = sql.charAt(i);

      int nonCode = endOfNonCode(sql, i);
      if (nonCode >= 0) {
        out.append(sql, i, nonCode);
        i = nonCode;
      } else if (c == '"') {
        StringBuilder identifier = new StringBuilder();
        i = readQuotedIdentifier(sql, i, identifier);
        String name = identifier.toString();

        // Skip "catalog". - keep the database and table, which the engine can resolve.
        if (name.equals(catalog) && i < sql.length() && sql.charAt(i) == '.') {
          i++;
          continue;
        }

        out.append('`').append(name.replace("`", "``")).append('`');
      } else {
        out.append(c);
        i++;
      }
    }

    return out.toString();
  }

  /**
   * True when the outermost {@code SELECT} list contains a star. Only depth zero counts: a star
   * inside a subquery or CTE cannot widen the view's own output, because the outer projection names
   * what it takes from it.
   */
  @VisibleForTesting
  static boolean hasProjectionStar(String sql) {
    int depth = 0;
    boolean inProjection = false;
    int i = 0;

    while (i < sql.length()) {
      int nonCode = endOfNonCode(sql, i);
      if (nonCode >= 0) {
        i = nonCode;
        continue;
      }

      char c = sql.charAt(i);
      if (c == '"') {
        i = readQuotedIdentifier(sql, i, new StringBuilder());
        continue;
      }

      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && c == '*' && inProjection) {
        return true;
      } else if (depth == 0 && keywordAt(sql, i, "select")) {
        inProjection = true;
        i += "select".length();
        continue;
      } else if (depth == 0 && keywordAt(sql, i, "from")) {
        // Only the outermost projection matters, and it ends at its own FROM.
        return false;
      }

      i++;
    }

    return false;
  }

  private static boolean keywordAt(String sql, int i, String keyword) {
    if (!sql.regionMatches(true, i, keyword, 0, keyword.length())) {
      return false;
    }

    boolean startsWord =
        i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1)) && sql.charAt(i - 1) != '_';
    int after = i + keyword.length();
    boolean endsWord =
        after >= sql.length()
            || !Character.isLetterOrDigit(sql.charAt(after)) && sql.charAt(after) != '_';
    return startsWord && endsWord;
  }

  /**
   * Returns the index just past the string literal or comment starting at {@code i}, or -1 when
   * none starts there.
   */
  private static int endOfNonCode(String sql, int i) {
    char c = sql.charAt(i);
    if (c == '\'') {
      return endOfLiteral(sql, i);
    }

    if (c == '-' && next(sql, i) == '-') {
      int end = sql.indexOf('\n', i);
      return end < 0 ? sql.length() : end;
    }

    if (c == '/' && next(sql, i) == '*') {
      int end = sql.indexOf("*/", i + 2);
      return end < 0 ? sql.length() : end + 2;
    }

    return -1;
  }

  /** Returns the index just past the closing quote of the literal starting at {@code start}. */
  private static int endOfLiteral(String sql, int start) {
    int i = start + 1;
    while (i < sql.length()) {
      if (sql.charAt(i) == '\'') {
        if (next(sql, i) == '\'') { // '' is an escaped quote, not the end
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }

    throw new ValidationException(
        "Cannot read Presto view: unterminated string literal in the SQL");
  }

  /**
   * Reads the identifier starting at {@code start} into {@code identifier}, unescaping {@code ""}.
   */
  private static int readQuotedIdentifier(String sql, int start, StringBuilder identifier) {
    int i = start + 1;
    while (i < sql.length()) {
      if (sql.charAt(i) == '"') {
        if (next(sql, i) == '"') {
          identifier.append('"');
          i += 2;
          continue;
        }
        return i + 1;
      }
      identifier.append(sql.charAt(i));
      i++;
    }

    throw new ValidationException(
        "Cannot read Presto view: unterminated quoted identifier in the SQL");
  }

  private static char next(String sql, int i) {
    return i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
  }

  /** Serves the synthesized metadata and refuses every write. */
  private static class ReadOnlyViewOperations implements ViewOperations {
    private final ViewMetadata metadata;

    ReadOnlyViewOperations(ViewMetadata metadata) {
      this.metadata = metadata;
    }

    @Override
    public ViewMetadata current() {
      return metadata;
    }

    @Override
    public ViewMetadata refresh() {
      return metadata;
    }

    @Override
    public void commit(ViewMetadata base, ViewMetadata newMetadata) {
      throw new UnsupportedOperationException(
          "Cannot modify a Presto/Athena view through Iceberg: recreate it in the engine that owns it");
    }
  }
}
