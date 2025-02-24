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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.DescribeCatalogOperation;
import org.apache.flink.table.operations.DescribeFunctionOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowCatalogsOperation;
import org.apache.flink.table.operations.ShowCreateCatalogOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation.FunctionScope;
import org.apache.flink.table.operations.ShowModulesOperation;
import org.apache.flink.table.operations.ShowPartitionsOperation;
import org.apache.flink.table.operations.ShowProceduresOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.UseModulesOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.ShowJarsOperation;
import org.apache.flink.table.operations.utils.LikeType;
import org.apache.flink.table.operations.utils.ShowLikeOperator;
import org.apache.flink.table.planner.parse.ExtendedParser;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test cases for the statements that neither belong to DDL nor DML for {@link
 * SqlNodeToOperationConversion}.
 */
public class SqlOtherOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    void testUseCatalog() {
        final String sql = "USE CATALOG cat1";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseCatalogOperation.class);
        assertThat(((UseCatalogOperation) operation).getCatalogName()).isEqualTo("cat1");
        assertThat(operation.asSummaryString()).isEqualTo("USE CATALOG cat1");
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testDescribeCatalog(boolean abbr, boolean extended) {
        final String catalogName = "cat1";
        final String sql =
                String.format(
                        "%s CATALOG %s %s",
                        abbr ? "DESC" : "DESCRIBE", extended ? "EXTENDED" : "", catalogName);
        Operation operation = parse(sql);
        assertThat(operation)
                .isInstanceOf(DescribeCatalogOperation.class)
                .asInstanceOf(InstanceOfAssertFactories.type(DescribeCatalogOperation.class))
                .extracting(
                        DescribeCatalogOperation::getCatalogName,
                        DescribeCatalogOperation::isExtended,
                        DescribeCatalogOperation::asSummaryString)
                .containsExactly(
                        catalogName,
                        extended,
                        String.format(
                                "DESCRIBE CATALOG: (identifier: [%s], isExtended: [%b])",
                                catalogName, extended));
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testDescribeFunction(boolean abbr, boolean extended) {
        final String functionName = "f1";
        final UnresolvedIdentifier functionIdentifier = UnresolvedIdentifier.of(functionName);
        final String sql =
                String.format(
                        "%s FUNCTION %s %s",
                        abbr ? "DESC" : "DESCRIBE", extended ? "EXTENDED" : "", functionName);
        Operation operation = parse(sql);
        assertThat(operation)
                .isInstanceOf(DescribeFunctionOperation.class)
                .asInstanceOf(InstanceOfAssertFactories.type(DescribeFunctionOperation.class))
                .extracting(
                        DescribeFunctionOperation::getSqlIdentifier,
                        DescribeFunctionOperation::isExtended,
                        DescribeFunctionOperation::asSummaryString)
                .containsExactly(
                        functionIdentifier,
                        extended,
                        String.format(
                                "DESCRIBE FUNCTION: (identifier: [%s], isExtended: [%b])",
                                functionIdentifier, extended));
    }

    @Test
    void testUseDatabase() {
        final String sql1 = "USE db1";
        Operation operation1 = parse(sql1);
        assertThat(operation1).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation1).getCatalogName()).isEqualTo("builtin");
        assertThat(((UseDatabaseOperation) operation1).getDatabaseName()).isEqualTo("db1");

        final String sql2 = "USE cat1.db1";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(UseDatabaseOperation.class);
        assertThat(((UseDatabaseOperation) operation2).getCatalogName()).isEqualTo("cat1");
        assertThat(((UseDatabaseOperation) operation2).getDatabaseName()).isEqualTo("db1");
    }

    @Test
    void testUseDatabaseWithException() {
        final String sql = "USE cat1.db1.tbl1";
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(ValidationException.class);
    }

    @Test
    void testLoadModule() {
        final String sql = "LOAD MODULE dummy WITH ('k1' = 'v1', 'k2' = 'v2')";
        final String expectedModuleName = "dummy";
        final Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put("k2", "v2");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(LoadModuleOperation.class);
        final LoadModuleOperation loadModuleOperation = (LoadModuleOperation) operation;

        assertThat(loadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
        assertThat(loadModuleOperation.getOptions()).isEqualTo(expectedOptions);
    }

    @Test
    void testUnloadModule() {
        final String sql = "UNLOAD MODULE dummy";
        final String expectedModuleName = "dummy";

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UnloadModuleOperation.class);

        final UnloadModuleOperation unloadModuleOperation = (UnloadModuleOperation) operation;

        assertThat(unloadModuleOperation.getModuleName()).isEqualTo(expectedModuleName);
    }

    @Test
    void testUseOneModule() {
        final String sql = "USE MODULES dummy";
        final List<String> expectedModuleNames = Collections.singletonList("dummy");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [dummy]");
    }

    @Test
    void testUseMultipleModules() {
        final String sql = "USE MODULES x, y, z";
        final List<String> expectedModuleNames = Arrays.asList("x", "y", "z");

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(UseModulesOperation.class);

        final UseModulesOperation useModulesOperation = (UseModulesOperation) operation;

        assertThat(useModulesOperation.getModuleNames()).isEqualTo(expectedModuleNames);
        assertThat(useModulesOperation.asSummaryString()).isEqualTo("USE MODULES: [x, y, z]");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowCatalogsTest")
    void testShowCatalogs(String sql, ShowCatalogsOperation expected, String expectedSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowCatalogsOperation.class).isEqualTo(expected);
        assertThat(operation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private static Stream<Arguments> inputForShowCatalogsTest() {
        return Stream.of(
                Arguments.of("show catalogs", new ShowCatalogsOperation(null), "SHOW CATALOGS"),
                Arguments.of(
                        "show catalogs like 'c%'",
                        new ShowCatalogsOperation(ShowLikeOperator.of(LikeType.LIKE, "c%")),
                        "SHOW CATALOGS LIKE 'c%'"),
                Arguments.of(
                        "show catalogs not like 'c%'",
                        new ShowCatalogsOperation(ShowLikeOperator.of(LikeType.NOT_LIKE, "c%")),
                        "SHOW CATALOGS NOT LIKE 'c%'"),
                Arguments.of(
                        "show catalogs ilike 'c%'",
                        new ShowCatalogsOperation(ShowLikeOperator.of(LikeType.ILIKE, "c%")),
                        "SHOW CATALOGS ILIKE 'c%'"),
                Arguments.of(
                        "show catalogs not ilike 'c%'",
                        new ShowCatalogsOperation(ShowLikeOperator.of(LikeType.NOT_ILIKE, "c%")),
                        "SHOW CATALOGS NOT ILIKE 'c%'"));
    }

    @Test
    void testShowModules() {
        final String sql = "SHOW MODULES";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isFalse();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW MODULES");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowTablesTest")
    void testShowTables(String sql, ShowTablesOperation expected, String expectedSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowTablesOperation.class).isEqualTo(expected);
        assertThat(operation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private static Stream<Arguments> inputForShowTablesTest() {
        return Stream.of(
                Arguments.of(
                        "SHOW TABLES from cat1.db1 not like 't%'",
                        new ShowTablesOperation(
                                "cat1",
                                "db1",
                                "FROM",
                                ShowLikeOperator.of(LikeType.NOT_LIKE, "t%")),
                        "SHOW TABLES FROM cat1.db1 NOT LIKE 't%'"),
                Arguments.of(
                        "SHOW TABLES in db2",
                        new ShowTablesOperation("builtin", "db2", "IN", null),
                        "SHOW TABLES IN builtin.db2"),
                Arguments.of(
                        "SHOW TABLES",
                        new ShowTablesOperation("builtin", "default", null, null),
                        "SHOW TABLES"));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowViewsTest")
    void testShowViews(String sql, ShowViewsOperation expected, String expectedSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowViewsOperation.class).isEqualTo(expected);
        assertThat(operation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private static Stream<Arguments> inputForShowViewsTest() {
        return Stream.of(
                Arguments.of(
                        "SHOW VIEWS from cat1.db1 not like 't%'",
                        new ShowViewsOperation(
                                "cat1",
                                "db1",
                                "FROM",
                                ShowLikeOperator.of(LikeType.NOT_LIKE, "t%")),
                        "SHOW VIEWS FROM cat1.db1 NOT LIKE 't%'"),
                Arguments.of(
                        "SHOW VIEWS in db2",
                        new ShowViewsOperation("builtin", "db2", "IN", null),
                        "SHOW VIEWS IN builtin.db2"),
                Arguments.of(
                        "SHOW VIEWS",
                        new ShowViewsOperation("builtin", "default", null, null),
                        "SHOW VIEWS"));
    }

    @Test
    void testShowCreateCatalog() {
        Operation operation = parse("show create catalog cat1");
        assertThat(operation).isInstanceOf(ShowCreateCatalogOperation.class);
        assertThat(operation.asSummaryString()).isEqualTo("SHOW CREATE CATALOG cat1");
    }

    @Test
    void testShowFullModules() {
        final String sql = "SHOW FULL MODULES";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowModulesOperation.class);
        final ShowModulesOperation showModulesOperation = (ShowModulesOperation) operation;

        assertThat(showModulesOperation.requireFull()).isTrue();
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW FULL MODULES");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowFunctionsTest")
    void testShowFunctions(String sql, ShowFunctionsOperation expected, String expectedSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowFunctionsOperation.class).isEqualTo(expected);
        assertThat(operation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private static Stream<Arguments> inputForShowFunctionsTest() {
        return Stream.of(
                Arguments.of(
                        "show functions",
                        new ShowFunctionsOperation(
                                FunctionScope.ALL, null, "builtin", "default", null),
                        "SHOW FUNCTIONS"),
                Arguments.of(
                        "show user functions",
                        new ShowFunctionsOperation(
                                FunctionScope.USER, null, "builtin", "default", null),
                        "SHOW USER FUNCTIONS"),
                Arguments.of(
                        "show functions from cat1.db1 not like 'f%'",
                        new ShowFunctionsOperation(
                                FunctionScope.ALL,
                                "FROM",
                                "cat1",
                                "db1",
                                ShowLikeOperator.of(LikeType.NOT_LIKE, "f%")),
                        "SHOW FUNCTIONS FROM cat1.db1 NOT LIKE 'f%'"),
                Arguments.of(
                        "show user functions in cat1.db1 ilike 'f%'",
                        new ShowFunctionsOperation(
                                FunctionScope.USER,
                                "IN",
                                "cat1",
                                "db1",
                                ShowLikeOperator.of(LikeType.ILIKE, "f%")),
                        "SHOW USER FUNCTIONS IN cat1.db1 ILIKE 'f%'"),
                Arguments.of(
                        "show functions in db1",
                        new ShowFunctionsOperation(FunctionScope.ALL, "IN", "builtin", "db1", null),
                        "SHOW FUNCTIONS IN builtin.db1"));
    }

    @Test
    void testShowDatabasesFailCase() {
        assertThatThrownBy(() -> parse("show databases in db.t"))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. Show databases from/in identifier [ db.t ] format error, catalog must be a single part identifier.");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowDatabasesTest")
    void testShowDatabases(String sql, ShowDatabasesOperation expected, String expectedSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowDatabasesOperation.class).isEqualTo(expected);
        assertThat(operation.asSummaryString()).isEqualTo(expectedSummary);
    }

    private static Stream<Arguments> inputForShowDatabasesTest() {
        return Stream.of(
                Arguments.of(
                        "SHOW DATABASES",
                        new ShowDatabasesOperation("builtin", null, null),
                        "SHOW DATABASES"),
                Arguments.of(
                        "show databases from cat1 not like 'f%'",
                        new ShowDatabasesOperation(
                                "cat1", "FROM", ShowLikeOperator.of(LikeType.NOT_LIKE, "f%")),
                        "SHOW DATABASES FROM cat1 NOT LIKE 'f%'"),
                Arguments.of(
                        "show databases from cat1 not ilike 'f%'",
                        new ShowDatabasesOperation(
                                "cat1", "FROM", ShowLikeOperator.of(LikeType.NOT_ILIKE, "f%")),
                        "SHOW DATABASES FROM cat1 NOT ILIKE 'f%'"),
                Arguments.of(
                        "show databases from cat1 like 'f%'",
                        new ShowDatabasesOperation(
                                "cat1", "FROM", ShowLikeOperator.of(LikeType.LIKE, "f%")),
                        "SHOW DATABASES FROM cat1 LIKE 'f%'"),
                Arguments.of(
                        "show databases in cat1 ilike 'f%'",
                        new ShowDatabasesOperation(
                                "cat1", "IN", ShowLikeOperator.of(LikeType.ILIKE, "f%")),
                        "SHOW DATABASES IN cat1 ILIKE 'f%'"),
                Arguments.of(
                        "show databases in cat1",
                        new ShowDatabasesOperation("cat1", "IN", null),
                        "SHOW DATABASES IN cat1"));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForShowProceduresTest")
    void testShowProcedures(String sql, ShowProceduresOperation expected) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowProceduresOperation.class).isEqualTo(expected);
    }

    private static Stream<Arguments> inputForShowProceduresTest() {
        return Stream.of(
                Arguments.of(
                        "SHOW procedures from cat1.db1 not like 't%'",
                        new ShowProceduresOperation(
                                "cat1",
                                "db1",
                                "FROM",
                                ShowLikeOperator.of(LikeType.NOT_LIKE, "t%"))),
                Arguments.of(
                        "SHOW procedures from cat1.db1 ilike 't%'",
                        new ShowProceduresOperation(
                                "cat1", "db1", "FROM", ShowLikeOperator.of(LikeType.ILIKE, "t%"))),
                Arguments.of(
                        "SHOW procedures in db1",
                        new ShowProceduresOperation("builtin", "db1", "IN", null)),
                Arguments.of(
                        "SHOW procedures",
                        new ShowProceduresOperation("builtin", "default", null, null)));
    }

    @ParameterizedTest
    @MethodSource("argsForTestShowFailedCase")
    void testShowProceduresFailCase(String sql, String expectedErrorMsg) {
        // test fail case
        assertThatThrownBy(() -> parse(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedErrorMsg);
    }

    private static Stream<Arguments> argsForTestShowFailedCase() {
        return Stream.of(
                Arguments.of(
                        "SHOW procedures in cat.db.t",
                        "SHOW PROCEDURES from/in identifier [ cat.db.t ] format error,"
                                + " it should be [catalog_name.]database_name."),
                Arguments.of(
                        "SHOW Views in cat.db1.t2",
                        "SHOW VIEWS from/in identifier [ cat.db1.t2 ] format error,"
                                + " it should be [catalog_name.]database_name."),
                Arguments.of(
                        "SHOW functions in cat.db3.t5",
                        "SHOW FUNCTIONS from/in identifier [ cat.db3.t5 ] format error,"
                                + " it should be [catalog_name.]database_name."),
                Arguments.of(
                        "SHOW tables in cat1.db3.t2",
                        "SHOW TABLES from/in identifier [ cat1.db3.t2 ] format error,"
                                + " it should be [catalog_name.]database_name."));
    }

    @Test
    void testShowPartitions() {
        Operation operation = parse("show partitions tbl");
        assertThat(operation).isInstanceOf(ShowPartitionsOperation.class);
        assertThat(operation.asSummaryString()).isEqualTo("SHOW PARTITIONS builtin.default.tbl");

        operation = parse("show partitions tbl partition (dt='2020-04-30 01:02:03')");
        assertThat(operation).isInstanceOf(ShowPartitionsOperation.class);
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "SHOW PARTITIONS builtin.default.tbl PARTITION (dt=2020-04-30 01:02:03)");
    }

    @Test
    void testAddJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            AddJarOperation operation =
                                    (AddJarOperation)
                                            parser.parse(String.format("ADD JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    void testRemoveJar() {
        Arrays.asList(
                        "./test.\njar",
                        "file:///path/to/whatever",
                        "../test-jar.jar",
                        "/root/test.jar",
                        "test\\ jar.jar",
                        "oss://path/helloworld.go")
                .forEach(
                        jarPath -> {
                            RemoveJarOperation operation =
                                    (RemoveJarOperation)
                                            parser.parse(String.format("REMOVE JAR '%s'", jarPath))
                                                    .get(0);
                            assertThat(operation.getPath()).isEqualTo(jarPath);
                        });
    }

    @Test
    void testShowJars() {
        final String sql = "SHOW JARS";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(ShowJarsOperation.class);
        final ShowJarsOperation showModulesOperation = (ShowJarsOperation) operation;
        assertThat(showModulesOperation.asSummaryString()).isEqualTo("SHOW JARS");
    }

    @Test
    void testSet() {
        Operation operation1 = parse("SET");
        assertThat(operation1).isInstanceOf(SetOperation.class);
        SetOperation setOperation1 = (SetOperation) operation1;
        assertThat(setOperation1.getKey()).isNotPresent();
        assertThat(setOperation1.getValue()).isNotPresent();

        Operation operation2 = parse("SET 'test-key' = 'test-value'");
        assertThat(operation2).isInstanceOf(SetOperation.class);
        SetOperation setOperation2 = (SetOperation) operation2;
        assertThat(setOperation2.getKey()).hasValue("test-key");
        assertThat(setOperation2.getValue()).hasValue("test-value");
    }

    @Test
    void testReset() {
        Operation operation1 = parse("RESET");
        assertThat(operation1).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation1).getKey()).isNotPresent();

        Operation operation2 = parse("RESET 'test-key'");
        assertThat(operation2).isInstanceOf(ResetOperation.class);
        assertThat(((ResetOperation) operation2).getKey()).isPresent();
        assertThat(((ResetOperation) operation2).getKey()).hasValue("test-key");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SET", "SET;", "SET ;", "SET\t;", "SET\n;"})
    void testSetCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(SetOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELP", "HELP;", "HELP ;", "HELP\t;", "HELP\n;"})
    void testHelpCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(HelpOperation.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CLEAR", "CLEAR;", "CLEAR ;", "CLEAR\t;", "CLEAR\n;"})
    void testClearCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(ClearOperation.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "QUIT;", "QUIT;", "QUIT ;", "QUIT\t;", "QUIT\n;", "EXIT;", "EXIT ;", "EXIT\t;",
                "EXIT\n;", "EXIT ; "
            })
    void testQuitCommands(String command) {
        ExtendedParser extendedParser = new ExtendedParser();
        assertThat(extendedParser.parse(command)).get().isInstanceOf(QuitOperation.class);
    }
}
