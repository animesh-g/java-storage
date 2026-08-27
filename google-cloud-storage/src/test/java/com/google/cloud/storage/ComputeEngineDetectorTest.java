/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.storage;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ComputeEngineDetectorTest {

  @Before
  @After
  public void setUpAndTearDown() {
    ComputeEngineDetector.resetForTesting();
  }

  @Test
  public void matchEngineFromClassName_identifiesSpark() {
    assertThat(
            ComputeEngineDetector.matchEngineFromClassName(
                "org.apache.spark.sql.execution.datasources.FileScanRDD"))
        .isEqualTo(ComputeEngineDetector.Engine.SPARK);

    assertThat(
            ComputeEngineDetector.matchEngineFromClassName(
                "org.apache.spark.deploy.SparkSubmit"))
        .isEqualTo(ComputeEngineDetector.Engine.SPARK);
  }

  @Test
  public void matchEngineFromClassName_identifiesTrino() {
    assertThat(
            ComputeEngineDetector.matchEngineFromClassName(
                "io.trino.filesystem.gcs.GcsFileSystem"))
        .isEqualTo(ComputeEngineDetector.Engine.TRINO);

    assertThat(
            ComputeEngineDetector.matchEngineFromClassName(
                "io.trino.execution.QueryTracker"))
        .isEqualTo(ComputeEngineDetector.Engine.TRINO);
  }

  @Test
  public void matchEngineFromClassName_identifiesPresto() {
    assertThat(
            ComputeEngineDetector.matchEngineFromClassName(
                "com.facebook.presto.hive.gcs.GcsConfigurationProvider"))
        .isEqualTo(ComputeEngineDetector.Engine.PRESTO);
  }

  @Test
  public void matchEngineFromClassName_ignoresUnrelatedClasses() {
    assertThat(ComputeEngineDetector.matchEngineFromClassName("com.google.cloud.storage.Storage"))
        .isNull();
    assertThat(ComputeEngineDetector.matchEngineFromClassName("java.lang.Thread")).isNull();
    assertThat(ComputeEngineDetector.matchEngineFromClassName("org.apache.hadoop.fs.FileSystem"))
        .isNull();
  }

  @Test
  public void findMatchingEngine_findsFirstMatchingFrame() {
    StackTraceElement[] frames =
        new StackTraceElement[] {
          new StackTraceElement(
              "com.google.cloud.storage.ComputeEngineDetector",
              "detectEngineFromStackTrace",
              "ComputeEngineDetector.java",
              85),
          new StackTraceElement(
              "org.apache.hadoop.fs.FileSystem", "get", "FileSystem.java", 100),
          new StackTraceElement(
              "org.apache.spark.sql.execution.datasources.FileScanRDD",
              "compute",
              "FileScanRDD.scala",
              120),
          new StackTraceElement(
              "io.trino.execution.QueryTracker", "run", "QueryTracker.java", 50)
        };

    assertThat(ComputeEngineDetector.findMatchingEngine(frames))
        .isEqualTo(ComputeEngineDetector.Engine.SPARK);
  }

  @Test
  public void findMatchingEngineFromClassNames_returnsNullWhenNoMatch() {
    List<String> frames =
        Arrays.asList(
            "java.lang.Thread",
            "org.apache.hadoop.fs.FileSystem",
            "com.google.cloud.storage.Storage");

    assertThat(ComputeEngineDetector.findMatchingEngineFromClassNames(frames)).isNull();
  }

  @Test
  public void detectEngineSuffix_returnsEmptyInStandardTestRunner() {
    // In standard JUnit, stack trace does not contain Spark or Trino
    assertThat(ComputeEngineDetector.detectEngineSuffix()).isEqualTo("");
  }

  @Test
  public void storageOptions_getApplicationNameReturnsExpected() {
    StorageOptions options = StorageOptions.http().build();
    // In test runner, suffix is empty, so app name is default gcloud-java/...
    assertThat(options.getApplicationName()).startsWith("gcloud-java");
  }
}
