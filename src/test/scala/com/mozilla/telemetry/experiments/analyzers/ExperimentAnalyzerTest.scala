/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.analyzers

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.Map


case class ExperimentDataset(experiment_id: String, experiment_branch: String, client_id: String)

class ExperimentAnalyzerTest extends FlatSpec with Matchers with DatasetSuiteBase {
  def fixture: DataFrame = {
    import spark.implicits._
    Seq(
      ExperimentDataset("experiment1", "control", "a"),
      ExperimentDataset("experiment1", "control", "b"),
      ExperimentDataset("experiment1", "control", "a"),
      ExperimentDataset("experiment1", "control", "c"),
      ExperimentDataset("experiment1", "branch1", "d"),
      ExperimentDataset("experiment1", "branch2", "e"),
      ExperimentDataset("experiment1", "branch2", "d")
    ).toDS().toDF()
  }

  "Pings and clients" can "be counted" in {
    val df = fixture
    val actual = ExperimentAnalyzer.getExperimentMetadata(df).collect.toSet
    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 4, "Experiment Metadata", "Metadata",
        Map.empty[Long, HistogramPoint],
        Some(Seq(Statistic(None, "Total Pings", 4.0), Statistic(None, "Total Clients", 3.0)))
      ),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1, "Experiment Metadata", "Metadata",
        Map.empty[Long, HistogramPoint],
        Some(Seq(Statistic(None, "Total Pings", 1.0), Statistic(None, "Total Clients", 1.0)))
      ),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2, "Experiment Metadata", "Metadata",
        Map.empty[Long, HistogramPoint],
        Some(Seq(Statistic(None, "Total Pings", 2.0), Statistic(None, "Total Clients", 2.0)))
      )
    )
    assert(actual == expected)
  }
}
