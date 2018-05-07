/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package conversion

import jobs._
import dsl._
import oozie.workflow._
import protocol._
import scalaxb._
import org.specs2.mutable._

class ConversionSpec extends Specification {

    implicit class ACTION_TRANSITION_ENRICHED(at: ACTION_TRANSITION.type) {
        def apply(to: String) = ACTION_TRANSITION(Map("@to" -> DataRecord(to)))
    }

    implicit class FORK_TRANSITION_ENRICHED(at: FORK_TRANSITION.type) {
        def apply(start: String) = FORK_TRANSITION(Map("@start" -> DataRecord(start)))
    }

    implicit class START_ENRICHED(start: START.type) {
        def apply(to: String) = START(Map("@to" -> DataRecord(to)))
    }

    implicit class DEFAULT_ENRICHED(default: DEFAULT.type) {
        def apply(to: String) = DEFAULT(Map("@to" -> DataRecord(to)))
    }

    implicit class END_ENRICHED(end: END.type) {
        def apply(name: String) = END(Map("@name" -> DataRecord(name)))
    }

    "Conversion" should {

        "give empty result for empty Workflow" in {
            val wf = WORKFLOWu45APP(
                workflowu45appoption = Seq(
                    DataRecord(None, Some("kill"), KILL(
                        message = "empty" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        Map("@name" -> DataRecord("kill"))))),
                start = START("end"),
                end = END("end"),
                attributes = Map("@name" -> DataRecord("empty")))
            Conversion(EmptyWorkflow) must_== wf
        }

        "give Workflow with single node for single Workflow" in {
            val wf = WORKFLOWu45APP(
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_start")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "single" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_start"),
                end = END("end"),
                attributes = Map("@name" -> DataRecord("single")))

            Conversion(SingleWorkflow) must_== wf
        }

        "give workflow with 4 sequential jobs" in {
            val wf = WORKFLOWu45APP(
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_third"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_third")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_fourth"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_fourth")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"),
                attributes = Map("@name" -> DataRecord("simple")))

            Conversion(SimpleWorkflow) must_== wf
        }

        "give workflow with 2 jobs running in parallel" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("simple-fork-join")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_secondA"),
                            FORK_TRANSITION("mr_secondB")),
                        attributes = Map("@name" -> DataRecord("fork-mr_secondA-mr_secondB")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_secondA")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_secondB")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        Map("@name" -> DataRecord("join-mr_secondA-mr_secondB"), "@to" -> DataRecord("end")))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple-fork-join" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SimpleForkJoin) must_== wf
        }

        "give workflow with 1 job, decision, then two more jobs" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("simple-decision")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_default-mr_option"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "true",
                                        attributes = Map("@to" -> DataRecord("mr_option")))),
                                default = DEFAULT(
                                    attributes = Map("@to" -> DataRecord("mr_default"))))),
                        attributes = Map("@name" -> DataRecord("decision-mr_default-mr_option")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_default")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_option")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple-decision" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SimpleDecision) must_== wf
        }

        "give workflow with 1 job, then sub workflow, then 1 more job" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("simple-sub-workflow")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_begin")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_first"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_third"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_third")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_fourth"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_fourth")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_final"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_final")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple-sub-workflow" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_begin"),
                end = END("end"))

            Conversion(SimpleSubWorkflow) must_== wf
        }

        "give workflow with two separate fork / joins" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("two-simple-fork-joins")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_secondA"),
                            FORK_TRANSITION("mr_secondB")),
                        attributes = Map("@name" -> DataRecord("fork-mr_secondA-mr_secondB")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_secondA")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_secondB")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(attributes = Map(
                        "@name" -> DataRecord("join-mr_secondA-mr_secondB"),
                        "@to" -> DataRecord("mr_third")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_third")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_fourthA-mr_fourthB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_fourthA"),
                            FORK_TRANSITION("mr_fourthB")),
                        attributes = Map("@name" -> DataRecord("fork-mr_fourthA-mr_fourthB")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_fourthA")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourthA-mr_fourthB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_fourthB")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourthA-mr_fourthB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(attributes = Map(
                        "@name" -> DataRecord("join-mr_fourthA-mr_fourthB"),
                        "@to" -> DataRecord("end")))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "two-simple-fork-joins" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(TwoSimpleForkJoins) must_== wf
        }

        "give workflow with nested fork / joins" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("nested-fork-join")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_secondA"),
                            FORK_TRANSITION("mr_secondB")),
                        attributes = Map("@name" -> DataRecord("fork-mr_secondA-mr_secondB")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_secondA")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_secondB")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_thirdC"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_thirdA"),
                            FORK_TRANSITION("mr_thirdB")),
                        attributes = Map("@name" -> DataRecord("fork-mr_thirdA-mr_thirdB")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_thirdC")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourth-mr_thirdC"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_thirdA")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_thirdB")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(attributes = Map(
                        "@name" -> DataRecord("join-mr_thirdA-mr_thirdB"),
                        "@to" -> DataRecord("mr_fourth")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_fourth")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourth-mr_thirdC"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(attributes = Map(
                        "@name" -> DataRecord("join-mr_fourth-mr_thirdC"),
                        "@to" -> DataRecord("end")))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "nested-fork-join" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(NestedForkJoin) must_== wf
        }

        "give workflow with subworkflow, including fork joins" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("sub-fork-join")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_start")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_first"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_third"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_third")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_fourth"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_fourth")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_thirdA"),
                            FORK_TRANSITION("mr_thirdB")),
                        attributes = Map("@name" -> DataRecord("fork-mr_thirdA-mr_thirdB")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_thirdA")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_thirdB")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(attributes = Map(
                        "@name" -> DataRecord("join-mr_thirdA-mr_thirdB"),
                        "@to" -> DataRecord("end")))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "sub-fork-join" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_start"),
                end = END("end"))

            Conversion(SubworkflowWithForkJoins) must_== wf
        }

        "give workflow with duplicate nodes" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("duplicate-nodes")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_second-mr_second2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_second"),
                            FORK_TRANSITION("mr_second2")),
                        attributes = Map("@name" -> DataRecord("fork-mr_second-mr_second2")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_second-mr_second2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second2")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_second-mr_second2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(attributes = Map(
                        "@name" -> DataRecord("join-mr_second-mr_second2"),
                        "@to" -> DataRecord("end")))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "duplicate-nodes" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(DuplicateNodes) must_== wf
        }

        "give workflow with syntactically sugared decision" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("sugar-option-decision")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_second-mr_option"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doOption}",
                                        attributes = Map("@to" -> DataRecord("mr_option")))),
                                default = DEFAULT("mr_second"))),
                        attributes = Map("@name" -> DataRecord("decision-mr_second-mr_option")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_option")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "sugar-option-decision" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SugarOption) must_== wf
        }

        "give workflow with regular decision and syntactically sugared decision" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("mixed-decision-styles")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_default2-decision-mr_default-mr_---"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "true",
                                        attributes = Map("@to" -> DataRecord("decision-mr_default-mr_option")))),
                                default = DEFAULT("mr_default2"))),
                        attributes = Map("@name" -> DataRecord("decision-mr_default2-decision-mr_default-mr_---")))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doOption}",
                                        attributes = Map("@to" -> DataRecord("mr_option")))),
                                default = DEFAULT("mr_default"))),
                        attributes = Map("@name" -> DataRecord("decision-mr_default-mr_option")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_option")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_default"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_default")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_default2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_default2")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "mixed-decision-styles" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(DecisionAndSugarOption) must_== wf
        }

        "give workflow with custom error-to message" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("custom-errorTo")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_first")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("mr_errorOption"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_errorOption")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_second")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "custom-errorTo" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(CustomErrorTo) must_== wf
        }

        "give workflow with sub-workflow ending in sugar decision option" in {
            val wf = WORKFLOWu45APP(
                attributes = Map("@name" -> DataRecord("sub-wf-ending-with-sugar-option")),
                workflowu45appoption = Seq(
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doOption}",
                                        attributes = Map("@to" -> DataRecord("mr_option")))),
                                default = DEFAULT("mr_default"))),
                        attributes = Map("@name" -> DataRecord("decision-mr_default-mr_option")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_default")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_last-mr_sugarOption"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_option")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_last"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doSugarOption}",
                                        attributes = Map("@to" -> DataRecord("mr_sugarOption")))),
                                default = DEFAULT("mr_last"))),
                        attributes = Map("@name" -> DataRecord("decision-mr_last-mr_sugarOption")))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_sugarOption")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_last"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        attributes = Map("@name" -> DataRecord("mr_last")),
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "sub-wf-ending-with-sugar-option" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        attributes = Map("@name" -> DataRecord("kill"))))),
                start = START("decision-mr_default-mr_option"),
                end = END("end"))
            Conversion(SubWfEndWithSugarOption) must_== wf
        }

    }

    def EmptyWorkflow = {
        val end = End dependsOn Nil
        Workflow("empty", end)
    }

    def SingleWorkflow = {
        val start = MapReduceJob("start") dependsOn Start
        val end = End dependsOn start
        Workflow("single", end)
    }

    def SimpleWorkflow = {
        val first = MapReduceJob("first") dependsOn Start
        val second = MapReduceJob("second") dependsOn first
        val third = MapReduceJob("third") dependsOn second
        val fourth = MapReduceJob("fourth") dependsOn third
        val end = End dependsOn fourth
        Workflow("simple", end)
    }

    def SimpleForkJoin = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val end = End dependsOn (secondA, secondB)
        Workflow("simple-fork-join", end)
    }

    def SimpleDecision = {
        val first = MapReduceJob("first") dependsOn Start
        val decision = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first //decision is a DecisionNode
        val default = MapReduceJob("default") dependsOn (decision default)
        val option = MapReduceJob("option") dependsOn (decision option "route1")
        val second = MapReduceJob("second") dependsOn OneOf(default, option)
        val done = End dependsOn second
        Workflow("simple-decision", done)
    }

    def SimpleSubWorkflow = {
        val first = MapReduceJob("begin") dependsOn Start
        val subWf = SimpleWorkflow dependsOn first
        val third = MapReduceJob("final") dependsOn subWf
        val end = End dependsOn third
        Workflow("simple-sub-workflow", end)
    }

    def TwoSimpleForkJoins = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val third = MapReduceJob("third") dependsOn (secondA, secondB)
        val fourthA = MapReduceJob("fourthA") dependsOn third
        val fourthB = MapReduceJob("fourthB") dependsOn third
        val end = End dependsOn (fourthA, fourthB)
        Workflow("two-simple-fork-joins", end)
    }

    def NestedForkJoin = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val thirdA = MapReduceJob("thirdA") dependsOn secondA
        val thirdB = MapReduceJob("thirdB") dependsOn secondA
        val thirdC = MapReduceJob("thirdC") dependsOn secondB
        val fourth = MapReduceJob("fourth") dependsOn (thirdA, thirdB)
        val end = End dependsOn (fourth, thirdC)
        Workflow("nested-fork-join", end)
    }

    def SubworkflowWithForkJoins = {
        val start = MapReduceJob("start") dependsOn Start
        val sub = SimpleWorkflow dependsOn start
        val thirdA = MapReduceJob("thirdA") dependsOn sub
        val thirdB = MapReduceJob("thirdB") dependsOn sub
        val end = End dependsOn (thirdA, thirdB)
        Workflow("sub-fork-join", end)
    }

    def DuplicateNodes = {
        val first = MapReduceJob("first") dependsOn Start
        val second = MapReduceJob("second") dependsOn first
        val third = MapReduceJob("second") dependsOn first
        val end = End dependsOn (second, third)
        Workflow("duplicate-nodes", end)
    }

    def SugarOption = {
        val first = MapReduceJob("first") dependsOn Start
        val option = MapReduceJob("option") dependsOn first doIf "${doOption}"
        val second = MapReduceJob("second") dependsOn Optional(option)
        val done = End dependsOn second
        Workflow("sugar-option-decision", done)
    }

    def DecisionAndSugarOption = {
        val first = MapReduceJob("first") dependsOn Start
        val decision = Decision(
            "route1" -> Predicates.AlwaysTrue) dependsOn first
        val option = MapReduceJob("option") dependsOn (decision option "route1") doIf "${doOption}"
        val default = MapReduceJob("default") dependsOn Optional(option)
        val default2 = MapReduceJob("default2") dependsOn OneOf(decision default, default)
        val end = End dependsOn default2
        Workflow("mixed-decision-styles", end)
    }

    def CustomErrorTo = {
        val first = MapReduceJob("first") dependsOn Start
        val errorOption = MapReduceJob("errorOption") dependsOn (first error)
        val second = MapReduceJob("second") dependsOn first
        val end = End dependsOn OneOf(second, errorOption)
        Workflow("custom-errorTo", end)
    }

    def SubWfEndWithSugarOption = {

        val wf = WfEndWithSugarOption dependsOn Start
        val last = MapReduceJob("last") dependsOn wf
        val end = End dependsOn last
        Workflow("sub-wf-ending-with-sugar-option", end)
    }

    def WfEndWithSugarOption = {

        val decision = Decision(
            "option" -> Predicates.BooleanProperty("doOption")) dependsOn Start
        val option = MapReduceJob("option") dependsOn (decision option "option")
        val default = MapReduceJob("default") dependsOn (decision default)
        val sugarOption = MapReduceJob("sugarOption") dependsOn default doIf "doSugarOption"
        val end = End dependsOn OneOf(option, Optional(sugarOption))
        Workflow("test-agg-content", end)
    }

}