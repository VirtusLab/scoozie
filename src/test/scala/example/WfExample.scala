/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */
package com.virtuslab.scoozie
package example

import scalaxb._
import oozie.workflow._
import protocol._

/*
 * Example from the Oozie Website:
 * "The following workflow definition example executes 4 Map-Reduce jobs in 3 steps -
 * 1 job, 2 jobs in parallel and 1 job."
 * (http://oozie.apache.org/docs/3.3.2/WorkflowFunctionalSpec.html#a3.2.6_Sub-workflow_Action)
 * translated to scoozie.
 */
object WfExample {
    val nodes: Seq[DataRecord[WORKFLOWu45APPOption]] = Seq(
        DataRecord(None, Some("action"), ACTION(
            actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property("mapred.map.tasks", "1"),
                    Property("mapred.input.dir", "${input}"),
                    Property("mapred.output.dir", "/usr/foo/${wf:id()}/temp1")))))),
            ok = ACTION_TRANSITION(Map("@to" -> DataRecord("fork"))),
            error = ACTION_TRANSITION(Map("@to" -> DataRecord("kill"))),
            attributes = Map("@name" -> DataRecord("firstjob")))),
        DataRecord(None, Some("fork"), FORK(
            path = Seq(
                FORK_TRANSITION(Map("@start" -> DataRecord("secondjob"))),
                FORK_TRANSITION(Map("@start" -> DataRecord("thirdjob")))),
            attributes = Map("@name" -> DataRecord("fork")))),
        DataRecord(None, Some("action"), ACTION(
            actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property("mapred.map.tasks", "1"),
                    Property("mapred.input.dir", "/usr/foo/${wf:id()}/temp1"),
                    Property("mapred.output.dir", "/usr/foo/${wf:id()}/temp2")))))),
            ok = ACTION_TRANSITION(Map("@to" -> DataRecord("join"))),
            error = ACTION_TRANSITION(Map("@to" -> DataRecord("kill"))),
            attributes = Map("@name" -> DataRecord("secondjob")))),
        DataRecord(None, Some("action"), ACTION(
            actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property("mapred.map.tasks", "1"),
                    Property("mapred.input.dir", "/usr/foo/${wf:id()}/temp1"),
                    Property("mapred.output.dir", "/usr/foo/${wf:id()}/temp3")))))),
            ok = ACTION_TRANSITION(Map("@to" -> DataRecord("join"))),
            error = ACTION_TRANSITION(Map("@to" -> DataRecord("kill"))),
            attributes = Map("@name" -> DataRecord("secondjob")))),
        DataRecord(None, Some("join"), JOIN(Map("@name" -> DataRecord("join"), "@to" -> DataRecord("finaljob")))),
        DataRecord(None, Some("action"), ACTION(
            actionoption = DataRecord(None, Some("Map-Reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property("mapred.map.tasks", "1"),
                    Property("mapred.input.dir", "/usr/foo/${wf:id()}/temp2,usr/foo/${wf:id()}/temp3"),
                    Property("mapred.output.dir", "${output}")))))),
            ok = ACTION_TRANSITION(Map("@to" -> DataRecord("end"))),
            error = ACTION_TRANSITION(Map("@to" -> DataRecord("kill"))))),
        DataRecord(None, Some("kill"), KILL(
            message = "Map/Reduce failed, error message[${wf:errorMessage()}]",
            attributes = Map("@name" -> DataRecord("kill")))))

    val wfApp = WORKFLOWu45APP(
        workflowu45appoption = nodes,
        start = START(Map("@to" -> DataRecord("firstjob"))),
        end = END(Map("@name" -> DataRecord("end"))),
        attributes = Map("@name" -> DataRecord("example-forkjoinwf")))
}
