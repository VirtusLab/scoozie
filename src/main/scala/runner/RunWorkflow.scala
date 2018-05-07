/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package runner

import java.util.Date

import com.klout.scoozie.conversion._
import com.klout.scoozie.dsl._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataOutputStream, FileSystem, Path }
import org.apache.oozie.client.{ OozieClient, WorkflowAction, WorkflowJob }
import scalaxb._
import oozie.workflow._
import protocol._
import conversion.Configuration._

object RunWorkflow {
    val SleepInterval = 5000

    def sequence[T](workflowMap: Map[T, AsyncOozieWorkflow]): Map[T, Either[OozieError, OozieSuccess]] = {
        val workflows = workflowMap map (_._2)

        while (workflows exists (_.isRunning)) {
            println("Workflow job running ...")
            workflows flatMap (_.actions) filter (_.getStatus == WorkflowAction.Status.RUNNING) foreach (action => {
                val now = new Date
                println(now + " " + action)
            })
            Thread.sleep(SleepInterval)
        }
        // print the final status to the workflow job
        println("Workflow jobs completed ...")
        workflows foreach (a => println(a.jobInfo()))
        workflowMap mapValues (_.successOrFail())
    }

    def async(workflow: Workflow, appPath: String, config: OozieConfig, postprocessing: Option[XmlPostProcessing]): AsyncOozieWorkflow = {
        prepWorkflow(workflow, appPath, config.properties, postprocessing)
        getOozieWorkflow(appPath, config)
    }

    def apply(workflow: Workflow, appPath: String, config: OozieConfig, postprocessing: Option[XmlPostProcessing]): Either[OozieError, OozieSuccess] = {
        prepWorkflow(workflow, appPath, config.properties, postprocessing)
        execWorkflow(appPath, config)
    }

    def prepWorkflow(workflow: Workflow, appPathString: String, properties: Map[String, String], postprocessing: Option[XmlPostProcessing]) = {
        val xmlString = getXMLString(workflow, postprocessing)
        val resolvedAppPath = resolveProperty(appPathString, properties)
        val appPath: Path = new Path(resolvedAppPath)
        //write xml file to hdfs
        val conf = new Configuration()
        conf.set("fs.defaultFs", properties.get("nameNode") match {
            case Some(prop) => prop
            case _          => throw new RuntimeException("error: no name node set")
        })
        val fs = FileSystem.get(conf)
        println("About to create path: " + appPath)
        writeFile(fs, appPath, xmlString)
    }

    def writeFile(fs: FileSystem, appPath: Path, data: String) = Helpers.retryable {
        () =>
            {
                if (fs.exists(appPath)) {
                    println("It exists, so deleting it first.")
                    fs.delete(appPath, false)
                }
                val out: FSDataOutputStream = fs.create(appPath)
                out.write(data.getBytes("UTF-8"))
                out.close
            }
    }

    /*
     * Resolves all ${foo} values in input property
     */
    def resolveProperty(prop: String, properties: Map[String, String]): String = {
        var newProp = prop
        while (newProp.contains("${")) {
            val subVar = newProp.substring(newProp.indexOf("${"), newProp.indexOf("}") + 1)
            val subVarName = subVar.substring(2, subVar.length - 1)
            val subVar_value = properties.get(subVarName) match {
                case Some(value) => value
                case _           => throw new RuntimeException("error: missing property value " + subVarName)
            }
            newProp = newProp.replace(subVar, subVar_value)
        }
        newProp
    }

    /*
     * Executes the workflow, retrying if necessary
     */
    def getOozieWorkflow(appPath: String, config: OozieConfig): AsyncOozieWorkflow = {
        val oc = RetryableOozieClient(new OozieClient(config.oozieUrl))
        // create a workflow job configuration and set the workflow application path
        val conf = oc.createConfiguration()
        //set workflow parameters
        config.properties foreach (pair => conf.setProperty(pair._1, pair._2))
        conf.setProperty(OozieClient.APP_PATH, appPath)
        //submit and start the workflow job
        val jobId: String = oc.run(conf);
        val wfJob: WorkflowJob = oc.getJobInfo(jobId)
        val consoleUrl: String = wfJob.getConsoleUrl
        println(s"Workflow job $jobId submitted and running")
        println("Workflow: " + wfJob.getAppName + " at " + wfJob.getAppPath)
        println("Console URL: " + consoleUrl)
        AsyncOozieWorkflow(oc, jobId, consoleUrl)
    }

    def execWorkflow(appPath: String, config: OozieConfig): Either[OozieError, OozieSuccess] = {

        val async = getOozieWorkflow(appPath, config)

        sequence(Map("blah" -> async)).map(_._2).toList.headOption match {
            case Some(result) => result
            case _            => async.successOrFail()
        }
    }

    def getXMLString(workflow: Workflow, postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)): String = {
        val defaultScope = scalaxb.toScope(None -> xmlWorkflowNamespace)
        val wf = Conversion(workflow)
        val wfXml = toXML[WORKFLOWu45APP](wf, Some("workflow"), "workflow-app", defaultScope)
        val prettyPrinter = new scala.xml.PrettyPrinter(Int.MaxValue, 4)
        val formattedXml = prettyPrinter.formatNodes(wfXml)
        val processedXml = postprocessing match {
            case Some(proccessingRules) => (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace (mapping._1, mapping._2))
            case _                      => formattedXml
        }
        processedXml
    }

}
