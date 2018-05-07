package com.virtuslab.scoozie
package runner

import org.apache.oozie.client.{ WorkflowAction, WorkflowJob }
import scala.collection.JavaConverters._

case class AsyncOozieWorkflow(oc: RetryableOozieClient, jobId: String, consoleUrl: String) {
    def jobInfo(): WorkflowJob = oc.getJobInfo(jobId)

    def jobLog(): String = oc.getJobLog(jobId)

    def isRunning(): Boolean = jobInfo.getStatus() == WorkflowJob.Status.RUNNING

    def isSuccess(): Boolean = jobInfo.getStatus() == WorkflowJob.Status.SUCCEEDED

    def actions(): List[WorkflowAction] = jobInfo().getActions.asScala.toList

    def successOrFail(): Either[OozieError, OozieSuccess] = {
        if (!this.isSuccess()) {
            Left(OozieError(jobId, jobLog(), consoleUrl))
        } else {
            Right(OozieSuccess(jobId))
        }
    }
}
