package com.virtuslab.scoozie
package runner

import java.util.Properties

import org.apache.oozie.client.{ OozieClient, WorkflowJob }

case class RetryableOozieClient(client: OozieClient) {

    def run(conf: Properties): String = Helpers.retryable {
        () => client.run(conf)
    }

    def createConfiguration(): Properties = Helpers.retryable {
        () => client.createConfiguration()
    }

    def getJobInfo(jobId: String): WorkflowJob = Helpers.retryable {
        () => client.getJobInfo(jobId)
    }

    def getJobLog(jobId: String): String = Helpers.retryable {
        () => client.getJobLog(jobId)
    }

}
