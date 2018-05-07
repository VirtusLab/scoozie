package org.virtuslab.scoozie
package runner

case class OozieError(jobId: String, jobLog: String, consoleUrl: String)
