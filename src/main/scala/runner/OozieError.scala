package com.klout.scoozie
package runner

case class OozieError(jobId: String, jobLog: String, consoleUrl: String)
