package com.virtuslab.scoozie
package jobs

import com.virtuslab.scoozie.dsl.Job

case class NoOpJob(name: String) extends Job {
    override val jobName: String = name
}
