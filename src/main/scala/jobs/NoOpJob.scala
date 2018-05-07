package org.virtuslab.scoozie
package jobs

import org.virtuslab.scoozie.dsl.Job

case class NoOpJob(name: String) extends Job {
    override val jobName: String = name
}
