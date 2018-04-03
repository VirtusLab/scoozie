package com.klout.scoozie
package jobs

import com.klout.scoozie.dsl.Job

case class NoOpJob(name: String) extends Job {
    override val jobName: String = name
}
