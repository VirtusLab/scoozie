package org.virtuslab.scoozie
package jobs

import org.virtuslab.scoozie.dsl.Job

case class MapReduceJob(name: String, prepare: List[FsTask] = List.empty, configuration: ArgList = Nil, configClass: Option[String] = None) extends Job {
    override val jobName = s"mr_$name"
}
