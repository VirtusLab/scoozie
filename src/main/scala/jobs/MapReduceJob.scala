package com.klout.scoozie
package jobs

import com.klout.scoozie.dsl.Job

case class MapReduceJob(name: String, prepare: List[FsTask] = List.empty, configuration: ArgList = Nil) extends Job {
    override val jobName = s"mr_$name"
}
