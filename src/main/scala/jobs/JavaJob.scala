package com.klout.scoozie
package jobs

import com.klout.scoozie.dsl.Job

case class JavaJob(mainClass: String, prepare: List[FsTask] = List.empty, configuration: ArgList = Nil, jvmOps: Option[String] = None, args: List[String] = Nil) extends Job {
    val domain: String = mainClass.substring(mainClass.lastIndexOf(".") + 1)
    override val jobName = s"java_$domain"
}
