package com.virtuslab.scoozie
package jobs

import com.virtuslab.scoozie.dsl.Job

case class JavaJob(mainClass: String, prepare: List[FsTask] = List.empty, configuration: ArgList = Nil, jvmOps: Option[String] = None, jvmOp: Seq[String] = Nil, args: List[String] = Nil) extends Job {
    val domain: String = mainClass.substring(mainClass.lastIndexOf(".") + 1)
    override val jobName = s"java_$domain"
}
