package com.klout.scoozie
package jobs

import com.klout.scoozie.dsl.Job

case class HiveJob(
    fileName:      String,
    configuration: ArgList             = Nil,
    parameters:    List[String]        = List.empty,
    prepare:       List[FsTask]        = List.empty,
    jobXml:        Option[Seq[String]] = None,
    otherFiles:    Option[Seq[String]] = None) extends Job {
    val dotIndex: Int = fileName.indexOf(".")
    val cleanName: String = {
        if (dotIndex > 0)
            fileName.substring(0, dotIndex)
        else
            fileName
    }
    override val jobName = s"hive_$cleanName"
}
