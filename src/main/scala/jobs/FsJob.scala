package org.virtuslab.scoozie
package jobs

import org.virtuslab.scoozie.dsl.Job

// Node: There is a limitation with the way scalaxb creates the FS Task
// case classes from workflow.xsd: It treats the different task types as
// separate sequences so ordering among the types is not possible.
// Need to address later.
case class FsJob(name: String, tasks: List[FsTask]) extends Job {
    override val jobName = s"fs_$name"
}
