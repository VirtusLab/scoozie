package org.virtuslab.scoozie
package conversion

import org.virtuslab.scoozie.dsl.{ DecisionNode, Job }

sealed trait WorkflowOption

case class WorkflowJob(job: Job) extends WorkflowOption

case class WorkflowDecision(predicates: List[(String, dsl.Predicate)], decisionNode: DecisionNode) extends WorkflowOption

case object WorkflowFork extends WorkflowOption

case object WorkflowJoin extends WorkflowOption

case object WorkflowEnd extends WorkflowOption
