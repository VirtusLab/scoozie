package org.virtuslab.scoozie
package dsl

case class SugarNode(work: Work, dependency: DecisionDependency, previousSugarNode: Option[SugarNode] = None)
