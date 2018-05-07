package com.virtuslab.scoozie
package dsl

sealed trait Work {
    def dependsOn(dep1: Dependency, deps: Dependency*): Node = Node(this, List(dep1) ++ deps)
    def dependsOn(deps: Seq[Dependency]): Node = Node(this, deps.toList)
    def dependsOn(sugarNode: SugarNode): SugarNode = SugarNode(this, sugarNode.dependency, Some(sugarNode))
}

case object End extends Work

trait Job extends Work {
    val jobName: String
}

case class Workflow(name: String, end: Node) extends Work

case class Kill(name: String) extends Work
