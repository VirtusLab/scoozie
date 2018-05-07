package org.virtuslab.scoozie
package dsl

sealed trait Dependency

case class ForkDependency(name: String) extends Dependency

case class JoinDependency(name: String) extends Dependency

case class Node(work: Work, dependencies: List[_ <: Dependency]) extends Dependency {

    def doIf(predicate: String): SugarNode = {
        //make sure predicate string is in ${foo} format
        val Pattern = """[${].*[}]""".r
        val formattedPredicate = predicate match {
            case Pattern() => predicate
            case _         => "${" + predicate + "}"
        }
        val decision = Decision(formattedPredicate -> Predicates.BooleanProperty(formattedPredicate)) dependsOn dependencies
        SugarNode(work, decision option formattedPredicate)
    }

    def error = ErrorTo(this) //used to set a custom error-to on a node
}

case object Start extends Dependency

case class OneOf(dep1: Dependency, deps: Dependency*) extends Dependency

case class DecisionDependency(parent: DecisionNode, option: Option[String]) extends Dependency

case class DecisionNode(decision: Decision, dependencies: Set[_ <: Dependency]) extends Dependency {
    val default: Dependency = DecisionDependency(this, None)
    val option: String => DecisionDependency = name => DecisionDependency(this, Some(name))

}

case class DoIf(predicate: String, deps: Dependency*) extends Dependency

case class ErrorTo(node: Node) extends Dependency
