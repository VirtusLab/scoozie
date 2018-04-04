package com.klout.scoozie
package dsl

object Optional {
    def toNode(sugarNode: SugarNode): Node = sugarNode.previousSugarNode match {
        case Some(previous) => Node(sugarNode.work, List(toNode(previous)))
        case _              => Node(sugarNode.work, List(sugarNode.dependency))
    }

    def apply(sugarNode: SugarNode) = OneOf(sugarNode.dependency.parent default, toNode(sugarNode))
}
