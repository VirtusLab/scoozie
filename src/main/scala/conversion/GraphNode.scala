package org.virtuslab.scoozie
package conversion

import com.google.common.base._
import org.virtuslab.scoozie.dsl.DecisionNode
import scalaxb._
import oozie.workflow._
import protocol._

case class GraphNode(
    var name:           String,
    var workflowOption: WorkflowOption,
    var before:         RefSet[GraphNode],
    var after:          RefSet[GraphNode],
    var decisionBefore: RefSet[GraphNode]           = RefSet(),
    var decisionAfter:  RefSet[GraphNode]           = RefSet(),
    var decisionRoutes: Set[(String, DecisionNode)] = Set.empty,
    var errorTo:        Option[GraphNode]           = None) {

    def getName(n: GraphNode) = n.name

    def beforeNames = before map (getName(_))
    def afterNames = after map (getName(_))
    def decisionBeforeNames = decisionBefore map (getName(_))
    def decisionAfterNames = decisionAfter map (getName(_))

    override def toString =
        s"GraphNode: name=[$name], option=[$workflowOption], before=[$beforeNames], after=[$afterNames], decisionBefore=[$decisionBeforeNames], decisionAfter=[$decisionAfterNames], decisionRoute=[$decisionRoutes], errorTo=[$errorTo]"

    override def equals(any: Any): Boolean = {
        any match {
            case node: GraphNode =>
                this.name == node.name &&
                    this.workflowOption == node.workflowOption &&
                    this.beforeNames == node.beforeNames &&
                    this.afterNames == node.afterNames &&
                    this.decisionRoutes == node.decisionRoutes &&
                    this.errorTo == node.errorTo &&
                    this.decisionAfterNames == node.decisionAfterNames &&
                    this.decisionBeforeNames == node.decisionBeforeNames
            case _ => false
        }
    }

    override def hashCode: Int = {
        Objects.hashCode(name, workflowOption, beforeNames, afterNames, decisionBeforeNames, decisionAfterNames, decisionRoutes, errorTo)
    }

    /*
     * Checks whether this GraphNode has the desired decisionRoute
     */
    def containsDecisionRoute(predicateRoute: String, decisionNode: DecisionNode): Boolean = {
        decisionRoutes contains (predicateRoute -> decisionNode)
    }

    /*
     * Gets route node name for specified predicate route - Only applies for
     * Decisions
     */
    private def nameRoutes(predicateRoutes: List[String]): String = {
        this.workflowOption match {
            case WorkflowDecision(predicates, decisionNode) =>
                predicateRoutes map { predicateRoute =>
                    decisionAfter.find(_.containsDecisionRoute(predicateRoute, decisionNode)) match {
                        case Some(routeNode) => routeNode.name
                        case _               => "kill"
                    }
                } mkString "-"
            case _ => throw new RuntimeException("error: getting route from non-decision node")
        }
    }

    def getDecisionRouteName(predicateRoute: String): String = {
        nameRoutes(List(predicateRoute))
    }

    def getDecisionName(predicateRoutes: List[String]): String = {
        nameRoutes("default" :: predicateRoutes)
    }

    lazy val toXmlWorkflowOption: Set[DataRecord[WORKFLOWu45APPOption]] = {
        if (after.size > 1) {
            workflowOption match {
                //after will be of size > 1 for forks
                case WorkflowFork =>
                case _ =>
                    throw new RuntimeException("error: nodes should only be singly linked " + afterNames)
            }
        }
        val okTransition = afterNames.headOption.getOrElse(
            decisionAfterNames.headOption.getOrElse("end"))
        workflowOption match {
            case WorkflowFork =>
                Set(DataRecord(None, Some("fork"), FORK(
                    path = afterNames.toSeq.map(start => FORK_TRANSITION.apply(Map("@start" -> DataRecord(start)))).toList,
                    attributes = Map("@name" -> DataRecord(name)))))
            case WorkflowJoin =>
                Set(DataRecord(None, Some("join"), JOIN(attributes = Map(
                    "@name" -> DataRecord(name),
                    "@to" -> DataRecord(okTransition)))))
            case WorkflowJob(job) =>
                val to = errorTo match {
                    case Some(node) => node.name
                    case _          => "kill"
                }
                Set(DataRecord(None, Some("action"), ACTION(
                    actionoption = Conversion convertJob job,
                    ok = ACTION_TRANSITION(Map("@to" -> DataRecord(okTransition))),
                    error = ACTION_TRANSITION(Map("@to" -> DataRecord(to))),
                    attributes = Map("@name" -> DataRecord(name)))))
            case WorkflowDecision(predicates, _) =>
                val defaultName = getDecisionRouteName("default")
                val caseSeq = predicates map (pred => {
                    val route = getDecisionRouteName(pred._1)
                    CASE(Conversion convertPredicate pred._2, Map("@to" -> DataRecord(route)))
                })
                Set(DataRecord(None, Some("decision"), DECISION(
                    switch = SWITCH(
                        switchsequence1 = SWITCHSequence1(
                            caseValue = caseSeq,
                            default = DEFAULT(Map("@to" -> DataRecord(defaultName))))),
                    attributes = Map("@name" -> DataRecord(name)))))
            case _ => ???
        }
    }
}

object GraphNode {
    def apply(name: String, workflowOption: WorkflowOption): GraphNode =
        GraphNode(name, workflowOption, RefSet(), RefSet())
}
