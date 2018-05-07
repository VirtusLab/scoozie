package org.virtuslab.scoozie
package verification

import conversion.Configuration._
import scalaxb._
import oozie.workflow._
import oozie.hive
import protocol._
import scala.xml._

object XMLVerification {

    def verify(referenceString: String, targetString: String): Boolean = {
        val referenceWf = getWorkflow(referenceString)
        val targetWf = getWorkflow(targetString)
        val referenceNodes: Map[String, WORKFLOWu45APPOption] = getNameNodeMap(referenceWf.workflowu45appoption)
        val targetNodes: Map[String, WORKFLOWu45APPOption] = getNameNodeMap(targetWf.workflowu45appoption)

        //will recur on start nodes of wf
        val firstRefNode = getNodeByName(referenceNodes, referenceWf.start.to)
        val firstTargetNode = getNodeByName(targetNodes, targetWf.start.to)

        areFunctionallySame(firstRefNode, firstTargetNode, referenceNodes, targetNodes)
    }

    def getNameNodeMap(wf: Seq[scalaxb.DataRecord[WORKFLOWu45APPOption]]): Map[String, WORKFLOWu45APPOption] = {
        wf.map(node => (getNodeName(node), node.value)) toMap
    }

    def getNodeName(node: DataRecord[WORKFLOWu45APPOption]): String = {
        node.value match {
            case decision: DECISION => decision.name
            case fork: FORK         => fork.name
            case join: JOIN         => join.name
            case kill: KILL         => kill.name
            case action: ACTION     => action.name
            case _                  => throw new RuntimeException("error: unexpected node type")
        }
    }

    def areFunctionallySame(ref: WORKFLOWu45APPOption, target: WORKFLOWu45APPOption, refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        val refNode = makeNamesBlank(ref)
        val targetNode = makeNamesBlank(target)

        (targetNode, refNode) match {

            case (ACTION(targetActionoption, targetOk, targetError, targetAny, targetName), ACTION(refActionoption, refOk, refError, refAny, refName)) =>
                val sameConfiguration = refActionoption == targetActionoption
                val sameNextNodes = nextNodesAreSame(refOk, targetOk, refError, targetError, refNodes, targetNodes)
                val areSame = sameConfiguration && sameNextNodes
                if (!sameConfiguration) {
                    println("node configurations do not match: \n" + refActionoption + "\n" + targetActionoption)
                }
                areSame

            case (target: KILL, ref: KILL) => true

            case (DECISION(targetSwitch, targetName), DECISION(refSwitch, refName)) =>
                val sameDecisionCases = decisionCasesAreSame(refSwitch.switchsequence1, targetSwitch.switchsequence1, refNodes, targetNodes)
                if (!sameDecisionCases)
                    println("decision cases are not the same: \n" + refSwitch + "\n" + targetSwitch)
                sameDecisionCases

            case (target: FORK, ref: FORK) =>
                val areSame = forkTransitionsAreSame(ref.path, target.path, refNodes, targetNodes)
                if (!areSame)
                    println("fork transitions are not the same: \n" + ref + "\n" + target)
                areSame

            case (target: JOIN, ref: JOIN) =>
                val areSame = {
                    if (oneGoesToEnd(ACTION_TRANSITION(Map("@to" -> DataRecord(ref.to))), ACTION_TRANSITION(Map("@to" -> DataRecord(target.to)))))
                        bothGoToEnd(ACTION_TRANSITION(Map("@to" -> DataRecord(ref.to))), ACTION_TRANSITION(Map("@to" -> DataRecord(target.to))))
                    else
                        areFunctionallySame(getNodeByName(refNodes, ref.to), getNodeByName(targetNodes, target.to), refNodes, targetNodes)
                }
                if (!areSame)
                    println("join transitions are not the same: \n" + ref + "\n" + target)
                areSame

            case (target, ref) =>
                if (ref.getClass != target.getClass) {
                    println("nodes not of same type: \n" + refNode + "\n" + targetNode)
                    false
                } else
                    ref == target
        }
    }

    def sortHiveParams(hiveAction: hive.ACTION): hive.ACTION = {
        val sortedParams = hiveAction.param.sorted
        hiveAction.copy(param = sortedParams)
    }

    def fromXMLString[A: XMLFormat](xmlString: String): A = {
        fromXML[A](xml.XML.loadString(xmlString))
    }

    def getWorkflow(xmlString: String): WORKFLOWu45APP = {
        val workflow = fromXMLString[WORKFLOWu45APP](xmlString)
        convertHive(workflow)
    }

    def makeNamesBlank(attributes: Map[String, DataRecord[Any]]): Map[String, DataRecord[Any]] = attributes + ("@name" -> DataRecord(""))

    def makeNamesBlank(node: WORKFLOWu45APPOption): WORKFLOWu45APPOption = {
        node match {
            case decision: DECISION => decision.copy(attributes = makeNamesBlank(decision.attributes))
            case fork: FORK         => fork.copy(attributes = makeNamesBlank(fork.attributes))
            case join: JOIN         => join.copy(attributes = makeNamesBlank(join.attributes))
            case kill: KILL         => kill.copy(attributes = makeNamesBlank(kill.attributes))
            case action: ACTION     => action.copy(attributes = makeNamesBlank(action.attributes))
            case _                  => node
        }
    }

    def oneGoesToEnd(refTransition: ACTION_TRANSITION, targetTransition: ACTION_TRANSITION): Boolean =
        refTransition.to == "end" || targetTransition.to == "end"

    def bothGoToEnd(refTransition: ACTION_TRANSITION, targetTransition: ACTION_TRANSITION): Boolean =
        refTransition.to == "end" && targetTransition.to == "end"

    def nextNodesAreSame(refOk: ACTION_TRANSITION, targetOk: ACTION_TRANSITION, refError: ACTION_TRANSITION, targetError: ACTION_TRANSITION, refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        val okToMatch = {
            if (oneGoesToEnd(refOk, targetOk))
                bothGoToEnd(refOk, targetOk)
            else
                areFunctionallySame(getNodeByName(refNodes, refOk.to), getNodeByName(targetNodes, targetOk.to), refNodes, targetNodes)
        }
        val errorToMatch = {
            if (oneGoesToEnd(refError, targetError))
                bothGoToEnd(refError, targetError)
            else
                areFunctionallySame(getNodeByName(refNodes, refError.to), getNodeByName(targetNodes, targetError.to), refNodes, targetNodes)
        }
        okToMatch && errorToMatch
    }

    def decisionCasesAreSame(refSwitch: SWITCHSequence1, targetSwitch: SWITCHSequence1, refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        val refCases = refSwitch.caseValue
        val targetCases = targetSwitch.caseValue
        val optionsSame: Boolean = {
            if (refCases.length != targetCases.length) {
                println("error: decisions don't have same number of paths")
                false
            } else {
                val decisionCasePairs = refCases.zip(targetCases)
                decisionCasePairs.forall {
                    case (refCase, targetCase) =>
                        val samePredicate = refCase.value == targetCase.value
                        val bothToEnd = refCase.to == "end" && targetCase.to == "end"
                        val functionallySame = areFunctionallySame(getNodeByName(refNodes, refCase.to), getNodeByName(targetNodes, targetCase.to), refNodes, targetNodes)
                        samePredicate && (bothToEnd || functionallySame)
                }
            }
        }
        val defaultsSame: Boolean = (refSwitch.default.to == "end" && targetSwitch.default.to == "end") || areFunctionallySame(getNodeByName(refNodes, refSwitch.default.to), getNodeByName(targetNodes, targetSwitch.default.to), refNodes, targetNodes)
        optionsSame && defaultsSame
    }

    def forkTransitionsAreSame(refTransitions: Seq[FORK_TRANSITION], targetTransitions: Seq[FORK_TRANSITION], refNodes: Map[String, WORKFLOWu45APPOption], targetNodes: Map[String, WORKFLOWu45APPOption]): Boolean = {
        if (refTransitions.length != targetTransitions.length)
            false
        else {
            refTransitions forall { refTransition =>
                val nextRefNode = getNodeByName(refNodes, refTransition.start)
                val matchingTransition = targetTransitions.find (targetTransition => areFunctionallySame(nextRefNode, getNodeByName(targetNodes, targetTransition.start), refNodes, targetNodes))
                matchingTransition nonEmpty
            }
        }
    }

    /*
         * Takes a workflow class, searches for un-processed Hive xml,
         * and converts it into a parameterized object
         */
    def convertHive(wf: WORKFLOWu45APP): WORKFLOWu45APP = {
        val options = wf.workflowu45appoption.map({ wfOption =>
            wfOption.value match {
                case ACTION(actionOption, ok, error, any, name) =>
                    //if we find an un-parsed Hive action, format it to a case class
                    val action: DataRecord[Any] = actionOption.value match {
                        case elem @ Elem(prefix, "hive", attributes, scope, children @ _*) =>
                            val deDupedScope = NamespaceBinding(null, xmlHiveActionNamespace, scala.xml.TopScope)
                            val copiedChildren: Seq[Node] = children.flatMap(formatHiveNode)
                            val newElem = Elem(prefix, "hive", attributes, deDupedScope, minimizeEmpty = false, copiedChildren: _*)
                            val hiveAction = fromXMLString[hive.ACTION](newElem.toString)
                            val processedHiveAction = sortHiveParams(hiveAction)
                            DataRecord(processedHiveAction)
                        case _ =>
                            actionOption
                    }
                    DataRecord(ACTION(action, ok, error, any, name))
                case _ => wfOption
            }
        })
        wf.copy(workflowu45appoption = options)
    }

    /*
     * Format Hive xml elements to get around all the gross name-spacing issues
     */
    def formatHiveNode(node: Node): Node = {
        val hiveNameSpacedNode = Elem(node.prefix, node.label, node.attributes, NamespaceBinding(null, xmlHiveActionNamespace, scala.xml.TopScope), minimizeEmpty = false, node.child.flatMap(formatHiveNode): _*)
        val workflowNameSpacedNode = Elem(node.prefix, node.label, node.attributes, NamespaceBinding(null, xmlWorkflowNamespace, scala.xml.TopScope), minimizeEmpty = false, node.child.flatMap(formatHiveNode): _*)
        val hiveNameSpacedTags = List("job-tracker", "name-node", "prepare", "job-xml", "configuration", "script", "param", "file")
        node match {
            case Elem(prefix, label, attributes, scope, children @ _*) =>
                if (hiveNameSpacedTags contains label)
                    hiveNameSpacedNode
                else
                    workflowNameSpacedNode
            case _ =>
                node
        }
    }

    def getNodeByName(nodes: Map[String, WORKFLOWu45APPOption], nodeName: String): WORKFLOWu45APPOption = {
        nodes.get(nodeName) match {
            case Some(node) => node
            case _          => throw new RuntimeException(s"error: node $nodeName not in workflow")
        }
    }
}
