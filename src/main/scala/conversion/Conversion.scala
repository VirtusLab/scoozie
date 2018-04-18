package com.klout.scoozie
package conversion

import com.klout.scoozie.dsl._
import com.klout.scoozie.jobs._
import com.klout.scoozie.verification._
import scalaxb._
import workflow._
import Configuration._

object Conversion {
    val JobTracker = "${jobTracker}"
    val NameNode = "${nameNode}"

    def apply(workflow: Workflow): WORKFLOWu45APP = {
        val flattenedNodes = Flatten(workflow).values.toSet
        val finalGraph = Verification.verify(flattenedNodes)
        val orderedNodes = order(RefSet(finalGraph.toSeq)).toList sortWith PartiallyOrderedNode.lt map (_.node)
        val workflowOptions = orderedNodes flatMap (_.toXmlWorkflowOption)
        val startTo: String = orderedNodes.headOption match {
            case Some(node) => node.name
            case _          => "end"
        }
        WORKFLOWu45APP(
            workflowu45appoption = workflowOptions :+ DataRecord(None, Some("kill"), KILL(
                message = workflow.name + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                attributes = Map("@name" -> DataRecord("kill")))),
            start = START(attributes = Map("@to" -> DataRecord(startTo))),
            end = END(attributes = Map("@name" -> DataRecord("end"))),
            attributes = Map("@name" -> DataRecord(workflow.name)))
    }

    def convertJob(job: Job): DataRecord[Any] = job match {
        case MapReduceJob(name, prep, config, configClass) =>
            DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some(JobTracker),
                nameu45node = Some(NameNode),
                prepare = getPrepare(prep),
                configuration = getConfiguration(config),
                configu45class = configClass))

        case HiveJob(fileName, config, params, prep, argument, jobXml, otherFiles) =>
            DataRecord(None, Some("hive"), ACTIONType(
                jobu45tracker = Some(JobTracker),
                nameu45node = Some(NameNode),
                prepare = getPrepare(prep),
                jobu45xml = jobXml.getOrElse(Seq.empty),
                configuration = getConfiguration(config),
                script = fileName,
                param = params,
                argument = argument.toList,
                file = otherFiles.getOrElse(Nil),
                attributes = Map("@xmlns" -> DataRecord(xmlHiveActionNamespace))))

        case JavaJob(mainClass, prep, config, jvmOps, jvmOp, args) =>
            DataRecord(None, Some("java"), JAVA(
                jobu45tracker = Some(JobTracker),
                nameu45node = Some(NameNode),
                mainu45class = mainClass,
                prepare = getPrepare(prep),
                configuration = getConfiguration(config),
                javaoption = jvmOp.map(op => DataRecord(None, Some("jvmOp"), op)) ++ jvmOps.map(op => DataRecord(None, Some("jvmOp"), op)),
                arg = args))

        //limitation: tasks must be of the same type
        case FsJob(name, tasks) =>
            DataRecord(None, Some("fs"), FS(
                nameu45node = Some(NameNode),
                jobu45xml = Nil,
                configuration = None,
                fsoption = tasks flatMap {
                    case Rm(path)     => Some(DataRecord(None, Some("rm"), DELETE(Map("@path" -> DataRecord(path)))))
                    case MkDir(path)  => Some(DataRecord(None, Some("mkdir"), MKDIR(Map("@path" -> DataRecord(path)))))
                    case Mv(from, to) => Some(DataRecord(None, Some("mv"), MOVE(Map("@source" -> DataRecord(from), "@target" -> DataRecord(to)))))
                    case ChMod(recursive, path, permissions, dirFiles) => Some(DataRecord(None, Some("chmod"), CHMOD(
                        if (recursive) Some(FLAG()) else None,
                        Map(
                            "@path" -> DataRecord(path),
                            "@permissions" -> DataRecord(permissions),
                            "@dir-files" -> DataRecord(dirFiles)))))
                    case Touchz(path) => Some(DataRecord(None, Some("touchz"), TOUCHZ(Map("@path" -> DataRecord(path)))))
                    case ChGrp(recursive, path, group, dirFiles) => Some(DataRecord(None, Some("chgrp"), CHGRP(
                        if (recursive) Some(FLAG()) else None,
                        Map(
                            "@path" -> DataRecord(path),
                            "@group" -> DataRecord(group),
                            "@dir-files" -> DataRecord(dirFiles)))))
                    case _ => None
                }))
        case _ => ???
    }

    def getPrepare(prep: List[FsTask]): Option[PREPARE] = {
        val deletes: Seq[DELETE] = prep flatMap {
            case Rm(path) => Some(DELETE(Map("@path" -> DataRecord(path))))
            case _        => None
        }
        val mkdirs: Seq[MKDIR] = prep flatMap {
            case MkDir(path) => Some(MKDIR(Map("@path" -> DataRecord(path))))
            case _           => None
        }
        if (!(deletes.isEmpty && mkdirs.isEmpty))
            Some(PREPARE(deletes, mkdirs))
        else
            None
    }
    def getConfiguration(config: ArgList): Option[CONFIGURATION] = {
        if (config.nonEmpty)
            Some(CONFIGURATION(config map (tuple => Property(tuple._1, tuple._2))))
        else
            None
    }

    def convertPredicate(pred: dsl.Predicate): String = {
        pred match {
            case dsl.Predicates.AlwaysTrue                => "true"
            case pred @ dsl.Predicates.BooleanProperty(_) => pred.formattedProperty
        }
    }

    def isDescendent(child: GraphNode, ancestor: GraphNode): Boolean = {
        if (child == ancestor)
            true
        else if (child.before == Set.empty && child.decisionBefore == Set.empty)
            false
        else (child.before ++ child.decisionBefore).exists (isDescendent(_, ancestor))
    }

    /*
     * Requires: from is a descendent of to
     */
    def getMaxDistFromNode(from: GraphNode, to: GraphNode): Int = {
        if (!isDescendent(from, to))
            Int.MaxValue
        else if (from eq to)
            0
        else 1 + ((from.before ++ from.decisionBefore) map ((currNode: GraphNode) => getMaxDistFromNode(currNode, to)) max)
    }

    def order(nodes: RefSet[GraphNode]): RefSet[PartiallyOrderedNode] = {
        // find node with no before nodes
        val startNodes = nodes.filter (n => Flatten.isStartNode(n))
        val startNode = startNodes.headOption
        nodes map ((currNode: GraphNode) => {
            if (startNodes contains currNode)
                PartiallyOrderedNode(currNode, 0)
            else {
                val from = currNode
                val to = startNode.get
                val dist = getMaxDistFromNode(from, to)
                PartiallyOrderedNode(currNode, dist)
            }
        })
    }
}
