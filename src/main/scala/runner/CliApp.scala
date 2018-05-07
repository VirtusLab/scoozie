package com.virtuslab.scoozie
package runner

import java.io.{ FileWriter, PrintWriter }

import com.virtuslab.scoozie.dsl.Workflow

import scala.io.Source

abstract class CliApp(wfs: List[Workflow], postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)) extends App {

    override def main(args: Array[String]): Unit = {
        var continue = true
        while (continue) {
            println("choose a workflow for more information: ")
            var index = 0
            wfs foreach { wf =>
                println(index + " -> " + wf.name)
                index += 1
            }
            val ln = Source.stdin.getLines
            val choice = ln.next.toInt
            if (choice < 0 || choice >= wfs.length) {
                println("invalid choice")
            } else {
                println(wfs(choice))
                val ln = Source.stdin.getLines
                println("generate xml for this workflow? y/n")
                val print = ln.next
                print match {
                    case "y" =>
                        val xmlString = RunWorkflow.getXMLString(wfs(choice), postprocessing)
                        println("input filename to write output to")
                        val outName = ln.next
                        println("print to screen? y/n")
                        val p2Screen = ln.next
                        if (p2Screen == "y")
                            println(xmlString)
                        writeToFile(xmlString, outName)
                    case _ =>
                }
            }
            println("exit? y/n")
            val exit = ln.next
            exit match {
                case "y" => continue = false
                case _   => continue = true
            }
        }
    }

    def writeToFile(toWrite: String, outfile: String): Unit = {
        val out = new PrintWriter(new FileWriter(outfile))
        out.println(toWrite)
        out.close()
    }
}
