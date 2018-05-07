package com.virtuslab.scoozie
package runner

import com.virtuslab.scoozie.dsl.Workflow

abstract class ScoozieApp(
    wf:             Workflow,
    propertiesFile: String,
    postprocessing: Option[XmlPostProcessing] = Some(XmlPostProcessing.Default)) extends App {

    /*
     * Usage is java -cp <...> com.klout.scoozie.ObjectName today
     * -todayString=foo2 -yesterdayString=foo3 ...
     */
    override def main(args: Array[String]) {
        val argString = ("" /: args)(_ + _.toString)
        //set up properties / configuration
        val usage = "java -cp <...> com.klout.scoozie.ObjectName -todayString=foo -yesterdayString=foo ..."
        var propertyMap = Helpers.readProperties(propertiesFile)
        if (args nonEmpty) {
            args foreach { arg =>
                if (arg(0) != '-')
                    throw new RuntimeException("error: usage is " + usage)
                propertyMap = Helpers.addProperty(propertyMap, arg.tail)
            }
        }
        val rawAppPath = propertyMap.get("scoozie.wf.application.path").get
        val appPath = {
            if (!rawAppPath.endsWith(".xml")) {
                val suffix = propertyMap.get("pathSuffix") match {
                    case Some(toSuffix) => "_" + toSuffix
                    case None           => ""
                }
                rawAppPath + "scoozie_" + wf.name + suffix + ".xml"
            } else
                throw new RuntimeException("error: you should not overwrite the .xml")
        }
        val oozieUrl = propertyMap.get("scoozie.oozie.url").get
        val config = OozieConfig(oozieUrl, propertyMap)
        //run
        RunWorkflow(wf, appPath, config, postprocessing) match {
            case Left(_)  => throw new RuntimeException("error: workflow execution failed")
            case Right(_) => Unit
        }
    }
}
