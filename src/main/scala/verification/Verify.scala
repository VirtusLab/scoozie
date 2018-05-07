package org.virtuslab.scoozie
package verification

object Verify extends App {
    val refPath = readLine("input reference xml path: ")
    val targetPath = readLine("input target scoozie generated xml path: ")
    val refXml = scala.io.Source.fromFile(refPath).mkString
    val targetXml = scala.io.Source.fromFile(targetPath).mkString
    val areSame = XMLVerification.verify(refXml, targetXml)
    if (areSame)
        println("workflows are functionally equal")
    else
        println("error: workflows are not functionally equal")
}