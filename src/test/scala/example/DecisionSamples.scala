package com.virtuslab.scoozie
package example

import com.virtuslab.scoozie.dsl._
import com.virtuslab.scoozie.jobs.{ MapReduceJob, NoOpJob }

object DecisionSamples {

    val FirstJob = MapReduceJob("foo") dependsOn Start

    val SomeDecision = Decision(
        "ifBing" -> Predicates.BooleanProperty("ifBing")) dependsOn Start

    val Route1 = MapReduceJob("skippingBing") dependsOn (SomeDecision default)
    val Route2 = MapReduceJob("processBing") dependsOn (SomeDecision option "ifBing")
    val Done = End dependsOn OneOf(Route1, Route2)

    val Pipeline = Workflow("decisions", Done)

    def newDecisionSample = {
        val first = NoOpJob("first") dependsOn Start
        val optionalNode = NoOpJob("optional") dependsOn first doIf "${doOptionalNode}"
        val alwaysDo = NoOpJob("always do") dependsOn Optional(optionalNode)
        val optionalNode2 = {
            val sub1 = NoOpJob("sub1") dependsOn Start
            val sub2 = NoOpJob("sub2") dependsOn sub1
            val sub3 = NoOpJob("sub3") dependsOn sub2
            Workflow("sub-wf", sub3)
        } dependsOn alwaysDo doIf "{doSubWf}"
        val alwaysDo2 = NoOpJob("always do 2") dependsOn Optional(optionalNode2)
        val end = End dependsOn alwaysDo2
        Workflow("new-decision", end)
    }
}
