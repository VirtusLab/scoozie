package org.virtuslab.scoozie
package runner

import org.virtuslab.scoozie.dsl._
import org.virtuslab.scoozie.jobs._

object Workflows {
    def MaxwellPipeline = {
        val featureGen = NoOpJob("FeatureGeneration") dependsOn Start
        val scoreCalc = NoOpJob("ScoreCalculation") dependsOn featureGen
        val momentGen = NoOpJob("MomentGeneration") dependsOn scoreCalc
        val end = End dependsOn momentGen
        Workflow("maxwell-pipeline-wf", end)
    }

}
