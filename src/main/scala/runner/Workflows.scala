package com.virtuslab.scoozie
package runner

import com.virtuslab.scoozie.dsl._
import com.virtuslab.scoozie.jobs._

object Workflows {
    def MaxwellPipeline = {
        val featureGen = NoOpJob("FeatureGeneration") dependsOn Start
        val scoreCalc = NoOpJob("ScoreCalculation") dependsOn featureGen
        val momentGen = NoOpJob("MomentGeneration") dependsOn scoreCalc
        val end = End dependsOn momentGen
        Workflow("maxwell-pipeline-wf", end)
    }

}
