package com.virtuslab.scoozie
package verification

import com.virtuslab.scoozie.conversion.GraphNode

//path is the child of the fork that this thread lies on
case class ForkThread(fork: GraphNode, path: GraphNode)
