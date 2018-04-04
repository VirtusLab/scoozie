package com.klout.scoozie
package verification

import com.klout.scoozie.conversion.GraphNode

//path is the child of the fork that this thread lies on
case class ForkThread(fork: GraphNode, path: GraphNode)
