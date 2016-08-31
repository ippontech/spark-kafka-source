package com.ippontech.kafka.util

// very simple stop watch to avoid using Guava's one
class Stopwatch {

  private val start = System.currentTimeMillis()

  override def toString() = (System.currentTimeMillis() - start) + " ms"

}
