package Lesson02

import scala.io.StdIn

object Test extends App{
  val name = StdIn.readLine("Name: ")

  if (name == "Igor") {
    println(s"Hello $name")
  }
  else {
    println("You is not Igor")
  }

  def plus (a: Int, b: Int):Int = {
    a+b
  }

  println("plus(1,2):" + plus(1,2))

  val ll = List(1,2,3,4,5)


  println(ll)
  println(ll.map(_+1))

  val n = StdIn.readLine("Put number:")

  if (n.toInt % 2 == 0)  println(s"This number $n is even") else println(s"This number $n is not even")

  val s = StdIn.readLine("Put text:")

  println(s"Text $s equal ${s.length} chars")

  val l:List[String] = List("This","is","test","concat","words")

  println(s"Input list $l")
  println(s"Output text ${l.mkString(" ")}")
}
