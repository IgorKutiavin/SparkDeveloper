package Lesson04

import scala.::
import scala.io.StdIn

object TestForLesson extends App {

  //1
  val a = 25
  val v = 87.5
  val n = "Stiff"
  val s = true

  //2
  println(a,v,n,s)

  //3
  def plus(a: Int, b: Int): Int = {
    a + b
  }
  println(plus(100,2))
  //4
  def check_ages (a:Int):String = {
    if (a < 30) "Молодой" else "Взрослый"
  }
  println(check_ages(a))
  //5
  for {i <- 1 to 10} yield println(i)

  //5.1
  val list_student = List("Mark","Pol","Eva","Anna")
  list_student.foreach(f => println(f))

  //6
  val nme = StdIn.readLine("What is you name:")
  val age = StdIn.readLine("How old a you:")
  val std = StdIn.readLine("You is student:")
  println(nme,age,std,check_ages(age.toInt))

  val list_num = 1 to 10

  val list_square = for {
    b1 <- list_num
  } yield (b1 * b1)

  val list_even = for {
    b1 <- list_num if b1 % 2 == 0
  } yield(b1)

  println("1 to 10:",list_num)
  println("squart:",list_square)
  println("even:", list_even)
}
