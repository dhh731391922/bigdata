package chapter01

import com.sun.xml.internal.bind.v2.TODO

import scala.io.StdIn
import scala.util.control.Breaks

object Scala01_HelloWorld {
  def main(args: Array[String]): Unit = {
    print("hello world")
    //三种输出方式
    //java方式
    val name="张三"
    val age =19
    val url="www.ai.com"
    println(name +age+url)//java
    printf("name=%s,age=%d.2f,url=%s \n",name,age,url)//c语言
    print(s"name=${name},age=${age},url=${url} \n" )//el表达式
    print(f"name=${name},age=${age}%.2f,url=${url} \n")//el表达式格式
    print(raw"name=${name},age=${age}%.2f,url=${url} \n")//原样输出
//    val names = StdIn.readLine()
//    print(names)
    //to <=
    //until <
    for (i <- 0 to 5){
      println(s"i=${i}")
    }
    for (i <- 0 until 5){
      println(s"i=${i}")
    }
    for (i<-Range(0,5)){//range就是until的返回值
      print(s"i=${i}")
    }
    for (i<-Range(0,5,2)){//range就是until的返回值
      println(s"i=${i}")
    }
    for (i<-Range(1,18,2)){
      println(" "*((18-i)/2)+"*"*i+" "*((18-i)/2))
    }
    for (i<-Range(1,18,2);f=(18-i)/2){//for循环定义变量，不推荐写法
      println(" "*f+"*"*i+" "*f)
    }

    for (i<- 1 to 3){
      for (j<- 1 to 3){
        println(s"${i}+${j}")
      }
    }
    for (i<-1 to 3;j<-1 to 3 ){
      println(s"${i}+${j}")   //嵌套for的简写
    }


    for {i<-Range(1,18,2)
         f=(18-i)/2}{//for循环定义变量，推荐写法
          println(" "*f+"*"*i+" "*f)
    }

    for (i<-Range(1,18,2) if i%3==0){//类似于continue满足条件的输出，不满足跳过
      println(" "*((18-i)/2)+"*"*i+" "*((18-i)/2))
    }
    Breaks.breakable{//相当于trycatch
      for (i<-1 to 3;j<-1 to 3 ){
        if (j==2)
          Breaks.break()//抛异常中断
        println(s"${i}+${j}")   //break
      }
    }
    def test(name : String*)= println(name)//可变参
    test()

    //函数参数的默认值,调用时如果函数第一个有默认值，单也不能省略,可是使用带名参数的调用
    def test1(name1:String="zcd" ,name :String)=println(s"${name1},${name}")
    test1(name="san")

    def f()={
      println("f")

    }

    def f0()={
//      f   返回函数的返回值
        f _ //返回函数本身
    }
    f0()()

    def f1(i:Int)={
        def f2(j:Int)={
          //TODO i外部变量引入到函数的内容，改变了这个变量（i）的生命周期，称之为闭包
          i*j
        }
      f2 _
    }
    println(f1(2)(3))

    //TODO 函数科里化
    def f3(i:Int)(j:Int)={
      i*j
    }
    println(f3(2)(3))
    //TODO 函数的类型（数据类型）=>数据类型 ()=>Int
    def f4(f:()=>Int)={
      f()+10
    }
    def f5()={
      5
    }
    println(f4(f5))
  }

  //TODO 使用匿名函数调用
//  println(f4(()=>{7}))
  def f6(f : ()=>Unit):Unit={
    f()
  }
  def f7()={
    println("xxx")
  }
  f6(f7)
  f6( ()=>{ println("xxx")} )
  println(s"xxxxx")
}
