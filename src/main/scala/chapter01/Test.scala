package chapter01

import scala.util.control.Breaks

object Test  {
  def main(args: Array[String]): Unit = {
    

    def f4(f:(Int)=>Unit){
      f(9)
    }
    f4((x)=>{println(x)})
    f4(println)





    def f6(f : ()=>Unit):Unit={
      f()
    }
    def f7()={
      println("xxx")
    }
    f6(f7)
    f6(()=>{println("ccccccccc")})
    def f8(f:(Int)=>Unit)={
        f(10)
    }
    f8((i:Int)=>{println(i)})
    f8(println)
    def f9(f:(Int,Int)=>Int):Int={
      f(10,10)
    }
   println(f9((i:Int,j:Int)=>{i+j}))
    println(f9(_+_))
  }
}
