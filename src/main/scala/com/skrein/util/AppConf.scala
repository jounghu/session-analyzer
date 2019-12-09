package com.skrein.util

import java.util.Properties

object AppConf {

  val prop = {
    val prop = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("app.properties")
    prop.load(in)
    prop
  }

  def getInt(key: String): Int = {
    Integer.valueOf(prop.get(key).asInstanceOf[String])
  }

  def getString(key: String): String = {
    prop.get(key).asInstanceOf[String]
  }

  def getBoolean(key: String): java.lang.Boolean = {
    java.lang.Boolean.valueOf(prop.get(key).asInstanceOf[String])
  }


  def main(args: Array[String]): Unit = {
    getBoolean("local")
  }

}

