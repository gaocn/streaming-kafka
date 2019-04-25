package org.apache.spark

import org.scalatest.{FunSuite, Outcome}

private[spark] abstract class SparkFunSuite extends FunSuite with Logging {

	/**
		* 在每次测试开始和结束时，添加当前测试组件名称和测试方法名
		* @param test 要测试的方法
		* @return
		*/
	final protected override def withFixture(test: NoArgTest): Outcome = {
		val testName = test.text
		val suiteName = this.getClass.getName
		val shortSuiteName = suiteName.replaceAll("org.apache.spark.streaming", "o.a.s.s")

		try {
			logInfo(s"\n\n==== TEST OUTPUT FOR ${shortSuiteName}: '${testName}' ====\n")
			test()
		} finally {
			logInfo(s"\n\n==== FINISHED ${shortSuiteName}: '${testName}' ====\n")
		}
	}
}
