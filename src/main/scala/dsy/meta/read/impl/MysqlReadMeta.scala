package dsy.meta.read.impl

import dsy.config.configs.{MYSQL_DEFAULT_DRIVER, MYSQL_DEFAULT_PASSWORD, MYSQL_DEFAULT_USER}
import dsy.meta.read.AbstractReadMeta


/**
 * mysql 元数据解析存储
 *
 * inType=mysql
 *
 * @param driver   driver=com.mysql.jdbc.Driver(默认值)
 * @param url      url=jdbc:mysql://xxxx:xx/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC
 * @param dbtable  dbtable=xxx 读取的表
 * @param user     user=xxx 用户名(root)
 * @param password password=xxx 密码(123456)
 */
case class MysqlReadMeta(
                          driver: String,
                          url: String,
                          dbtable: String,
                          user: String,
                          password: String
                        )


object MysqlReadMeta extends AbstractReadMeta {


  /**
   * 将Map集合数据解析到 MysqlReadMeta 中封装返回
   *
   * @param ruleMap 规则map集合
   * @return 读取 mysql 数据源封装对象 元数据对象
   */
  override def getObject(ruleMap: Map[String, String]): MysqlReadMeta = {
    //解析Map进行封装
    val driver = setMetaElementValue(ruleMap, "driver", MYSQL_DEFAULT_DRIVER)
    val url = setMetaElementValue(ruleMap, "url")
    val dbtable = setMetaElementValue(ruleMap, "dbtable")
    val user = setMetaElementValue(ruleMap, "user", MYSQL_DEFAULT_USER)
    val password = setMetaElementValue(ruleMap, "password", MYSQL_DEFAULT_PASSWORD)

    //封装元数据对象
    MysqlReadMeta(driver, url, dbtable, user, password)
  }
}