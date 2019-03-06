package com.gmall.common.datamodules

/**
  * @aythor HeartisTiger
  *         2019-03-06 19:54
  */
/**
  * 产品表
  *
  * @param product_id   商品的ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )
