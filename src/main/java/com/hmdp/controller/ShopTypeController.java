package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopTypeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@RestController
@RequestMapping("/shop-type")
public class ShopTypeController {
    @Resource
    private IShopTypeService typeService;

    /**
     * 查询店铺类型
     * @return
     */
    @GetMapping("list")
    public Result queryTypeList() {
        //List<ShopType> typeList = typeService.query().orderByAsc("sort").list();
        //return Result.ok(typeList);

        // 上面是原代码
        return typeService.queryTypeList();

    }
}
